import json
import threading
from pathlib import Path

import pytest
from conftest import utc, write_raw_file

from bsky_iceberg_loader.consumer import QueueConsumer
from bsky_iceberg_loader.events import S3Object


class FakeSQS:
    def __init__(self, messages):
        self.pending = list(messages)
        self.deleted = []
        self.released = []

    def receive_message(self, QueueUrl, MaxNumberOfMessages, WaitTimeSeconds):
        batch, self.pending = self.pending[:MaxNumberOfMessages], self.pending[MaxNumberOfMessages:]
        return {"Messages": batch} if batch else {}

    def change_message_visibility_batch(self, QueueUrl, Entries):
        assert len(Entries) <= 10 and all(e["VisibilityTimeout"] == 0 for e in Entries)
        self.released.extend(e["ReceiptHandle"] for e in Entries)
        return {"Successful": [{"Id": e["Id"]} for e in Entries], "Failed": []}

    def delete_message_batch(self, QueueUrl, Entries):
        assert len(Entries) <= 10
        self.deleted.extend(e["ReceiptHandle"] for e in Entries)
        return {"Successful": [{"Id": e["Id"]} for e in Entries], "Failed": []}


def message(n, *keys, bucket="bucket", size=None):
    obj = {} if size is None else {"size": size}
    body = json.dumps(
        {
            "Records": [
                {
                    "eventSource": "aws:s3",
                    "eventName": "ObjectCreated:Put",
                    "s3": {"bucket": {"name": bucket}, "object": {"key": key, **obj}},
                }
                for key in keys
            ]
        }
    )
    return {"MessageId": f"m{n}", "ReceiptHandle": f"r{n}", "Body": body}


@pytest.fixture
def bucket_dir(tmp_path) -> Path:
    return tmp_path / "bucket"


def make_consumer(sqs, loader, bucket_dir, **kwargs):
    # Map s3://bucket/key onto files under bucket_dir.
    return QueueConsumer(
        sqs,
        "queue",
        loader,
        object_exists=lambda o: (bucket_dir / o.key).exists(),
        uri_for=lambda o: (bucket_dir / o.key).as_uri(),
        source_prefix="jetstream/",
        **{"wait_time": 0, "batch_window": 0, **kwargs},
    )


def table_files(loader):
    return sorted({r.source_file for r in loader.spark.table(loader.table).select("source_file").collect()})


def test_loads_files_and_deletes_messages(loader, bucket_dir):
    a = write_raw_file(bucket_dir / "jetstream/date=2026-10-22/hour=06/a.parquet", [utc(2026, 10, 22, 6, 0)])
    b = write_raw_file(bucket_dir / "jetstream/date=2026-10-22/hour=06/b.parquet", [utc(2026, 10, 22, 6, 1)])
    sqs = FakeSQS(
        [
            message(1, "jetstream/date=2026-10-22/hour=06/a.parquet"),
            message(2, "jetstream/date=2026-10-22/hour=06/b.parquet"),
        ]
    )
    consumer = make_consumer(sqs, loader, bucket_dir)

    assert consumer.process(consumer.receive_batch()) is True
    assert table_files(loader) == [a, b]
    assert sqs.deleted == ["r1", "r2"]


def test_ignores_other_prefixes_and_test_events(loader, bucket_dir):
    write_raw_file(bucket_dir / "iceberg/data/x.parquet", [utc(2026, 10, 22, 6, 0)])
    test_event = {"MessageId": "t", "ReceiptHandle": "rt", "Body": json.dumps({"Event": "s3:TestEvent"})}
    sqs = FakeSQS([message(1, "iceberg/data/x.parquet", "jetstream/notes.txt"), test_event])
    consumer = make_consumer(sqs, loader, bucket_dir)

    assert consumer.process(consumer.receive_batch()) is True
    assert table_files(loader) == []
    assert sqs.deleted == ["r1", "rt"]


def test_missing_objects_are_skipped(loader, bucket_dir):
    a = write_raw_file(bucket_dir / "jetstream/a.parquet", [utc(2026, 10, 22, 6, 0)])
    sqs = FakeSQS([message(1, "jetstream/a.parquet", "jetstream/gone.parquet")])
    consumer = make_consumer(sqs, loader, bucket_dir)

    assert consumer.process(consumer.receive_batch()) is True
    assert table_files(loader) == [a]
    assert sqs.deleted == ["r1"]


def test_malformed_messages_stay_on_queue(loader, bucket_dir):
    write_raw_file(bucket_dir / "jetstream/a.parquet", [utc(2026, 10, 22, 6, 0)])
    bad = {"MessageId": "bad", "ReceiptHandle": "rbad", "Body": "not json"}
    sqs = FakeSQS([bad, message(1, "jetstream/a.parquet")])
    consumer = make_consumer(sqs, loader, bucket_dir)

    assert consumer.process(consumer.receive_batch()) is True
    assert sqs.deleted == ["r1"]


def test_load_failure_deletes_nothing(loader, bucket_dir):
    (bucket_dir / "jetstream").mkdir(parents=True)
    (bucket_dir / "jetstream/corrupt.parquet").write_text("not parquet")
    sqs = FakeSQS([message(1, "jetstream/corrupt.parquet")])
    consumer = make_consumer(sqs, loader, bucket_dir)

    assert consumer.process(consumer.receive_batch()) is False
    assert sqs.deleted == []
    assert table_files(loader) == []


def test_redelivered_messages_do_not_duplicate_rows(loader, bucket_dir):
    write_raw_file(bucket_dir / "jetstream/a.parquet", [utc(2026, 10, 22, 6, 0)])
    sqs = FakeSQS([message(1, "jetstream/a.parquet"), message(2, "jetstream/a.parquet")])
    consumer = make_consumer(sqs, loader, bucket_dir, max_messages=1)

    assert consumer.process(consumer.receive_batch()) is True
    assert consumer.process(consumer.receive_batch()) is True
    assert loader.spark.table(loader.table).count() == 1
    assert sqs.deleted == ["r1", "r2"]


def test_receive_batch_respects_max_messages(loader, bucket_dir):
    sqs = FakeSQS([message(n, f"jetstream/{n}.parquet") for n in range(25)])
    consumer = make_consumer(sqs, loader, bucket_dir, max_messages=12, batch_window=60)

    assert len(consumer.receive_batch()) == 12
    assert len(consumer.receive_batch()) == 12
    assert len(consumer.receive_batch()) == 1
    assert consumer.receive_batch() == []


def test_s3object_default_uri(loader):
    consumer = QueueConsumer(FakeSQS([]), "q", loader, object_exists=lambda o: True)
    assert consumer.uri_for(S3Object("b", "k")) == "s3://b/k"


def sized_files(bucket_dir, n):
    keys = [f"jetstream/date=2026-10-22/hour=06/{i}.parquet" for i in range(n)]
    uris = [write_raw_file(bucket_dir / k, [utc(2026, 10, 22, 6, i)]) for i, k in enumerate(keys)]
    return keys, uris


def test_large_batches_load_in_byte_capped_commits(loader, bucket_dir):
    keys, uris = sized_files(bucket_dir, 5)
    sqs = FakeSQS([message(i, k, size=60) for i, k in enumerate(keys)])
    consumer = make_consumer(sqs, loader, bucket_dir, max_load_bytes=150)
    commits = []
    load = loader.load
    loader.load = lambda u: commits.append(len(u)) or load(u)

    assert consumer.process(consumer.receive_batch()) is True
    assert commits == [2, 2, 1]
    assert table_files(loader) == sorted(uris)
    assert len(sqs.deleted) == 5


def test_failed_chunk_keeps_only_its_messages(loader, bucket_dir):
    keys, uris = sized_files(bucket_dir, 4)
    msgs = [message(i, k, size=60) for i, k in enumerate(keys)]
    consumer = make_consumer(FakeSQS([]), loader, bucket_dir, max_load_bytes=120)
    load = loader.load
    calls = []

    def flaky(u):
        calls.append(u)
        if len(calls) == 2:
            raise RuntimeError("boom")
        return load(u)

    loader.load = flaky
    assert consumer.process(msgs) is False
    # The first chunk committed, so its messages are gone; the second's stay.
    assert consumer.sqs.deleted == ["r0", "r1"]
    assert table_files(loader) == sorted(uris[:2])

    # Redelivery of what's left loads it once.
    assert consumer.process(msgs[2:]) is True
    assert table_files(loader) == sorted(uris)
    assert consumer.sqs.deleted == ["r0", "r1", "r2", "r3"]


def test_stop_releases_messages_after_the_current_chunk(loader, bucket_dir):
    keys, uris = sized_files(bucket_dir, 4)
    msgs = [message(i, k, size=60) for i, k in enumerate(keys)]
    sqs = FakeSQS([])
    consumer = make_consumer(sqs, loader, bucket_dir, max_load_bytes=60)
    stop = threading.Event()
    load = loader.load
    # The signal arrives during the first commit.
    loader.load = lambda u: stop.set() or load(u)

    assert consumer.process(msgs, stop) is True
    assert table_files(loader) == [uris[0]]
    assert sqs.deleted == ["r0"]
    assert sqs.released == ["r1", "r2", "r3"]


def test_message_files_stay_in_one_chunk(loader, bucket_dir):
    keys, uris = sized_files(bucket_dir, 3)
    # One message with two files over the cap, then a small one.
    msgs = [message(0, keys[0], keys[1], size=100), message(1, keys[2], size=10)]
    consumer = make_consumer(FakeSQS([]), loader, bucket_dir, max_load_bytes=150)
    commits = []
    load = loader.load
    loader.load = lambda u: commits.append(len(u)) or load(u)

    assert consumer.process(msgs) is True
    assert commits == [2, 1]

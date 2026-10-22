import json
from pathlib import Path

import pytest
from conftest import utc, write_raw_file

from bsky_iceberg_loader.consumer import QueueConsumer
from bsky_iceberg_loader.events import S3Object


class FakeSQS:
    def __init__(self, messages):
        self.pending = list(messages)
        self.deleted = []

    def receive_message(self, QueueUrl, MaxNumberOfMessages, WaitTimeSeconds):
        batch, self.pending = self.pending[:MaxNumberOfMessages], self.pending[MaxNumberOfMessages:]
        return {"Messages": batch} if batch else {}

    def delete_message_batch(self, QueueUrl, Entries):
        assert len(Entries) <= 10
        self.deleted.extend(e["ReceiptHandle"] for e in Entries)
        return {"Successful": [{"Id": e["Id"]} for e in Entries], "Failed": []}


def message(n, *keys, bucket="bucket"):
    body = json.dumps(
        {
            "Records": [
                {
                    "eventSource": "aws:s3",
                    "eventName": "ObjectCreated:Put",
                    "s3": {"bucket": {"name": bucket}, "object": {"key": key}},
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

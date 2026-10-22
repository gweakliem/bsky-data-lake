import json

import pytest

from bsky_iceberg_loader.events import MalformedMessage, S3Object, parse_message


def s3_notification(*records):
    return json.dumps(
        {
            "Records": [
                {
                    "eventSource": "aws:s3",
                    "eventName": event,
                    "s3": {"bucket": {"name": bucket}, "object": {"key": key}},
                }
                for event, bucket, key in records
            ]
        }
    )


def test_direct_s3_notification():
    body = s3_notification(
        ("ObjectCreated:Put", "b", "jetstream/date=2026-10-22/hour=06/f1.parquet"),
        ("ObjectCreated:CompleteMultipartUpload", "b", "jetstream/f2.parquet"),
    )
    assert parse_message(body) == [
        S3Object("b", "jetstream/date=2026-10-22/hour=06/f1.parquet"),
        S3Object("b", "jetstream/f2.parquet"),
    ]


def test_keys_are_url_decoded():
    body = s3_notification(("ObjectCreated:Put", "b", "jetstream/date%3D2026-10-22/a+b.parquet"))
    assert parse_message(body) == [S3Object("b", "jetstream/date=2026-10-22/a b.parquet")]


def test_ignores_non_create_events():
    body = s3_notification(("ObjectRemoved:Delete", "b", "k.parquet"))
    assert parse_message(body) == []


def test_test_event():
    assert parse_message(json.dumps({"Service": "Amazon S3", "Event": "s3:TestEvent"})) == []


def test_sns_envelope():
    inner = s3_notification(("ObjectCreated:Put", "b", "k.parquet"))
    body = json.dumps({"Type": "Notification", "Message": inner})
    assert parse_message(body) == [S3Object("b", "k.parquet")]


def test_eventbridge_event():
    body = json.dumps(
        {
            "source": "aws.s3",
            "detail-type": "Object Created",
            "detail": {"bucket": {"name": "b"}, "object": {"key": "jetstream/date=2026-10-22/k.parquet"}},
        }
    )
    assert parse_message(body) == [S3Object("b", "jetstream/date=2026-10-22/k.parquet")]


@pytest.mark.parametrize("body", ["not json", "[]", '{"foo": 1}', '{"Records": [{"eventSource": "aws:s3", "eventName": "ObjectCreated:Put"}]}'])
def test_malformed(body):
    with pytest.raises(MalformedMessage):
        parse_message(body)


def test_uri():
    assert S3Object("b", "a/k.parquet").uri == "s3://b/a/k.parquet"

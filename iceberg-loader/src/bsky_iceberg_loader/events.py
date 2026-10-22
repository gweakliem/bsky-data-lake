"""Parse SQS message bodies carrying S3 object-created notifications."""

from __future__ import annotations

import json
from dataclasses import dataclass
from urllib.parse import unquote_plus


@dataclass(frozen=True)
class S3Object:
    bucket: str
    key: str

    @property
    def uri(self) -> str:
        return f"s3://{self.bucket}/{self.key}"


class MalformedMessage(ValueError):
    """The message body is not a recognizable S3 notification."""


def parse_message(body: str) -> list[S3Object]:
    """Return the objects created according to one SQS message body.

    Handles S3 event notifications sent directly to SQS, the same wrapped in
    an SNS envelope, and EventBridge "Object Created" events. Test events and
    non-create events yield an empty list.
    """
    try:
        doc = json.loads(body)
    except json.JSONDecodeError as e:
        raise MalformedMessage(f"not JSON: {e}") from e
    if not isinstance(doc, dict):
        raise MalformedMessage("not a JSON object")

    # SNS fan-out wraps the S3 notification as a JSON string.
    if doc.get("Type") == "Notification" and "Message" in doc:
        return parse_message(doc["Message"])

    # Sent once when the bucket notification is configured.
    if doc.get("Event") == "s3:TestEvent":
        return []

    if "Records" in doc:
        objects = []
        for record in doc["Records"]:
            if record.get("eventSource") != "aws:s3":
                continue
            if not record.get("eventName", "").startswith("ObjectCreated:"):
                continue
            try:
                bucket = record["s3"]["bucket"]["name"]
                # Keys in S3 notifications are URL-encoded, with spaces as '+'.
                key = unquote_plus(record["s3"]["object"]["key"])
            except (KeyError, TypeError) as e:
                raise MalformedMessage(f"S3 record missing {e}") from e
            objects.append(S3Object(bucket, key))
        return objects

    if doc.get("source") == "aws.s3":
        if doc.get("detail-type") != "Object Created":
            return []
        try:
            # EventBridge keys are not URL-encoded.
            return [S3Object(doc["detail"]["bucket"]["name"], doc["detail"]["object"]["key"])]
        except (KeyError, TypeError) as e:
            raise MalformedMessage(f"EventBridge event missing {e}") from e

    raise MalformedMessage("unrecognized message format")

"""Poll SQS for S3 notifications and load the referenced files in batches."""

from __future__ import annotations

import logging
import threading
import time
from collections.abc import Callable
from typing import Any

from .events import MalformedMessage, S3Object, parse_message
from .loader import RawTableLoader

log = logging.getLogger(__name__)

SQS_MAX_BATCH = 10


class QueueConsumer:
    """Long-polls an SQS queue and appends newly created files to the table.

    Messages are deleted only after the Iceberg commit that includes their
    files succeeds, so failures are retried once the visibility timeout
    expires (and land in the queue's dead-letter queue, if configured, after
    its max receive count). Redelivered files that were already committed are
    skipped by the loader.
    """

    def __init__(
        self,
        sqs: Any,
        queue_url: str,
        loader: RawTableLoader,
        *,
        object_exists: Callable[[S3Object], bool],
        uri_for: Callable[[S3Object], str] = lambda o: o.uri,
        source_prefix: str = "",
        suffix: str = ".parquet",
        max_messages: int = 100,
        batch_window: float = 30.0,
        wait_time: int = 20,
    ):
        self.sqs = sqs
        self.queue_url = queue_url
        self.loader = loader
        self.object_exists = object_exists
        self.uri_for = uri_for
        self.source_prefix = source_prefix
        self.suffix = suffix
        self.max_messages = max_messages
        self.batch_window = batch_window
        self.wait_time = wait_time

    def run(self, stop: threading.Event) -> None:
        """Process batches until `stop` is set."""
        while not stop.is_set():
            messages = self.receive_batch(stop)
            if messages:
                self.process(messages)

    def receive_batch(self, stop: threading.Event | None = None) -> list[dict]:
        """Collect up to `max_messages`, waiting at most `batch_window` seconds
        after the first message arrives. Returns [] after an empty long poll."""
        messages: list[dict] = []
        first_at = None
        while len(messages) < self.max_messages:
            if first_at is not None:
                remaining = self.batch_window - (time.monotonic() - first_at)
                if remaining <= 0:
                    break
                wait = min(self.wait_time, max(1, int(remaining)))
            else:
                wait = self.wait_time
            resp = self.sqs.receive_message(
                QueueUrl=self.queue_url,
                MaxNumberOfMessages=min(SQS_MAX_BATCH, self.max_messages - len(messages)),
                WaitTimeSeconds=wait,
            )
            received = resp.get("Messages", [])
            if not received:
                # Keep waiting out the window only if that was a real long poll;
                # otherwise this would spin on short polls.
                if not messages or wait == 0 or (stop and stop.is_set()):
                    break
                continue
            if first_at is None:
                first_at = time.monotonic()
            messages.extend(received)
            if stop and stop.is_set():
                break
        return messages

    def process(self, messages: list[dict]) -> bool:
        """Load the files referenced by `messages`; returns True on success."""
        done: list[dict] = []
        objects: dict[S3Object, None] = {}  # ordered set
        for msg in messages:
            try:
                parsed = parse_message(msg["Body"])
            except MalformedMessage as e:
                # Left on the queue so it ends up in the dead-letter queue.
                log.error("unparseable message %s: %s", msg.get("MessageId"), e)
                continue
            done.append(msg)
            for obj in parsed:
                if obj.key.startswith(self.source_prefix) and obj.key.endswith(self.suffix):
                    objects[obj] = None
                else:
                    log.debug("ignoring %s", obj.uri)

        present = []
        for obj in objects:
            if self.object_exists(obj):
                present.append(obj)
            else:
                log.warning("skipping %s: object no longer exists", obj.uri)

        uris = [self.uri_for(obj) for obj in present]
        try:
            loaded = self.loader.load(uris)
        except Exception:
            log.exception("failed to load %d file(s); will retry after visibility timeout", len(uris))
            return False

        if uris:
            log.info(
                "appended %d file(s) to %s (%d already loaded)",
                len(loaded),
                self.loader.table,
                len(uris) - len(loaded),
            )
        self._delete(done)
        return True

    def _delete(self, messages: list[dict]) -> None:
        for i in range(0, len(messages), SQS_MAX_BATCH):
            chunk = messages[i : i + SQS_MAX_BATCH]
            resp = self.sqs.delete_message_batch(
                QueueUrl=self.queue_url,
                Entries=[
                    {"Id": str(n), "ReceiptHandle": m["ReceiptHandle"]}
                    for n, m in enumerate(chunk)
                ],
            )
            for failure in resp.get("Failed", []):
                # The files are committed; redelivery will be deduplicated.
                log.warning("failed to delete message: %s", failure)

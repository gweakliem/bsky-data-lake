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


def is_commit_conflict(e: Exception) -> bool:
    """Whether a load failed because another writer committed first."""
    # Sail reports it as e.g. "conflict: Cannot commit catalog-managed table ..."
    return "conflict:" in str(e) or "CommitFailedException" in str(e)


class QueueConsumer:
    """Long-polls an SQS queue and appends newly created files to the table.

    Messages are deleted only after the Iceberg commit that includes their
    files succeeds, so failures are retried once the visibility timeout
    expires (and land in the queue's dead-letter queue, if configured, after
    its max receive count). Redelivered files that were already committed are
    skipped by the loader.

    A batch is loaded in commits of at most `max_load_bytes` of files (by the
    sizes in the notifications), because Sail's write path holds several
    times the input in memory.
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
        max_load_bytes: int = 128 * 2**20,
        batch_window: float = 30.0,
        wait_time: int = 20,
        conflict_retry_delays: tuple[float, ...] = (5, 15, 45),
        sleep: Callable[[float], None] = time.sleep,
    ):
        self.sqs = sqs
        self.queue_url = queue_url
        self.loader = loader
        self.object_exists = object_exists
        self.uri_for = uri_for
        self.source_prefix = source_prefix
        self.suffix = suffix
        self.max_messages = max_messages
        self.max_load_bytes = max_load_bytes
        self.batch_window = batch_window
        self.wait_time = wait_time
        # Sail can't refresh a table, so a retry waits for its catalog cache
        # to pick up the other writer's commit.
        self.conflict_retry_delays = conflict_retry_delays
        self.sleep = sleep

    def run(self, stop: threading.Event) -> None:
        """Process batches until `stop` is set."""
        while not stop.is_set():
            messages = self.receive_batch(stop)
            if messages:
                self.process(messages, stop)

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

    def process(self, messages: list[dict], stop: threading.Event | None = None) -> bool:
        """Load the files referenced by `messages`; returns False if a load failed.

        Messages are grouped into commits of at most `max_load_bytes`, and
        each commit's messages are deleted as soon as it succeeds. If `stop`
        is set between commits, the rest are released back to the queue, so a
        shutdown waits for at most one commit.
        """
        pending: list[tuple[dict, list[S3Object]]] = []
        for msg in messages:
            try:
                parsed = parse_message(msg["Body"])
            except MalformedMessage as e:
                # Left on the queue so it ends up in the dead-letter queue.
                log.error("unparseable message %s: %s", msg.get("MessageId"), e)
                continue
            pending.append((msg, [obj for obj in parsed if self._wanted(obj)]))

        chunks = self._chunks(pending)
        for i, chunk in enumerate(chunks):
            if i and stop is not None and stop.is_set():
                rest = [msg for later in chunks[i:] for msg, _ in later]
                log.info("stopping; releasing %d message(s) back to the queue", len(rest))
                self._release(rest)
                return True
            if not self._load(chunk):
                return False
            self._delete([msg for msg, _ in chunk])
        return True

    def _wanted(self, obj: S3Object) -> bool:
        if not (obj.key.startswith(self.source_prefix) and obj.key.endswith(self.suffix)):
            log.debug("ignoring %s", obj.uri)
            return False
        if not self.object_exists(obj):
            log.warning("skipping %s: object no longer exists", obj.uri)
            return False
        return True

    def _load(self, chunk: list[tuple[dict, list[S3Object]]]) -> bool:
        """Commit one chunk's files. On failure its messages stay on the queue
        and are retried after the visibility timeout."""
        objects = list(dict.fromkeys(obj for _, objs in chunk for obj in objs))
        if not objects:
            return True
        uris = [self.uri_for(obj) for obj in objects]
        started = time.monotonic()
        for attempt, delay in enumerate([*self.conflict_retry_delays, None], start=1):
            try:
                loaded = self.loader.load(uris)
                break
            except Exception as e:
                # Another writer (Glue's compactor) committed first. Retrying
                # is safe: the load skips files that are already in.
                if delay is not None and is_commit_conflict(e):
                    log.warning("commit conflict on attempt %d; retrying in %g s", attempt, delay)
                    self.sleep(delay)
                    continue
                log.exception("failed to load %d file(s); will retry after visibility timeout", len(uris))
                return False
        log.info(
            "appended %d file(s), %.0f MB, to %s in %.1f s (%d already loaded)",
            len(loaded),
            sum(obj.size for obj in objects) / 1e6,
            self.loader.table,
            time.monotonic() - started,
            len(uris) - len(loaded),
        )
        return True

    def _chunks(
        self, pending: list[tuple[dict, list[S3Object]]]
    ) -> list[list[tuple[dict, list[S3Object]]]]:
        # A message's files stay in one chunk. One larger than the cap gets a
        # chunk of its own.
        chunks: list[list[tuple[dict, list[S3Object]]]] = []
        size = 0
        for msg, objects in pending:
            msg_size = sum(obj.size for obj in objects)
            if not chunks or size + msg_size > self.max_load_bytes:
                chunks.append([])
                size = 0
            chunks[-1].append((msg, objects))
            size += msg_size
        return chunks

    def _release(self, messages: list[dict]) -> None:
        """Make messages visible again now, rather than after the visibility timeout."""
        for i in range(0, len(messages), SQS_MAX_BATCH):
            chunk = messages[i : i + SQS_MAX_BATCH]
            resp = self.sqs.change_message_visibility_batch(
                QueueUrl=self.queue_url,
                Entries=[
                    {"Id": str(n), "ReceiptHandle": m["ReceiptHandle"], "VisibilityTimeout": 0}
                    for n, m in enumerate(chunk)
                ],
            )
            for failure in resp.get("Failed", []):
                # Harmless: it reappears once the visibility timeout expires.
                log.warning("failed to release message: %s", failure)

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

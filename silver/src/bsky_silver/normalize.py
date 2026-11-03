"""Turn raw Jetstream events into rows for the silver tables.

Pure Python, so the rules can be tested without Spark. `normalize` maps one
bronze payload to a target table and that table's fields; the job runs it over
Arrow batches (see `job.py`).
"""

from __future__ import annotations

import datetime as dt
import hashlib
import json
from typing import Any

POST = "app.bsky.feed.post"
LIKE = "app.bsky.feed.like"
REPOST = "app.bsky.feed.repost"
FOLLOW = "app.bsky.graph.follow"
BLOCK = "app.bsky.graph.block"
PROFILE = "app.bsky.actor.profile"

# Collections whose records have a strong-ref subject (an AT URI) or a DID subject.
SUBJECT_URI = {LIKE: "likes", REPOST: "reposts"}
SUBJECT_DID = {FOLLOW: "follows", BLOCK: "blocks"}
# Collections with a silver table; deletes are recorded only for these.
TABLED = {POST, PROFILE, *SUBJECT_URI, *SUBJECT_DID}

MENTION = "app.bsky.richtext.facet#mention"
LINK = "app.bsky.richtext.facet#link"
TAG = "app.bsky.richtext.facet#tag"

IGNORED = "ignored"
REJECTS = "rejects"


class Rejected(Exception):
    """The event is malformed; the message is the reason recorded in `rejects`."""


def normalize(payload: str) -> tuple[str, dict[str, Any]]:
    """Map one bronze payload to (target, fields).

    The target is a silver table, `rejects` (fields hold the reason), or
    `ignored` (fields hold the collection or kind, for the run's counts).
    """
    try:
        return _normalize(payload)
    except Rejected as e:
        return REJECTS, {"reason": str(e), "event_id": "reject:" + hashlib.sha256(payload.encode()).hexdigest()}


def _normalize(payload: str) -> tuple[str, dict[str, Any]]:
    try:
        event = json.loads(payload)
    except ValueError:
        raise Rejected("invalid JSON") from None
    if not isinstance(event, dict):
        raise Rejected("event is not an object")
    did = _required_str(event, "did")
    time_us = event.get("time_us")
    if not isinstance(time_us, int) or isinstance(time_us, bool):
        raise Rejected("missing time_us")
    kind = event.get("kind")
    base = {"did": did, "time_us": time_us}

    if kind == "commit":
        return _commit(event, base)
    if kind == "identity":
        identity = _required_dict(event, "identity")
        seq = _optional_int(identity, "seq")
        return "identities", base | {
            "event_id": f"{did}#identity#{seq}#{time_us}",
            "handle": _optional_str(identity, "handle"),
            "seq": seq,
        }
    if kind == "account":
        account = _required_dict(event, "account")
        active = account.get("active")
        if not isinstance(active, bool):
            raise Rejected("account without active")
        seq = _optional_int(account, "seq")
        return "accounts", base | {
            "event_id": f"{did}#account#{seq}#{time_us}",
            "active": active,
            "status": _optional_str(account, "status"),
            "seq": seq,
        }
    return IGNORED, {"collection": f"kind:{kind}"}


def _commit(event: dict, base: dict[str, Any]) -> tuple[str, dict[str, Any]]:
    commit = _required_dict(event, "commit")
    collection = _required_str(commit, "collection", "commit.")
    if collection not in TABLED:
        return IGNORED, {"collection": collection}
    operation = _required_str(commit, "operation", "commit.")
    rkey = _required_str(commit, "rkey", "commit.")
    rev = _required_str(commit, "rev", "commit.")
    did = base["did"]
    row = base | {
        "event_id": f"{did}/{collection}/{rkey}#{rev}",
        "collection": collection,
        "operation": operation,
        "rkey": rkey,
        "rev": rev,
        "uri": f"at://{did}/{collection}/{rkey}",
    }
    if operation == "delete":
        return "deletes", row
    if operation not in ("create", "update"):
        raise Rejected(f"unknown operation {operation!r}")

    record = _required_dict(commit, "record", "commit.")
    row["cid"] = _optional_str(commit, "cid")
    row["created_at"] = _timestamp(record.get("createdAt"))
    if collection == POST:
        return "posts", row | _post(record)
    if collection == PROFILE:
        return "profiles", row | {
            "display_name": _optional_str(record, "displayName"),
            "description": _optional_str(record, "description"),
            "self_labels": _self_labels(record),
            "record": json.dumps(record, ensure_ascii=False, separators=(",", ":")),
        }
    if collection in SUBJECT_URI:
        subject = record.get("subject")
        uri = subject.get("uri") if isinstance(subject, dict) else None
        if not isinstance(uri, str):
            raise Rejected(f"{collection} without subject.uri")
        return SUBJECT_URI[collection], row | {"subject_uri": uri}
    subject = record.get("subject")
    if not isinstance(subject, str):
        raise Rejected(f"{collection} without subject DID")
    return SUBJECT_DID[collection], row | {"subject_did": subject}


def _post(record: dict) -> dict[str, Any]:
    text = _optional_str(record, "text")
    reply = record.get("reply") if isinstance(record.get("reply"), dict) else {}
    embed = record.get("embed") if isinstance(record.get("embed"), dict) else None
    mentions, links, tags = [], [], []
    for facet in _dicts(record.get("facets")):
        for feature in _dicts(facet.get("features")):
            kind = feature.get("$type")
            if kind == MENTION and isinstance(feature.get("did"), str):
                mentions.append(feature["did"])
            elif kind == LINK and isinstance(feature.get("uri"), str):
                links.append(feature["uri"])
            elif kind == TAG and isinstance(feature.get("tag"), str):
                tags.append(feature["tag"])
    # Posts can also carry tags outside the text.
    tags += [t for t in _list(record.get("tags")) if isinstance(t, str)]
    external = _external(embed)
    return {
        "text": text,
        # Missing and empty both mean unknown.
        "langs": [lang for lang in _list(record.get("langs")) if isinstance(lang, str)],
        "reply_root_uri": _ref_uri(reply.get("root")),
        "reply_parent_uri": _ref_uri(reply.get("parent")),
        "embed_type": embed.get("$type") if embed and isinstance(embed.get("$type"), str) else None,
        "quote_uri": _quote_uri(embed),
        "image_alts": _image_alts(embed),
        "external_uri": _optional_str(external, "uri") if external else None,
        "external_title": _optional_str(external, "title") if external else None,
        "mention_dids": _unique(mentions),
        "link_uris": _unique(links),
        "tags": _unique(tags),
        "self_labels": _self_labels(record),
        "record": json.dumps(record, ensure_ascii=False, separators=(",", ":")),
    }


def _quote_uri(embed: dict | None) -> str | None:
    if not embed:
        return None
    kind = embed.get("$type")
    if kind == "app.bsky.embed.record":
        return _ref_uri(embed.get("record"))
    if kind == "app.bsky.embed.recordWithMedia":
        inner = embed.get("record")
        return _ref_uri(inner.get("record")) if isinstance(inner, dict) else None
    return None


def _media(embed: dict | None) -> dict | None:
    """The media part of an embed: itself, or recordWithMedia's `media`."""
    if not embed:
        return None
    if embed.get("$type") == "app.bsky.embed.recordWithMedia":
        media = embed.get("media")
        return media if isinstance(media, dict) else None
    return embed


def _image_alts(embed: dict | None) -> list[str]:
    media = _media(embed)
    if not media:
        return []
    kind = media.get("$type")
    # Galleries hold their images under `items`, not `images`.
    images = media.get("images") if kind == "app.bsky.embed.images" else media.get("items") if kind == "app.bsky.embed.gallery" else None
    return [i["alt"] for i in _dicts(images) if isinstance(i.get("alt"), str) and i["alt"]]


def _external(embed: dict | None) -> dict | None:
    media = _media(embed)
    if media and media.get("$type") == "app.bsky.embed.external" and isinstance(media.get("external"), dict):
        return media["external"]
    return None


def _self_labels(record: dict) -> list[str]:
    labels = record.get("labels")
    if not isinstance(labels, dict):
        return []
    return _unique(v["val"] for v in _dicts(labels.get("values")) if isinstance(v.get("val"), str))


def _ref_uri(ref: Any) -> str | None:
    return ref["uri"] if isinstance(ref, dict) and isinstance(ref.get("uri"), str) else None


def _timestamp(value: Any) -> dt.datetime | None:
    """createdAt is client-supplied: unparseable values become null rather than rejects."""
    if not isinstance(value, str):
        return None
    try:
        parsed = dt.datetime.fromisoformat(value)
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=dt.timezone.utc)
    try:
        return parsed.astimezone(dt.timezone.utc)
    except OverflowError:
        return None


def _required_dict(obj: dict, key: str, prefix: str = "") -> dict:
    value = obj.get(key)
    if not isinstance(value, dict):
        raise Rejected(f"missing {prefix}{key}")
    return value


def _required_str(obj: dict, key: str, prefix: str = "") -> str:
    value = obj.get(key)
    if not isinstance(value, str) or not value:
        raise Rejected(f"missing {prefix}{key}")
    return value


def _optional_str(obj: dict, key: str) -> str | None:
    value = obj.get(key)
    if value is not None and not isinstance(value, str):
        raise Rejected(f"{key} is not a string")
    return value


def _optional_int(obj: dict, key: str) -> int | None:
    value = obj.get(key)
    return value if isinstance(value, int) and not isinstance(value, bool) else None


def _list(value: Any) -> list:
    return value if isinstance(value, list) else []


def _dicts(value: Any) -> list[dict]:
    return [v for v in _list(value) if isinstance(v, dict)]


def _unique(values) -> list[str]:
    return list(dict.fromkeys(values))

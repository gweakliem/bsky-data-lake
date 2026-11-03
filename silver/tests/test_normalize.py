import datetime as dt
import json

import pytest
from conftest import fixtures

from bsky_silver.normalize import IGNORED, REJECTS, normalize


def expected_target(case: str) -> str:
    if case.endswith("_delete"):
        return "deletes"
    if case.startswith("post_"):
        return "posts"
    if case.startswith("profile_"):
        return "profiles"
    if case.startswith("account_"):
        return "accounts"
    return {
        "like": "likes",
        "repost": "reposts",
        "follow": "follows",
        "block": "blocks",
        "identity": "identities",
    }.get(case, IGNORED)


@pytest.mark.parametrize("case,event", fixtures(), ids=lambda v: v if isinstance(v, str) else "")
def test_every_fixture_lands_in_its_table(case, event):
    target, fields = normalize(json.dumps(event))
    assert target == expected_target(case), fields


def post(fields: dict, **commit) -> str:
    """A post-create event; `fields` merge into the record, `commit` overrides commit keys."""
    return json.dumps(
        {
            "did": "did:plc:author",
            "time_us": 1761998400000000,
            "kind": "commit",
            "commit": {
                "rev": "3m4abc",
                "operation": "create",
                "collection": "app.bsky.feed.post",
                "rkey": "3m4rkey",
                "cid": "bafycid",
                "record": {"$type": "app.bsky.feed.post", "createdAt": "2026-11-01T12:00:00.000Z", "text": "hi"} | fields,
            }
            | commit,
        }
    )


def test_post_identity_and_times():
    target, row = normalize(post({}))
    assert target == "posts"
    assert row["uri"] == "at://did:plc:author/app.bsky.feed.post/3m4rkey"
    assert row["event_id"] == "did:plc:author/app.bsky.feed.post/3m4rkey#3m4abc"
    assert row["created_at"] == dt.datetime(2026, 11, 1, 12, tzinfo=dt.timezone.utc)
    assert row["langs"] == [] and row["embed_type"] is None and row["reply_root_uri"] is None


def test_reply_uris():
    _, row = normalize(post({"reply": {"root": {"uri": "at://r/p/1", "cid": "c"}, "parent": {"uri": "at://p/p/2", "cid": "c"}}}))
    assert (row["reply_root_uri"], row["reply_parent_uri"]) == ("at://r/p/1", "at://p/p/2")


def test_quote_and_quote_with_media():
    _, row = normalize(post({"embed": {"$type": "app.bsky.embed.record", "record": {"uri": "at://q/p/1", "cid": "c"}}}))
    assert (row["embed_type"], row["quote_uri"]) == ("app.bsky.embed.record", "at://q/p/1")

    _, row = normalize(
        post(
            {
                "embed": {
                    "$type": "app.bsky.embed.recordWithMedia",
                    "record": {"$type": "app.bsky.embed.record", "record": {"uri": "at://q/p/2", "cid": "c"}},
                    "media": {"$type": "app.bsky.embed.images", "images": [{"alt": "a cat"}, {"alt": ""}]},
                }
            }
        )
    )
    assert row["quote_uri"] == "at://q/p/2"
    assert row["image_alts"] == ["a cat"]


def test_gallery_alts_come_from_items():
    _, row = normalize(post({"embed": {"$type": "app.bsky.embed.gallery", "items": [{"alt": "one"}, {"alt": "two"}]}}))
    assert row["image_alts"] == ["one", "two"]


def test_external_link():
    _, row = normalize(post({"embed": {"$type": "app.bsky.embed.external", "external": {"uri": "https://e.x/a", "title": "A"}}}))
    assert (row["external_uri"], row["external_title"]) == ("https://e.x/a", "A")


def test_facets_and_record_tags():
    facets = [
        {"features": [{"$type": "app.bsky.richtext.facet#mention", "did": "did:plc:m"}]},
        {"features": [{"$type": "app.bsky.richtext.facet#link", "uri": "https://l.x"}]},
        {"features": [{"$type": "app.bsky.richtext.facet#tag", "tag": "art"}, {"$type": "com.objkt.richtext.facet#token"}]},
        {"features": [{"$type": "app.bsky.richtext.facet#tag", "tag": "art"}]},
    ]
    _, row = normalize(post({"facets": facets, "tags": ["extra"]}))
    assert row["mention_dids"] == ["did:plc:m"]
    assert row["link_uris"] == ["https://l.x"]
    assert row["tags"] == ["art", "extra"]


def test_self_labels_and_langs():
    _, row = normalize(post({"labels": {"$type": "com.atproto.label.defs#selfLabels", "values": [{"val": "bot"}]}, "langs": ["en", "ja"]}))
    assert row["self_labels"] == ["bot"]
    assert row["langs"] == ["en", "ja"]


def test_unparseable_created_at_is_null_not_rejected():
    target, row = normalize(post({"createdAt": "yesterday"}))
    assert target == "posts" and row["created_at"] is None


def test_delete_has_no_record():
    target, row = normalize(post({}, operation="delete", record=None))
    assert target == "deletes"
    assert row["collection"] == "app.bsky.feed.post"
    assert row["uri"] == "at://did:plc:author/app.bsky.feed.post/3m4rkey"


@pytest.mark.parametrize(
    "payload,reason",
    [
        ("{not json", "invalid JSON"),
        ("[1, 2]", "event is not an object"),
        ('{"time_us": 1, "kind": "commit"}', "missing did"),
        ('{"did": "did:plc:x", "kind": "commit"}', "missing time_us"),
        (post({}, rkey=None), "missing commit.rkey"),
        (post({}, record=None), "missing commit.record"),
        (post({"text": 42}), "text is not a string"),
        (post({}, operation="mutate"), "unknown operation 'mutate'"),
        (post({}, collection="app.bsky.feed.like", record={"subject": "nope"}), "app.bsky.feed.like without subject.uri"),
        (post({}, collection="app.bsky.graph.follow", record={"subject": {"uri": "x"}}), "app.bsky.graph.follow without subject DID"),
        ('{"did": "did:plc:x", "time_us": 1, "kind": "account", "account": {"status": "deleted"}}', "account without active"),
    ],
)
def test_malformed_events_are_rejected_with_a_reason(payload, reason):
    target, fields = normalize(payload)
    assert target == REJECTS
    assert fields["reason"] == reason
    assert fields["event_id"].startswith("reject:")


def test_unknown_collections_and_kinds_are_ignored():
    assert normalize(post({}, collection="app.bsky.feed.threadgate")) == (IGNORED, {"collection": "app.bsky.feed.threadgate"})
    assert normalize('{"did": "did:plc:x", "time_us": 1, "kind": "sync"}') == (IGNORED, {"collection": "kind:sync"})

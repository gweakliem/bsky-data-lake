"""Stratified sample of one day's events from a bronze_extract.py file, as
JSON lines of {"case", "event"}. Sampling is deterministic (ordered by hash).

    uv run --with duckdb python bronze_fixtures.py bronze.duckdb 2026-11-01 ../tests/fixtures/jetstream.jsonl
"""
import json, sys
import duckdb

db, DAY, OUT = sys.argv[1:4]
con = duckdb.connect(db, read_only=True)
con.sql("SET temp_directory = ''")
con.sql(f"""CREATE TEMP VIEW e AS SELECT collection, operation, kind, payload, payload->'$.commit.record' rec
  FROM events WHERE day = DATE '{DAY}' AND payload IS NOT NULL""")
# Posts must be safe to publish: no adult self-labels, nothing that looks like contact details.
SAFE = r"""NOT regexp_matches(coalesce(rec->>'$.text', ''), '[\w.+-]+@[\w-]+\.\w|\+?\d[\d ().-]{8,}\d')
  AND NOT regexp_matches(lower(concat_ws(' ', rec->>'$.text', rec->>'$.description', rec->>'$.displayName')),
                         'sex|porn|nsfw|nude|naked|strip|bbw|cock|dick|cum|fuck|shit|depravity|onlyfans|セックス|エロ')
  AND NOT list_has_any(coalesce(json_extract_string(rec, '$.labels.values[*].val'), []),
                       ['porn', 'sexual', 'nudity', 'graphic-media', 'suggestive', 'nsfw', 'adult', 'sexy', 'gore'])"""
POST = f"collection = 'app.bsky.feed.post' AND operation = 'create' AND {SAFE}"
strata = {
    # Posts: the shapes silver has to extract.
    "post_plain": (f"{POST} AND NOT json_exists(rec, '$.reply') AND NOT json_exists(rec, '$.embed') AND NOT json_exists(rec, '$.facets')", 15),
    "post_reply": (f"{POST} AND json_exists(rec, '$.reply')", 15),
    "post_images": (f"{POST} AND rec->>'$.embed.\"$type\"' = 'app.bsky.embed.images'", 8),
    "post_external": (f"{POST} AND rec->>'$.embed.\"$type\"' = 'app.bsky.embed.external'", 8),
    "post_quote": (f"{POST} AND rec->>'$.embed.\"$type\"' = 'app.bsky.embed.record'", 8),
    "post_quote_images": (f"{POST} AND rec->>'$.embed.media.\"$type\"' = 'app.bsky.embed.images'", 3),
    "post_quote_external": (f"{POST} AND rec->>'$.embed.media.\"$type\"' = 'app.bsky.embed.external'", 3),
    "post_quote_video": (f"{POST} AND rec->>'$.embed.media.\"$type\"' = 'app.bsky.embed.video'", 2),
    "post_video": (f"{POST} AND rec->>'$.embed.\"$type\"' = 'app.bsky.embed.video'", 5),
    "post_gallery": (f"{POST} AND rec->>'$.embed.\"$type\"' = 'app.bsky.embed.gallery'", 3),
    "post_embed_nonstandard": (f"{POST} AND json_exists(rec, '$.embed') AND rec->>'$.embed.\"$type\"' NOT IN ('app.bsky.embed.images','app.bsky.embed.external','app.bsky.embed.record','app.bsky.embed.recordWithMedia','app.bsky.embed.video','app.bsky.embed.gallery')", 3),
    "post_mention": (f"{POST} AND list_contains(json_extract_string(rec, '$.facets[*].features[*].\"$type\"'), 'app.bsky.richtext.facet#mention')", 5),
    "post_link": (f"{POST} AND list_contains(json_extract_string(rec, '$.facets[*].features[*].\"$type\"'), 'app.bsky.richtext.facet#link')", 5),
    "post_tag": (f"{POST} AND list_contains(json_extract_string(rec, '$.facets[*].features[*].\"$type\"'), 'app.bsky.richtext.facet#tag')", 5),
    "post_facet_nonstandard": (f"{POST} AND len(list_filter(json_extract_string(rec, '$.facets[*].features[*].\"$type\"'), lambda x: x NOT IN ('app.bsky.richtext.facet#mention','app.bsky.richtext.facet#link','app.bsky.richtext.facet#tag'))) > 0", 3),
    "post_self_labels": (f"{POST} AND json_exists(rec, '$.labels')", 3),
    "post_multi_lang": (f"{POST} AND json_array_length(rec->'$.langs') > 1", 3),
    "post_no_langs": (f"{POST} AND NOT json_exists(rec, '$.langs')", 3),
    "post_empty_text": (f"{POST} AND coalesce(rec->>'$.text', '') = ''", 3),
    "post_long_text": (f"{POST} AND length(rec->>'$.text') > 300", 2),
    "post_backdated": (f"{POST} AND try_cast(rec->>'$.createdAt' AS TIMESTAMPTZ) < now() - INTERVAL 30 DAY", 2),
    "post_update": (f"collection = 'app.bsky.feed.post' AND operation = 'update' AND {SAFE}", 3),
    "post_delete": ("collection = 'app.bsky.feed.post' AND operation = 'delete'", 5),
    # Other collections above 0.1%, creates and deletes.
    "like": ("collection = 'app.bsky.feed.like' AND operation = 'create'", 8),
    "like_delete": ("collection = 'app.bsky.feed.like' AND operation = 'delete'", 3),
    "repost": ("collection = 'app.bsky.feed.repost' AND operation = 'create'", 5),
    "repost_delete": ("collection = 'app.bsky.feed.repost' AND operation = 'delete'", 2),
    "follow": ("collection = 'app.bsky.graph.follow' AND operation = 'create'", 5),
    "follow_delete": ("collection = 'app.bsky.graph.follow' AND operation = 'delete'", 2),
    "block": ("collection = 'app.bsky.graph.block' AND operation = 'create'", 4),
    "block_delete": ("collection = 'app.bsky.graph.block' AND operation = 'delete'", 2),
    "threadgate": ("collection = 'app.bsky.feed.threadgate'", 3),
    "postgate": ("collection = 'app.bsky.feed.postgate'", 3),
    "listitem": ("collection = 'app.bsky.graph.listitem'", 3),
    "profile_create": (f"collection = 'app.bsky.actor.profile' AND operation = 'create' AND {SAFE}", 3),
    "profile_update": (f"collection = 'app.bsky.actor.profile' AND operation = 'update' AND {SAFE}", 4),
    "profile_delete": ("collection = 'app.bsky.actor.profile' AND operation = 'delete'", 1),
    # Identity and account events, every account status.
    "identity": ("kind = 'identity'", 5),
    "account_active": ("kind = 'account' AND payload->>'$.account.active' = 'true'", 3),
    **{f"account_{s}": (f"kind = 'account' AND payload->>'$.account.status' = '{s}'", 2) for s in ["takendown", "deleted", "throttled", "deactivated"]},
    # Collections silver ignores (counted, not loaded).
    "unknown_collection": (f"kind = 'commit' AND collection NOT LIKE 'app.bsky.%' AND {SAFE}", 5),
}
out = open(OUT, "w")
total = 0
for name, (where, n) in strata.items():
    rows = con.sql(f"SELECT payload FROM e WHERE {where} ORDER BY hash(payload) LIMIT {n}").fetchall()
    for (p,) in rows:
        out.write(json.dumps({"case": name, "event": json.loads(p)}, ensure_ascii=False, separators=(",", ":")) + "\n")
    total += len(rows)
    if len(rows) < n:
        print(f"{name}: only {len(rows)} of {n}")
print("total", total)

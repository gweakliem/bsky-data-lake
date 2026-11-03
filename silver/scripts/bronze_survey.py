"""S1 survey queries over a bronze_extract.py file. Per-post numbers use one day.

    uv run --with duckdb python bronze_survey.py bronze.duckdb 2026-11-01
"""

import sys

import duckdb

db, DAY = sys.argv[1], sys.argv[2]
con = duckdb.connect(db, read_only=True)
# Fail rather than spill: the extract is already large.
con.sql("SET temp_directory = ''")


def show(title, sql):
    print(f"\n## {title}")
    rel = con.sql(sql)
    cols = rel.columns
    rows = rel.fetchall()
    print("| " + " | ".join(cols) + " |")
    print("|" + "---|" * len(cols))
    for r in rows:
        print("| " + " | ".join(f"{v:,}" if isinstance(v, int) else (f"{v:.3f}" if isinstance(v, float) else str(v)) for v in r) + " |")


con.sql(f"""CREATE TEMP VIEW posts AS
  SELECT day, time_us, payload,
         payload->'$.commit.record' AS rec,
         payload->>'$.did' AS did, payload->>'$.commit.rkey' AS rkey, payload->>'$.commit.rev' AS rev
  FROM events WHERE collection = 'app.bsky.feed.post' AND operation = 'create' AND day = DATE '{DAY}'""")

show(f"Distinct collections on {DAY}, and how many are above 0.1%", f"""WITH c AS (SELECT collection, count(*) n FROM events WHERE kind = 'commit' AND day = DATE '{DAY}' GROUP BY 1)
       SELECT count(*) collections, count(*) FILTER (WHERE n > 0.001 * (SELECT sum(n) FROM c)) above_0_1_pct FROM c""")
show("Events per day by kind", "SELECT day, kind, count(*) n FROM events GROUP BY ALL ORDER BY day, n DESC")
show(
    "Collection x operation (both days)",
    """SELECT coalesce(collection, '(' || kind || ')') collection, operation, count(*) n,
              round(100.0 * count(*) / sum(count(*)) OVER (), 3) pct
       FROM events GROUP BY ALL ORDER BY n DESC LIMIT 30""",
)
show(
    "Post operations per day",
    """SELECT day, operation, count(*) n FROM events WHERE collection = 'app.bsky.feed.post'
       GROUP BY ALL ORDER BY day, operation""",
)
show(
    "Optional post fields (creates, share of posts)",
    """SELECT count(*) posts,
         avg((json_exists(rec, '$.reply'))::int) reply,
         avg((json_exists(rec, '$.embed'))::int) embed,
         avg((json_exists(rec, '$.facets'))::int) facets,
         avg((coalesce(json_array_length(rec->'$.langs'), 0) > 0)::int) langs,
         avg((json_exists(rec, '$.labels'))::int) self_labels,
         avg((coalesce(length(rec->>'$.text'), 0) = 0)::int) empty_text
       FROM posts""",
)
show(
    "Embed types",
    """SELECT rec->>'$.embed."$type"' embed_type, count(*) n, round(100.0 * count(*) / (SELECT count(*) FROM posts), 2) pct_posts
       FROM posts WHERE json_exists(rec, '$.embed') GROUP BY 1 ORDER BY n DESC""",
)
show(
    "recordWithMedia media types",
    """SELECT rec->>'$.embed.media."$type"' media_type, count(*) n FROM posts
       WHERE rec->>'$.embed."$type"' = 'app.bsky.embed.recordWithMedia' GROUP BY 1 ORDER BY n DESC""",
)
show(
    "Facet features (posts with at least one)",
    """WITH f AS (
         SELECT DISTINCT row_id, unnest(json_extract_string(rec, '$.facets[*].features[*]."$type"')) feature
         FROM (SELECT row_number() OVER () row_id, rec FROM posts WHERE json_exists(rec, '$.facets')))
       SELECT feature, count(*) posts, round(100.0 * count(*) / (SELECT count(*) FROM posts), 2) pct_posts
       FROM f GROUP BY 1 ORDER BY posts DESC""",
)
show(
    "Top languages (first lang)",
    """SELECT coalesce(rec->>'$.langs[0]', '(none)') lang, count(*) n, round(100.0 * count(*) / (SELECT count(*) FROM posts), 2) pct
       FROM posts GROUP BY 1 ORDER BY n DESC LIMIT 12""",
)
show(
    "Posts with several langs",
    "SELECT json_array_length(rec->'$.langs') n_langs, count(*) n FROM posts GROUP BY 1 ORDER BY 1",
)
show(
    "Self-label values",
    """SELECT unnest(json_extract_string(rec, '$.labels.values[*].val')) val, count(*) n
       FROM posts WHERE json_exists(rec, '$.labels') GROUP BY 1 ORDER BY n DESC""",
)
show(
    "Text length (chars) percentiles",
    """SELECT quantile_cont(length(rec->>'$.text'), [0.5, 0.9, 0.99]) p50_p90_p99, max(length(rec->>'$.text')) max_len,
              avg(length(rec->>'$.text')) mean FROM posts""",
)
show(
    "createdAt vs event time (posts)",
    """WITH d AS (SELECT epoch_us(try_cast(rec->>'$.createdAt' AS TIMESTAMPTZ)) - time_us AS skew_us FROM posts)
       SELECT count(*) FILTER (WHERE skew_us IS NULL) unparseable,
              count(*) FILTER (WHERE skew_us > 300e6) future_over_5min,
              count(*) FILTER (WHERE skew_us < -86400e6) older_than_1day,
              quantile_cont(skew_us / 1e6, [0.01, 0.5, 0.99]) p01_p50_p99_secs FROM d""",
)
show(
    "Duplicate post events (same did, rkey, rev)",
    """SELECT count(*) events, count(DISTINCT (did, rkey, rev)) distinct_events,
              count(*) - count(DISTINCT (did, rkey, rev)) duplicates FROM posts""",
)
show(
    "Unknown / non-app.bsky collections",
    f"""SELECT split_part(collection, '.', 1) || '.' || split_part(collection, '.', 2) ns, count(*) n,
              count(DISTINCT collection) collections
       FROM events WHERE kind = 'commit' AND day = DATE '{DAY}' GROUP BY 1 ORDER BY n DESC LIMIT 15""",
)
show(
    "Identity and account events",
    """SELECT kind, payload->>'$.account.status' status, payload->>'$.account.active' active, count(*) n
       FROM events WHERE kind IN ('identity', 'account') GROUP BY ALL ORDER BY n DESC""",
)

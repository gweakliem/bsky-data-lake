"""Draw the evaluation sample: eligible English posts from one day, stratified.

Random sampling yields almost no spam, adult or machine-written posts, so most
strata are drawn from signals that make them likelier. Metrics over the whole
sample are therefore not population rates; they're for choosing thresholds.

Writes eval/sample.jsonl (committed: URIs and strata only, the repo is public)
and eval/.cache/posts.jsonl (git-ignored: each post's state for labeling and
evaluation).

    uv run python -m bsky_enrich.sample 2026-11-09
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

from bsky_enrich import lake, spec

EVAL = Path(__file__).resolve().parents[2] / "eval"
SAMPLE = EVAL / "sample.jsonl"
CACHE = EVAL / ".cache"

ADULT_LABELS = ["porn", "sexual", "nudity", "graphic-media"]
ADULT_TAGS = ["nsfw", "porn", "nsfwart", "onlyfans", "nude", "hentai", "lewd", "adult"]

# Stratum -> (posts, WHERE over the candidate table). One post per account per stratum.
STRATA: dict[str, tuple[int, str]] = {
    "random": (120, "TRUE"),
    "adult_signal": (
        50,
        f"(list_has_any(self_labels, {ADULT_LABELS}) OR list_has_any(list_transform(tags, t -> lower(t)), {ADULT_TAGS}))",
    ),
    "high_volume": (50, "author_posts >= 100"),
    "shared_link": (30, "link_accounts >= 20"),
    "many_tags": (25, "len(tags) >= 5"),
    # Long, polished text from prolific accounts: a weak signal, but random sampling finds almost none.
    "ai_signal": (25, "length(text) >= 250 AND author_posts >= 20"),
}


def draw(con, day: str, seed: float = 0.42) -> list[dict]:
    s = lake.SILVER
    con.execute(f"SELECT setseed({seed})")
    con.execute(
        f"""CREATE OR REPLACE TEMP TABLE candidates AS
        WITH p AS (
            SELECT uri, did, text, langs, image_alts, external_uri, external_title, quote_uri, reply_parent_uri,
                reply_root_uri, tags, self_labels
            FROM {s}.posts
            WHERE day = DATE '{day}' AND operation = 'create'
                -- Posts already deleted can't be checked on bsky.app; later deletes still get through.
                AND uri NOT IN (SELECT uri FROM {s}.deletes WHERE day >= DATE '{day}' AND collection = 'app.bsky.feed.post')
                AND list_has_any(list_transform(langs, l -> lower(split_part(l, '-', 1))), ['en'])
                AND {spec.ELIGIBLE_SQL}
        )
        SELECT p.*, count(*) OVER (PARTITION BY did) AS author_posts,
            CASE WHEN external_uri IS NULL THEN 0 ELSE count(DISTINCT did) OVER (PARTITION BY external_uri) END AS link_accounts
        FROM p"""
    )
    picked: list[dict] = []
    seen: set[str] = set()
    for stratum, (n, where) in STRATA.items():
        relation = con.sql(
            f"""SELECT * FROM (
                    SELECT *, row_number() OVER (PARTITION BY did ORDER BY random()) AS per_author
                    FROM candidates WHERE {where}
                ) WHERE per_author = 1 ORDER BY random() LIMIT {n * 2}"""
        )
        cols, rows = relation.columns, relation.fetchall()
        taken = 0
        for row in rows:
            post = dict(zip(cols, row))
            if post["uri"] in seen or taken == n:
                continue
            seen.add(post["uri"])
            picked.append({"stratum": stratum, **post})
            taken += 1
        print(f"{stratum}: {taken}", file=sys.stderr)
    return picked


def context(con, day: str, posts: list[dict]) -> dict[str, dict]:
    """Handle, display name, and quoted and parent post text, by post URI."""
    s = lake.SILVER
    con.execute(
        "CREATE OR REPLACE TEMP TABLE picked (uri VARCHAR, did VARCHAR, quote_uri VARCHAR, reply_parent_uri VARCHAR, reply_root_uri VARCHAR)"
    )
    con.executemany(
        "INSERT INTO picked VALUES (?, ?, ?, ?, ?)",
        [[p["uri"], p["did"], p["quote_uri"], p["reply_parent_uri"], p["reply_root_uri"]] for p in posts],
    )
    # The latest handle and profile seen up to the post's day, and referenced posts from the week before.
    rows = con.sql(
        f"""
        WITH handles AS (
            SELECT did, arg_max(handle, time_us) AS handle FROM {s}.identities
            WHERE day <= DATE '{day}' AND did IN (SELECT did FROM picked) GROUP BY did
        ), names AS (
            SELECT did, arg_max(display_name, time_us) AS display_name FROM {s}.profiles
            WHERE day <= DATE '{day}' AND did IN (SELECT did FROM picked) GROUP BY did
        ), referenced AS (
            SELECT uri, arg_max(text, rev) AS text FROM {s}.posts
            WHERE day BETWEEN DATE '{day}' - 7 AND DATE '{day}'
                AND uri IN (SELECT quote_uri FROM picked UNION SELECT reply_parent_uri FROM picked UNION SELECT reply_root_uri FROM picked)
            GROUP BY uri
        )
        SELECT picked.uri, h.handle, n.display_name, q.text, r.text, t.text
        FROM picked
        LEFT JOIN handles h USING (did)
        LEFT JOIN names n USING (did)
        LEFT JOIN referenced q ON q.uri = picked.quote_uri
        LEFT JOIN referenced r ON r.uri = picked.reply_parent_uri
        LEFT JOIN referenced t ON t.uri = picked.reply_root_uri"""
    ).fetchall()
    return {
        uri: {"handle": h, "display_name": n, "quoted_text": q, "parent_text": r, "root_text": t}
        for uri, h, n, q, r, t in rows
    }


def refresh(con) -> list[dict]:
    """The committed sample's posts again, for rebuilding their state after the spec changes."""
    sample = [json.loads(line) for line in SAMPLE.open()]
    con.execute("CREATE OR REPLACE TEMP TABLE wanted (uri VARCHAR, stratum VARCHAR, day DATE)")
    con.executemany("INSERT INTO wanted VALUES (?, ?, ?)", [[r["uri"], r["stratum"], r["day"]] for r in sample])
    days = sorted({r["day"] for r in sample})
    relation = con.sql(
        f"""SELECT w.stratum, p.uri, p.did, p.text, p.langs, p.image_alts, p.external_uri, p.external_title, p.quote_uri,
                p.reply_parent_uri, p.reply_root_uri, p.tags, p.self_labels
            FROM {lake.SILVER}.posts p JOIN wanted w ON w.uri = p.uri AND w.day = p.day
            WHERE p.day IN ({", ".join(f"DATE '{d}'" for d in days)}) AND p.operation = 'create'"""
    )
    by_uri = {row[1]: dict(zip(relation.columns, row)) for row in relation.fetchall()}
    return [by_uri[r["uri"]] for r in sample if r["uri"] in by_uri]


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("day", nargs="?", help="UTC day to sample, YYYY-MM-DD")
    parser.add_argument("--refresh", action="store_true", help="rebuild the cached state of the committed sample (after a spec change)")
    args = parser.parse_args()
    con = lake.connect()
    if args.refresh:
        posts = refresh(con)
        day = json.loads(SAMPLE.open().readline())["day"]
    else:
        if not args.day:
            parser.error("give the day to sample, or --refresh")
        if SAMPLE.exists():
            sys.exit(f"{SAMPLE} exists; the eval set is drawn once. Use --refresh to rebuild its state, or delete it to redraw.")
        day = args.day
        posts = draw(con, day)
        with SAMPLE.open("w") as committed:
            for p in posts:
                committed.write(json.dumps({"uri": p["uri"], "stratum": p["stratum"], "day": day}) + "\n")
    ctx = context(con, day, posts)
    CACHE.mkdir(parents=True, exist_ok=True)
    with (CACHE / "posts.jsonl").open("w") as cache:
        for p in posts:
            record = {k: v for k, v in p.items() if k != "per_author"}
            record["state"] = spec.state(p, **ctx.get(p["uri"], {}))
            cache.write(json.dumps(record, default=str) + "\n")
    print(f"{len(posts)} posts' state -> {CACHE / 'posts.jsonl'}", file=sys.stderr)


if __name__ == "__main__":
    main()

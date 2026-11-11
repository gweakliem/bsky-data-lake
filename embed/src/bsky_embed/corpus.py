"""Draw the E1 corpus: a random slice of one day's eligible posts, every language.

Posts already deleted are left out. The C1 eval sample is added on top, so its
hand topic labels can score the clusters. Text follows text_spec v1: post text,
image alt texts and the link title, one per line; never the reply parent.

    uv run python -m bsky_embed.corpus            # about 2 min; writes eval/.cache/corpus.jsonl
"""

from __future__ import annotations

import json
import sys
from pathlib import Path

from bsky_embed.spec import text
from bsky_enrich import lake, spec
from bsky_enrich.sample import SAMPLE

EVAL = Path(__file__).resolve().parents[2] / "eval"
CACHE = EVAL / ".cache"
CORPUS = CACHE / "corpus.jsonl"
DAY = "2026-11-09"
SIZE = 100_000
SEED = "e1"
TEXT_SPEC = "v1"  # bsky_embed.spec.text


def load() -> list[dict]:
    return [json.loads(line) for line in CORPUS.open()]


def main() -> None:
    con = lake.connect()
    con.execute("SET threads = 8")  # more parallel S3 reads than this fail to connect
    s = lake.SILVER
    c1 = [json.loads(line)["uri"] for line in SAMPLE.open()]
    con.execute("CREATE TEMP TABLE c1 (uri VARCHAR)")
    con.executemany("INSERT INTO c1 VALUES (?)", [[u] for u in c1])
    relation = con.sql(
        f"""WITH p AS (
                SELECT uri, did, langs, text, image_alts, external_title FROM {s}.posts
                WHERE day = DATE '{DAY}' AND operation = 'create' AND {spec.ELIGIBLE_SQL}
            ),
            live AS (
                SELECT * FROM p WHERE uri NOT IN (
                    SELECT uri FROM {s}.deletes WHERE day >= DATE '{DAY}' AND collection = 'app.bsky.feed.post')
            )
            (SELECT *, false AS c1 FROM live WHERE uri NOT IN (SELECT uri FROM c1) ORDER BY hash(uri || '{SEED}') LIMIT {SIZE})
            UNION ALL
            SELECT *, true AS c1 FROM p WHERE uri IN (SELECT uri FROM c1)"""
    )
    rows = [dict(zip(relation.columns, r)) for r in relation.fetchall()]
    CACHE.mkdir(parents=True, exist_ok=True)
    with CORPUS.open("w") as out:
        for r in sorted(rows, key=lambda r: r["uri"]):
            lang = (r["langs"] or [None])[0]
            out.write(json.dumps({
                "uri": r["uri"], "did": r["did"], "lang": lang.split("-")[0].lower() if lang else None,
                "c1": r["c1"], "text": text(r),
            }) + "\n")
    print(f"{len(rows)} posts ({sum(r['c1'] for r in rows)} from the C1 sample) -> {CORPUS}", file=sys.stderr)


if __name__ == "__main__":
    main()

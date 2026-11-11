"""Judge whether pooled search results are relevant: Jev for every pair, a hand sample to check Jev.

The pool is the union of every system's top 10 per query (search.py). Jev
answers one Noul per (query, post), in any language. Hand judgments are drawn
from English queries and English posts, spread over Jev's probabilities, and
shown without them, judged from the same text Jev sees (u when only the thread
or link would tell); their agreement with Jev is what the E1 numbers rest on.
Both are committed (query ids, URIs and answers only).

    uv run python -m bsky_embed.judge          # ask Jev about pairs not yet judged
    uv run python -m bsky_embed.judge --hand   # judge the hand sample in a real terminal; b goes back, q quits
"""

from __future__ import annotations

import argparse
import asyncio
import json
import random
import sys
import textwrap

from typesafe_sdk import Noul, NoulCriteria

from bsky_embed import corpus, search

MODEL = "jev-1.14.0"
JEV = corpus.EVAL / "judgments-jev.jsonl"
HAND = corpus.EVAL / "judgments-hand.jsonl"
HAND_SAMPLE = 150

RELEVANT = Noul(
    instructions="Is `post` a relevant result for the search `query`? Judge `post.text` in whatever language it is written; "
    "`query` and `post` need not share a language.",
    criteria=NoulCriteria(
        true="The post is about what `query` is looking for: it discusses, shows, reports on or directly reacts to it",
        false="The post is about something else, or only shares a word or name with `query` without being about it",
    ),
)


def pool() -> list[tuple[str, int]]:
    results = json.loads(search.RESULTS.read_text())
    pairs = {(qid, i) for hits in results.values() for qid, idx in hits.items() for i in idx}
    return sorted(pairs)


def load(path) -> dict[tuple[str, str], dict]:
    if not path.exists():
        return {}
    return {(j["qid"], j["uri"]): j for j in map(json.loads, path.open())}


async def ask_jev(pairs: list[tuple[dict, dict]], concurrency: int = 8) -> None:
    from dotenv import load_dotenv
    from typesafe_sdk import AsyncTypeSafeClient

    load_dotenv(corpus.EVAL.parents[1] / ".env")
    gate = asyncio.Semaphore(concurrency)
    tokens = 0
    async with AsyncTypeSafeClient(model=MODEL) as client:

        async def one(q: dict, post: dict, out) -> None:
            nonlocal tokens
            async with gate:
                r = await client.system_one(state={"query": q["query"], "post": {"text": post["text"]}}, questions={"relevant": RELEVANT})
            tokens += r.usage.input_tokens
            out.write(json.dumps({"qid": q["id"], "query": q["query"], "uri": post["uri"], "p": r.answers["relevant"].noul, "model": r.model}) + "\n")
            out.flush()

        with JEV.open("a") as out:
            await asyncio.gather(*(one(q, p, out) for q, p in pairs))
    print(f"{len(pairs)} pairs, {tokens:,} input tokens (${tokens * 0.042 / 1e6:.4f})", file=sys.stderr)


def hand(posts: list[dict], queries: dict[str, dict], jev: dict) -> None:
    from bsky_enrich.label import key

    by_uri = {p["uri"]: p for p in posts}
    done = list(map(json.loads, HAND.open())) if HAND.exists() else []
    seen = {(j["qid"], j["uri"]) for j in done}
    # English queries and posts only; a third each from Jev's low, middle and high probabilities.
    eligible = [j for j in jev.values() if queries[j["qid"]]["lang"] == "en" and by_uri[j["uri"]]["lang"] == "en"]
    rng = random.Random("e1-hand")
    sample = []
    for lo, hi in [(0, 0.2), (0.2, 0.8), (0.8, 1.01)]:
        band = sorted((j for j in eligible if lo <= j["p"] < hi), key=lambda j: (j["qid"], j["uri"]))
        sample += rng.sample(band, min(HAND_SAMPLE // 3, len(band)))
    rng.shuffle(sample)
    todo = [j for j in sample if (j["qid"], j["uri"]) not in seen]
    print("y = relevant, n = not, judged from the text shown (what the models and Jev see);\n"
          "u = can't tell without the thread or link (counted, left out of agreement); b goes back; q quits.")
    i = 0
    while i < len(todo):
        j = todo[i]
        print("\033[2J\033[H", end="")
        print(f"[{len(done) + 1}/{len(sample)}]\n\nquery: {queries[j['qid']]['query']}\n")
        post = by_uri[j["uri"]]
        print(textwrap.fill(post["text"], 100, initial_indent="  ", subsequent_indent="  ", replace_whitespace=False) + "\n")
        print(f"  https://bsky.app/profile/{post['did']}/post/{post['uri'].rsplit('/', 1)[-1]}\n")
        k = key("relevant? [y/n/u, b=back, q=quit]", "ynubq")
        if k == "q":
            break
        if k == "b":
            if done and i > 0:
                done.pop()
                i -= 1
            continue
        done.append({"qid": j["qid"], "uri": j["uri"], "relevant": {"y": True, "n": False, "u": None}[k]})
        HAND.write_text("".join(json.dumps(d) + "\n" for d in done))
        i += 1
    print(f"\n{len(done)} of {len(sample)} hand-judged in {HAND}")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--hand", action="store_true", help="hand-judge the sample in the terminal")
    args = parser.parse_args()
    posts = corpus.load()
    queries = {q["id"]: q for q in search.load_queries()}
    jev = load(JEV)
    if args.hand:
        hand(posts, queries, jev)
        return
    # A pair is re-asked if its query's text changed since it was judged.
    todo = [(queries[qid], posts[i]) for qid, i in pool()
            if (j := jev.get((qid, posts[i]["uri"]))) is None or j["query"] != queries[qid]["query"]]
    print(f"{len(todo)} pairs to judge", file=sys.stderr)
    if todo:
        asyncio.run(ask_jev(todo))


if __name__ == "__main__":
    main()

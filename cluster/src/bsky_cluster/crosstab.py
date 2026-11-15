"""Step 3: which communities engage with which stories.

    uv run python -m bsky_cluster.crosstab 2026-11-13 --communities 2026-11-07 2026-11-13

Stories are what is said (text-embedding clusters of one day's posts, stories.py);
communities are who likes what (the likes-graph SVD over a week, communities.py).
The cross-tab counts the day's likes on each story's posts by each community's
members, and scores each pair by lift: the community's share of the story's likes
over its share of all likes on clustered posts that day. Lift 3 means the
community likes that story three times as much as its overall liking predicts.

Reads stories-<day>.parquet/.json, likes-<day>.parquet and communities-<start>_<end>
.parquet/.json. Writes crosstab-<day>.md, and site/stories.json and
site/communities.json: the reports site's snapshot (publish.py). The snapshot
leaves adult communities out entirely and holds no account ids, only aggregates
and a few example posts per story and community.
"""

from __future__ import annotations

import argparse
import datetime as dt
import json
import logging

import duckdb

from bsky_cluster.fetch import DATA

log = logging.getLogger("bsky_cluster.crosstab")
MIN_LIKES = 30  # likes a (story, community) pair needs before its lift means anything
MIN_STORY_LIKES = 200
MIN_LIFT = 1.5  # listed pairs: clearly more than the community's overall liking predicts
MIN_SHARE = 0.01  # and this share of the story's likes, so a tiny community's quirk doesn't top the list
TOP = 5
SITE = DATA / "site"


def likes_by(day: dt.date, stem: str) -> tuple[dict[tuple[int, int], int], dict[int, int]]:
    """Likes on each story's posts by each community's members, and every story's likes from anyone."""
    con = duckdb.connect()
    con.execute(f"""
        CREATE TEMP TABLE l AS
        SELECT s.story, l.did FROM (SELECT DISTINCT did, subject_uri FROM read_parquet('{DATA}/likes-{day}.parquet')) l
        JOIN read_parquet('{DATA}/stories-{day}.parquet') s ON l.subject_uri = s.uri WHERE s.story >= 0""")
    everyone = dict(con.execute("SELECT story, count(*) FROM l GROUP BY 1").fetchall())
    pairs = con.execute(f"""
        SELECT story, community, count(*) FROM l JOIN read_parquet('{DATA}/communities-{stem}.parquet') c USING (did)
        GROUP BY 1, 2""").fetchall()
    return {(int(s), int(c)): int(n) for s, c, n in pairs}, {int(s): int(n) for s, n in everyone.items()}


def lifts(pairs: dict[tuple[int, int], int]) -> dict[tuple[int, int], tuple[float, float]]:
    """(lift, share of the story's community likes) for every pair with enough likes."""
    rows: dict[int, int] = {}
    cols: dict[int, int] = {}
    for (s, c), n in pairs.items():
        rows[s] = rows.get(s, 0) + n
        cols[c] = cols.get(c, 0) + n
    total = sum(rows.values())
    out = {(s, c): (n * total / (rows[s] * cols[c]), n / rows[s]) for (s, c), n in pairs.items()
           if n >= MIN_LIKES and rows[s] >= MIN_STORY_LIKES and n / rows[s] >= MIN_SHARE}
    return {k: v for k, v in out.items() if v[0] >= MIN_LIFT}


def run(day: dt.date, start: dt.date, end: dt.date) -> None:
    stem = f"{start}_{end}"
    stories = json.loads((DATA / f"stories-{day}.json").read_text())
    comms = json.loads((DATA / f"communities-{stem}.json").read_text())
    community = {c["id"]: c for c in comms["communities"]}
    pairs, everyone = likes_by(day, stem)
    lift = lifts(pairs)
    in_comms = sum(pairs.values())
    log.info("%d likes on clustered posts, %d (%.0f%%) by accounts in a community; %d pairs with %d+ likes",
             sum(everyone.values()), in_comms, 100 * in_comms / max(sum(everyone.values()), 1), len(lift), MIN_LIKES)

    def top_communities(s: int) -> list[dict]:
        got = sorted(((v, c) for (ss, c), v in lift.items() if ss == s and not community[c]["adult"]), reverse=True)
        return [{"id": c, "words": community[c]["words"][:4], "lift": round(l, 1), "share": round(sh, 3)}
                for (l, sh), c in got[:TOP]]

    shown = stories["stories"] + stories["topics"]
    by_id = {s["id"]: s for s in shown}

    def top_stories(c: int) -> list[dict]:
        got = sorted(((v, s) for (s, cc), v in lift.items() if cc == c and s in by_id), reverse=True)
        return [{"id": s, "rank": by_id[s]["rank"], "words": by_id[s]["words"][:4], "lift": round(l, 1),
                 "share": round(sh, 3)} for (l, sh), s in got[:TOP]]

    for s in shown:
        s["likes"] = everyone.get(s["id"], 0)
        s["communities"] = top_communities(s["id"])
    kept = [c for c in comms["communities"] if not c["adult"]]
    for c in kept:
        c["stories"] = top_stories(c["id"])

    SITE.mkdir(exist_ok=True)
    (SITE / "stories.json").write_text(json.dumps(stories, ensure_ascii=False))
    (SITE / "communities.json").write_text(json.dumps(
        {k: v for k, v in comms.items() if k != "communities"} | {
            "communities": kept, "adult_left_out": len(comms["communities"]) - len(kept)}, ensure_ascii=False))
    report(day, start, end, stories, kept, everyone, in_comms)


def report(day, start, end, stories, kept, everyone, in_comms) -> None:
    def name(words):
        return ", ".join(words[:4])

    lines = [f"# Stories x communities, {day}", "",
             f"Likes on {day} on the day's clustered posts ({sum(everyone.values()):,}), by members of the "
             f"{start} to {end} communities ({in_comms:,} likes). Lift is a community's share of a story's likes "
             f"over its share of all those likes; pairs need {MIN_LIKES}+ likes, {MIN_SHARE:.0%}+ of the story's and lift {MIN_LIFT}+. Adult communities are left out.",
             "", "## Who engages with each story", ""]
    for s in stories["stories"]:
        lines.append(f"### {s['rank']}. {name(s['words'])} ({s['posts']:,} posts, {s['likes']:,} likes)")
        lines += [f"- {c['lift']:.1f}x, {100 * c['share']:.0f}% of its likes: community {c['id']} ({name(c['words'])})"
                  for c in s["communities"]] or ["- (too few likes)"]
        lines.append("")
    lines += ["## What each community engages with", ""]
    for c in kept[:60]:
        lines.append(f"### {c['rank']}. {name(c['words'])} ({c['accounts']:,} accounts)")
        lines += [f"- {s['lift']:.1f}x: {'story ' + str(s['rank']) if s['rank'] else 'topic'} ({name(s['words'])})"
                  for s in c["stories"]] or ["- (no story with enough likes)"]
        lines.append("")
    out = DATA / f"crosstab-{day}.md"
    out.write_text("\n".join(lines))
    log.info("report: %s; site snapshot: %s", out, SITE)


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)s: %(message)s")
    p = argparse.ArgumentParser()
    p.add_argument("day", type=dt.date.fromisoformat)
    p.add_argument("--communities", nargs=2, type=dt.date.fromisoformat, required=True, metavar=("START", "END"))
    a = p.parse_args()
    run(a.day, *a.communities)


if __name__ == "__main__":
    main()

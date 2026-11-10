"""Hand-label the evaluation sample in the terminal, one key per answer.

Shows each post as Jev sees it (text, alt texts, link title and domain, quoted
and parent post, author), without its stratum or self-labels. Label from what's
shown; the bsky.app link is there for when you can't tell. Labels append to
eval/labels.jsonl (committed: URIs and labels only), so quitting and rerunning
picks up where you left off.

    uv run python -m bsky_enrich.label          # run it in a real terminal
"""

from __future__ import annotations

import json
import sys
import termios
import textwrap
import tty
from pathlib import Path

from bsky_enrich import spec
from bsky_enrich.sample import CACHE, EVAL

LABELS = EVAL / "labels.jsonl"
# 1-9 then 0 for the topics, x for none fits.
TOPIC_KEYS = dict(zip("1234567890", spec.TOPICS))


def key(prompt: str, allowed: str) -> str:
    print(prompt, end=" ", flush=True)
    fd = sys.stdin.fileno()
    old = termios.tcgetattr(fd)
    try:
        tty.setcbreak(fd)
        while (k := sys.stdin.read(1).lower()) not in allowed:
            pass
    finally:
        termios.tcsetattr(fd, termios.TCSADRAIN, old)
    print(k)
    return k


def show(record: dict, n: int, total: int) -> None:
    s = record["state"]
    p = s["post"]
    wrap = lambda t, indent="  ": textwrap.fill(t, 100, initial_indent=indent, subsequent_indent=indent)
    print("\033[2J\033[H", end="")
    print(f"[{n}/{total}]  https://bsky.app/profile/{record['did']}/post/{record['uri'].rsplit('/', 1)[-1]}")
    if a := s.get("author"):
        print(f"author: {a.get('display_name') or ''} @{a.get('handle') or '?'}")
    if root := s.get("thread_root"):
        print("\nthread starts with:\n" + wrap(root["text"], "  >> "))
    if parent := s.get("reply_parent"):
        print("\nreplying to:\n" + wrap(parent["text"], "  > "))
    print("\npost:\n" + wrap(p["text"] or "(no text)"))
    for alt in p.get("image_alts", []):
        print("  [image] " + textwrap.shorten(alt, 300))
    if link := p.get("link"):
        print(f"  [link] {link.get('title') or ''} ({link.get('domain') or '?'})")
    if q := p.get("quoted_post_text"):
        print("\nquoting:\n" + wrap(q, "  | "))
    print()


def main() -> None:
    records = [json.loads(line) for line in (CACHE / "posts.jsonl").open()]
    done = {json.loads(line)["uri"] for line in LABELS.open()} if LABELS.exists() else set()
    todo = [r for r in records if r["uri"] not in done]
    print("Keys: y/n per question; ai also u (can't tell); s skips a post you can't judge; q quits.")
    topics = "  ".join(f"{k}={t}" for k, t in TOPIC_KEYS.items())
    with LABELS.open("a") as out:
        for i, record in enumerate(todo, start=len(done) + 1):
            show(record, i, len(records))
            first = key("spam? [y/n, s=skip, q=quit]", "ynsq")
            if first == "q":
                break
            if first == "s":
                out.write(json.dumps({"uri": record["uri"], "skipped": True}) + "\n")
                continue
            label = {"uri": record["uri"], "spam": first == "y"}
            label["adult"] = key("adult? [y/n]", "yn") == "y"
            label["ai_generated"] = {"y": True, "n": False, "u": None}[key("written by AI? [y/n/u]", "ynu")]
            label["humor"] = key("joke, sarcasm or snark? [y/n]", "yn") == "y"
            print(topics + "  x=none fits")
            t = key("topic?", "x" + "".join(TOPIC_KEYS))
            label["topic"] = TOPIC_KEYS.get(t)
            if label["topic"]:
                # A second topic that would also be a fair answer; the eval accepts either.
                alt = key("also acceptable? [topic key, or space for none]", " " + "".join(k for k in TOPIC_KEYS if k != t))
                label["topic_alt"] = TOPIC_KEYS.get(alt)
            out.write(json.dumps(label) + "\n")
            out.flush()
    print(f"\n{sum(1 for _ in LABELS.open()) if LABELS.exists() else 0} of {len(records)} labeled in {LABELS}")


if __name__ == "__main__":
    main()

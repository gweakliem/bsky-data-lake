"""Hand-label the evaluation sample in the terminal, one key per answer.

Shows each post as Jev sees it (text, alt texts, link title and domain, quoted
and parent post, author), without its stratum or self-labels. Label from what's
shown; the bsky.app link is there for when you can't tell. Labels append to
eval/labels.jsonl (committed: URIs and labels only), so quitting and rerunning
picks up where you left off. b steps back a question, or to the previous
post, whose label is removed and redone.

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


def render(record: dict) -> str:
    """A post as Jev sees it, with its bsky.app link first. Shared with evaluate --disagreements."""
    s = record["state"]
    p = s["post"]
    wrap = lambda t, indent="  ": textwrap.fill(t, 100, initial_indent=indent, subsequent_indent=indent)
    out = [f"https://bsky.app/profile/{record['did']}/post/{record['uri'].rsplit('/', 1)[-1]}"]
    if a := s.get("author"):
        out.append(f"author: {a.get('display_name') or ''} @{a.get('handle') or '?'}")
    if root := s.get("thread_root"):
        out.append("\nthread starts with:\n" + wrap(root["text"], "  >> "))
    if parent := s.get("reply_parent"):
        out.append("\nreplying to:\n" + wrap(parent["text"], "  > "))
    out.append("\npost:\n" + wrap(p["text"] or "(no text)"))
    for alt in p.get("image_alts", []):
        out.append("  [image] " + textwrap.shorten(alt, 300))
    if link := p.get("link"):
        out.append(f"  [link] {link.get('title') or ''} ({link.get('domain') or '?'})")
    if q := p.get("quoted_post_text"):
        out.append("\nquoting:\n" + wrap(q, "  | "))
    return "\n".join(out)


def show(record: dict, n: int, total: int) -> None:
    print("\033[2J\033[H", end="")
    print(f"[{n}/{total}]  {render(record)}\n")


def ask(record: dict) -> dict | str:
    """One post's label, "back" (b at the first question) or "quit". b at a later question re-asks the one before."""
    yn = lambda k: k == "y"
    topics = "  ".join(f"{k}={t}" for k, t in TOPIC_KEYS.items())
    steps = [
        ("spam", "spam? [y/n, s=skip, b=back, q=quit]", "ynsq", yn),
        ("adult", "adult? [y/n]", "yn", yn),
        ("ai_generated", "written by AI? [y/n/u]", "ynu", {"y": True, "n": False, "u": None}.get),
        ("humor", "joke, sarcasm or snark? [y/n]", "yn", yn),
        ("topic", topics + "  x=none fits\ntopic?", "x" + "".join(TOPIC_KEYS), TOPIC_KEYS.get),
        # A second topic that would also be a fair answer; the eval accepts either.
        ("topic_alt", "also acceptable? [topic key, or space for none]", " " + "".join(TOPIC_KEYS), TOPIC_KEYS.get),
    ]
    label = {"uri": record["uri"]}
    i = 0
    while i < len(steps):
        field, prompt, allowed, parse = steps[i]
        if field == "topic_alt":
            if label["topic"] is None:
                break
            allowed = allowed.replace(next(k for k, t in TOPIC_KEYS.items() if t == label["topic"]), "")
        k = key(prompt, allowed + "b")
        if k == "b":
            if i == 0:
                return "back"
            i -= 1
            continue
        if k == "q":
            return "quit"
        if k == "s":
            return {"uri": record["uri"], "skipped": True}
        label[field] = parse(k)
        i += 1
    return label


def describe(label: dict) -> str:
    if label.get("skipped"):
        return "skipped"
    topic = "/".join(t for t in (label.get("topic"), label.get("topic_alt")) if t) or "none fits"
    fmt = lambda v: {True: "y", False: "n", None: "u"}[v]
    return f"spam={fmt(label['spam'])} adult={fmt(label['adult'])} ai={fmt(label['ai_generated'])} humor={fmt(label['humor'])} topic={topic}"


def save(labels: list[dict]) -> None:
    tmp = LABELS.with_suffix(".tmp")
    tmp.write_text("".join(json.dumps(lab) + "\n" for lab in labels))
    tmp.replace(LABELS)


def main() -> None:
    records = [json.loads(line) for line in (CACHE / "posts.jsonl").open()]
    by_uri = {r["uri"]: r for r in records}
    labels = [json.loads(line) for line in LABELS.open()] if LABELS.exists() else []
    done = {lab["uri"] for lab in labels}
    queue = [r for r in records if r["uri"] not in done]
    previous: dict[str, dict] = {}  # labels removed by going back, shown while the post is redone
    notice = ""
    print("Keys: y/n per question; ai also u (can't tell); s skips a post you can't judge; b goes back; q quits.")
    while queue:
        record = queue[0]
        show(record, len(labels) + 1, len(records))
        if old := previous.get(record["uri"]):
            print(f"previously: {describe(old)}\n")
        if notice:
            print(notice + "\n")
            notice = ""
        result = ask(record)
        if result == "quit":
            # Posts gone back to but not redone keep their old labels.
            labels += [previous[r["uri"]] for r in queue if r["uri"] in previous]
            save(labels)
            break
        if result == "back":
            if not labels or labels[-1]["uri"] not in by_uri:
                notice = "nothing to go back to"
                continue
            old = labels.pop()
            save(labels)
            previous[old["uri"]] = old
            queue.insert(0, by_uri[old["uri"]])
            continue
        labels.append(result)
        save(labels)
        queue.pop(0)
        previous.pop(record["uri"], None)
    print(f"\n{len(labels)} of {len(records)} labeled in {LABELS}")


if __name__ == "__main__":
    main()

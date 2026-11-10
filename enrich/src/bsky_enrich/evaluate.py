"""Run the spec over the labeled sample and report how well each question does.

Answers are cached per spec version and model in eval/.cache/, so rerunning
the report costs nothing; only posts without a cached answer are sent.

    uv run python -m bsky_enrich.evaluate            # ask Jev what isn't cached, then report
    uv run python -m bsky_enrich.evaluate --cached   # report from the cache only
    uv run python -m bsky_enrich.evaluate --disagreements spam | less   # posts where Jev and the labels differ
"""

from __future__ import annotations

import argparse
import asyncio
import json
import sys
from collections import Counter
from pathlib import Path

from bsky_enrich import spec
from bsky_enrich.label import LABELS, render
from bsky_enrich.sample import CACHE

PRICE_PER_MTOK = 0.042  # USD per million input tokens, jev-1.14.0 (docs.typesafe.ai/models)
THRESHOLDS = [0.3, 0.5, 0.7, 0.9]
NOULS = ["spam", "adult", "ai_generated", "humor"]


def answers_path() -> Path:
    return CACHE / f"answers-{spec.SPEC_VERSION}-{spec.MODEL}.jsonl"


async def ask(records: list[dict], concurrency: int = 8) -> None:
    from dotenv import load_dotenv
    from typesafe_sdk import AsyncTypeSafeClient

    from bsky_enrich.lake import REPO

    load_dotenv(REPO / ".env")
    questions = spec.QUESTIONS | spec.ORDER_CHECK
    gate = asyncio.Semaphore(concurrency)
    path = answers_path()
    async with AsyncTypeSafeClient(model=spec.MODEL) as client:

        async def one(record: dict, out) -> None:
            async with gate:
                r = await client.system_one(state=record["state"], questions=questions)
            row = {"uri": record["uri"], "model": r.model, "input_tokens": r.usage.input_tokens}
            for qid, a in r.answers.items():
                row[qid] = a.noul if a.type == "noul" else {"choice": a.choice, "probabilities": a.probabilities, "confidence": a.confidence}
            out.write(json.dumps(row) + "\n")
            out.flush()

        with path.open("a") as out:
            await asyncio.gather(*(one(r, out) for r in records))


def precision_recall(pairs: list[tuple[float, bool]], threshold: float) -> tuple[float | None, float | None, int]:
    """Precision and recall of `p >= threshold` against the labels, and how many were predicted yes."""
    tp = sum(1 for p, y in pairs if p >= threshold and y)
    predicted = sum(1 for p, _ in pairs if p >= threshold)
    actual = sum(1 for _, y in pairs if y)
    return (tp / predicted if predicted else None), (tp / actual if actual else None), predicted


def pct(x: float | None) -> str:
    return "  -  " if x is None else f"{100 * x:4.0f}%"


def report(labels: list[dict], answers: dict[str, dict], records: dict[str, dict]) -> str:
    rows = [(lab, answers[lab["uri"]]) for lab in labels if lab["uri"] in answers]
    out = [f"spec {spec.SPEC_VERSION}, {spec.MODEL}: {len(rows)} labeled posts with answers"]
    strata = Counter(records[lab["uri"]]["stratum"] for lab, _ in rows)
    out.append("strata: " + ", ".join(f"{k} {v}" for k, v in strata.most_common()))

    for q in NOULS:
        pairs = [(a[q], lab[q]) for lab, a in rows if lab.get(q) is not None]
        positives = sum(y for _, y in pairs)
        out.append(f"\n{q}: {positives} of {len(pairs)} labeled yes; {sum(1 for p, _ in pairs if 0.3 < p < 0.7)} answers between 0.3 and 0.7")
        out.append("  threshold  precision  recall  predicted yes")
        for t in THRESHOLDS:
            p, r, n = precision_recall(pairs, t)
            out.append(f"  {t:>9}  {pct(p):>9}  {pct(r):>6}  {n:>13}")
        if q == "ai_generated":
            unsure = sum(1 for lab, _ in rows if lab.get(q) is None)
            hist = Counter(min(int(a[q] * 10), 9) for _, a in rows)
            out.append(f"  labeled 'can't tell': {unsure}. Answers by decile: " + " ".join(f"{d / 10:.1f}:{hist[d]}" for d in range(10)))

    adult = [(lab, records[lab["uri"]]) for lab, _ in rows]
    self_labeled = [(bool(set(r.get("self_labels") or []) & {"porn", "sexual", "nudity"}), lab["adult"]) for lab, r in adult]
    p, r, n = precision_recall([(1.0 if s else 0.0, y) for s, y in self_labeled], 0.5)
    out.append(f"  adult, self-labels (porn/sexual/nudity) as the predictor: precision {pct(p)}, recall {pct(r)}, {n} self-labeled")

    topical = [(lab, a) for lab, a in rows if "topic" in lab]
    fits = [(a["topic_fits"], lab["topic"] is not None) for lab, a in topical]
    out.append(f"\ntopic_fits: {sum(y for _, y in fits)} of {len(fits)} labeled with a topic")
    out.append("  threshold  precision  recall")
    for t in THRESHOLDS:
        p, r, _ = precision_recall(fits, t)
        out.append(f"  {t:>9}  {pct(p):>9}  {pct(r):>6}")
    # Correct when Jev's pick is the labeled topic or the labeled alternative.
    with_topic = [({lab["topic"], lab.get("topic_alt")} - {None}, a) for lab, a in topical if lab["topic"] is not None]
    if with_topic:
        correct = sum(1 for t, a in with_topic if a["topic"]["choice"] in t)
        exact = sum(1 for lab, a in topical if lab["topic"] is not None and a["topic"]["choice"] == lab["topic"])
        out.append(
            f"topic: {correct} of {len(with_topic)} correct ({pct(correct / len(with_topic))}) among posts labeled with a topic, "
            f"{exact} matching the first label"
        )
        sure = [(t, a) for t, a in with_topic if a["topic_fits"] >= 0.7]
        if sure:
            out.append(f"  where topic_fits >= 0.7: {sum(1 for t, a in sure if a['topic']['choice'] in t)} of {len(sure)} correct")
        same = sum(1 for _, a in topical if a["topic"]["choice"] == a["topic_reversed"]["choice"])
        out.append(f"  same choice with the options reversed: {same} of {len(topical)}")
        confusion = Counter(("/".join(sorted(t)), a["topic"]["choice"]) for t, a in with_topic if a["topic"]["choice"] not in t)
        out.append("  most common confusions (labeled -> chosen): " + ", ".join(f"{t}->{c} {n}" for (t, c), n in confusion.most_common(8)))

    tokens = [a["input_tokens"] for _, a in rows]
    if tokens:
        mean = sum(tokens) / len(tokens)
        # The order check is eval-only; production requests are slightly smaller.
        out.append(
            f"\ncost: {sum(tokens):,} input tokens (${sum(tokens) * PRICE_PER_MTOK / 1e6:.4f}), "
            f"{mean:.0f} per post including the eval-only order check, "
            f"so about ${mean * PRICE_PER_MTOK:.0f} per million posts"
        )
    return "\n".join(out)


def disagreements(question: str, threshold: float, labels: list[dict], answers: dict[str, dict], records: dict[str, dict]) -> str:
    """The posts where Jev and the labels differ on one question, Jev's most confident misses first."""
    rows = [(lab, answers[lab["uri"]], records[lab["uri"]]) for lab in labels if lab["uri"] in answers]
    out: list[str] = []

    def section(title: str, items: list[tuple[str, dict]]) -> None:
        out.append(f"\n{'=' * 100}\n{title}: {len(items)}\n{'=' * 100}")
        for summary, record in items:
            out.append(f"\n--- {summary}  [{record['stratum']}]\n{render(record)}")

    if question == "topic":
        def probs(a: dict) -> str:
            top = sorted(a["topic"]["probabilities"].items(), key=lambda kv: -kv[1])[:3]
            return ", ".join(f"{t} {p:.2f}" for t, p in top) + f"; topic_fits {a['topic_fits']:.2f}"

        wrong, unlabeled = [], []
        for lab, a, r in rows:
            if "topic" not in lab:
                continue
            accepted = {lab["topic"], lab.get("topic_alt")} - {None}
            if not accepted:
                if a["topic_fits"] >= threshold:
                    unlabeled.append((a["topic_fits"], f"labeled none fits; Jev: {probs(a)}", r))
            elif a["topic"]["choice"] not in accepted:
                wrong.append((a["topic"]["confidence"], f"labeled {'/'.join(sorted(accepted))}; Jev: {probs(a)}", r))
        section("Jev's topic isn't the labeled one", [(s, r) for _, s, r in sorted(wrong, key=lambda x: -x[0])])
        section(f"labeled none fits, Jev's topic_fits >= {threshold}", [(s, r) for _, s, r in sorted(unlabeled, key=lambda x: -x[0])])
        return "\n".join(out)

    pairs = [(a[question], lab[question], r) for lab, a, r in rows if lab.get(question) is not None]
    false_yes = sorted(((p, r) for p, y, r in pairs if p >= threshold and not y), key=lambda x: -x[0])
    false_no = sorted(((p, r) for p, y, r in pairs if p < threshold and y), key=lambda x: x[0])
    section(f"{question}: Jev >= {threshold}, labeled no", [(f"{question} {p:.2f}", r) for p, r in false_yes])
    section(f"{question}: labeled yes, Jev < {threshold}", [(f"{question} {p:.2f}", r) for p, r in false_no])
    return "\n".join(out)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--cached", action="store_true", help="don't call the API; report from cached answers")
    parser.add_argument(
        "--disagreements", choices=[*NOULS, "topic"], metavar="QUESTION",
        help=f"list the posts where Jev and the labels differ ({', '.join(NOULS)} or topic), from the cache only",
    )
    parser.add_argument("--threshold", type=float, default=0.5, help="yes threshold for --disagreements (default 0.5)")
    args = parser.parse_args()
    records = {r["uri"]: r for r in map(json.loads, (CACHE / "posts.jsonl").open())}
    labels = [lab for lab in map(json.loads, LABELS.open()) if not lab.get("skipped")]
    path = answers_path()
    cached = {a["uri"]: a for a in map(json.loads, path.open())} if path.exists() else {}
    missing = [records[lab["uri"]] for lab in labels if lab["uri"] not in cached]
    if args.disagreements:
        print(disagreements(args.disagreements, args.threshold, labels, cached, records))
        return
    if missing and not args.cached:
        print(f"asking Jev about {len(missing)} post(s)", file=sys.stderr)
        asyncio.run(ask(missing))
        cached = {a["uri"]: a for a in map(json.loads, path.open())}
    print(report(labels, cached, records))


if __name__ == "__main__":
    main()

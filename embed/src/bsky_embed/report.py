"""Report E1: search quality per system, Jev's agreement with the hand judgments, cost and speed, and clustering.

Relevance is Jev's p >= 0.5. Recall is measured against the pool (every
system's top 10), so it means "found of the relevant posts any system found",
capped at 10.

Clustering runs k-means (k=40) on each model's post vectors and reports:
  - topic AMI: agreement of clusters with C1's hand topics on the C1 posts
    (about 140 English posts, so a rough signal);
  - language-split share: the share of non-English posts that land in clusters
    at least 80% one non-English language, i.e. clustered by language rather
    than subject.
`--clusters MODEL` prints each cluster's language mix and the posts nearest
its center, for reading.

    uv run python -m bsky_embed.report
    uv run python -m bsky_embed.report --clusters qwen3-8b | less
"""

from __future__ import annotations

import argparse
import json
import math
from collections import Counter, defaultdict

import numpy as np

from bsky_embed import corpus, embed, judge, search
from bsky_embed.models import MODELS

ELIGIBLE_PER_DAY = 2.9e6  # eligible posts a day (C1, 2026-11-09)
K_CLUSTERS = 40
GROUPS = {"en": {"news", "everyday", "entertainment", "commerce"}, "native": {"native"}, "cross": {"cross"}}


def search_metrics(results: dict, queries: dict, posts: list[dict], rel: dict) -> dict[str, dict[str, dict]]:
    """{system: {group: {ndcg, p10, recall, xlang}}}."""
    relevant_in_pool: dict[str, set[str]] = defaultdict(set)
    for (qid, uri), r in rel.items():
        if r:
            relevant_in_pool[qid].add(uri)
    out: dict[str, dict[str, dict]] = {}
    for system, hits in results.items():
        per_group: dict[str, list[tuple[float, float, float]]] = defaultdict(list)
        xlang = [0, 0]  # relevant hits in another language than the query, all relevant hits
        for qid, idx in hits.items():
            q = queries[qid]
            n_rel = len(relevant_in_pool[qid])
            if not n_rel:
                continue
            flags = [rel.get((qid, posts[i]["uri"]), False) for i in idx[: search.K]]
            dcg = sum(f / math.log2(r + 2) for r, f in enumerate(flags))
            idcg = sum(1 / math.log2(r + 2) for r in range(min(n_rel, search.K)))
            found = sum(flags)
            per_group[next(g for g, kinds in GROUPS.items() if q["kind"] in kinds)].append(
                (dcg / idcg, found / search.K, found / min(n_rel, search.K))
            )
            for i, f in zip(idx, flags):
                if f:
                    xlang[1] += 1
                    xlang[0] += posts[i]["lang"] not in (q["lang"], None)
        out[system] = {g: dict(zip(["ndcg", "p10", "recall"], np.mean(v, axis=0))) | {"n": len(v)} for g, v in per_group.items()}
        all_rows = [row for v in per_group.values() for row in v]
        out[system]["all"] = dict(zip(["ndcg", "p10", "recall"], np.mean(all_rows, axis=0))) | {"n": len(all_rows)}
        out[system]["all"]["xlang"] = xlang[0] / xlang[1] if xlang[1] else 0.0
    return out


def clusters(name: str, posts: list[dict], k: int = K_CLUSTERS) -> tuple[np.ndarray, np.ndarray, np.ndarray]:
    """Labels, centers and vectors; labels cached per model."""
    from sklearn.cluster import MiniBatchKMeans

    v = embed.vectors(name)
    path = corpus.CACHE / "clusters" / f"{name}-k{k}.npz"
    if path.exists():
        z = np.load(path)
        return z["labels"], z["centers"], v
    km = MiniBatchKMeans(n_clusters=k, random_state=0, batch_size=4096, n_init=3).fit(v)
    path.parent.mkdir(parents=True, exist_ok=True)
    np.savez(path, labels=km.labels_, centers=km.cluster_centers_)
    return km.labels_, km.cluster_centers_, v


def cluster_metrics(name: str, posts: list[dict], c1_topics: dict[str, str]) -> dict:
    from sklearn.metrics import adjusted_mutual_info_score

    labels, _, _ = clusters(name, posts)
    tagged = [(labels[i], c1_topics[p["uri"]]) for i, p in enumerate(posts) if p["uri"] in c1_topics]
    ami = adjusted_mutual_info_score([t for _, t in tagged], [c for c, _ in tagged]) if tagged else float("nan")
    by_cluster: dict[int, Counter] = defaultdict(Counter)
    for c, p in zip(labels, posts):
        by_cluster[c][p["lang"] or "?"] += 1
    split = 0
    non_en = sum(1 for p in posts if p["lang"] not in ("en", None))
    for counts in by_cluster.values():
        lang, n = counts.most_common(1)[0]
        if lang not in ("en", "?") and n / sum(counts.values()) >= 0.8:
            split += n
    return {"topic_ami": ami, "lang_split": split / non_en if non_en else 0.0}


def show_clusters(name: str, posts: list[dict], per_cluster: int = 6) -> str:
    labels, centers, v = clusters(name, posts)
    out = []
    for c in np.argsort(-np.bincount(labels)):
        members = np.flatnonzero(labels == c)
        langs = Counter(posts[i]["lang"] or "?" for i in members)
        mix = ", ".join(f"{l} {100 * n / len(members):.0f}%" for l, n in langs.most_common(3))
        near = members[np.argsort(-(v[members] @ centers[c]))[:per_cluster]]
        out.append(f"\n=== cluster {c}: {len(members)} posts ({100 * len(members) / len(posts):.1f}%); {mix}")
        out += ["  - " + posts[i]["text"][:160].replace("\n", " ") for i in near]
    return "\n".join(out)


def agreement(jev: dict, hand: dict) -> str:
    pairs = [(jev[key]["p"] >= 0.5, h["relevant"]) for key, h in hand.items() if key in jev]
    if not pairs:
        return "hand judgments: none yet (judge --hand)"
    agree = sum(a == b for a, b in pairs)
    tp = sum(a and b for a, b in pairs)
    jy, hy = sum(a for a, _ in pairs), sum(b for _, b in pairs)
    return (
        f"Jev vs hand on {len(pairs)} English pairs: {100 * agree / len(pairs):.0f}% agree; "
        f"of Jev's yeses {100 * tp / jy if jy else 0:.0f}% hand-relevant, of hand-relevant {100 * tp / hy if hy else 0:.0f}% Jev yes"
    )


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--clusters", metavar="MODEL", choices=list(MODELS), help="print one model's clusters")
    args = parser.parse_args()
    posts = corpus.load()
    if args.clusters:
        print(show_clusters(args.clusters, posts))
        return
    queries = {q["id"]: q for q in search.load_queries()}
    results = json.loads(search.RESULTS.read_text())
    jev = judge.load(judge.JEV)
    rel = {key: j["p"] >= 0.5 for key, j in jev.items() if j["query"] == queries[key[0]]["query"]}
    c1 = {}
    from bsky_enrich.label import LABELS

    for lab in map(json.loads, LABELS.open()):
        if lab.get("topic"):
            c1[lab["uri"]] = lab["topic"]

    m = search_metrics(results, queries, posts, rel)
    print(f"{len(posts):,} posts, {len(queries)} queries, {sum(rel.values())} of {len(rel)} pooled pairs relevant (Jev p >= 0.5)")
    print(agreement(jev, judge.load(judge.HAND)))
    print("\nsearch (nDCG@10 / recall@10 vs pool); en = English queries, native = non-English, cross = English query, mostly non-English posts")
    print(f"  {'system':<15} {'all':>11} {'en':>11} {'native':>11} {'cross':>11}  other-lang share of hits")
    for system in sorted(m, key=lambda s: -m[s]["all"]["ndcg"]):
        cells = [f"{m[system][g]['ndcg']:.2f}/{m[system][g]['recall']:.2f}" if g in m[system] else "-" for g in ["all", "en", "native", "cross"]]
        print(f"  {system:<15} " + " ".join(f"{c:>11}" for c in cells) + f"  {100 * m[system]['all']['xlang']:.0f}%")

    print("\nmodels: dims, bytes/vector (float32), posts/s on MPS or the API, on 4 CPU threads, hosted cost a day at 2.9M posts")
    print(f"  {'model':<15} {'dims':>5} {'bytes':>6} {'mps/api':>8} {'cpu4':>6} {'tok/post':>8} {'$/day':>6}   topic AMI  lang-split")
    for name in MODELS:
        path = embed.VECTORS / f"{name}.json"
        if not path.exists():
            continue
        meta = json.loads(path.read_text())
        tpp = meta.get("prompt_tokens", 0) / meta["posts"] if MODELS[name].hosted else None
        cost = f"{tpp * ELIGIBLE_PER_DAY * MODELS[name].usd_per_mtok / 1e6:6.2f}" if tpp else "     -"
        cm = cluster_metrics(name, posts, c1)
        print(
            f"  {name:<15} {meta['dims']:>5} {meta['dims'] * 4:>6} {meta['posts_per_sec']:>8} {meta.get('cpu_posts_per_sec', '-'):>6} "
            f"{f'{tpp:.0f}' if tpp else '-':>8} {cost}   {cm['topic_ami']:9.2f}  {100 * cm['lang_split']:9.0f}%"
        )


if __name__ == "__main__":
    main()

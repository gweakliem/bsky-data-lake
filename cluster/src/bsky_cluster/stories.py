"""Step 1: cluster one day's posts by their text embeddings, and rank the clusters as stories.

    uv run python -m bsky_cluster.stories 2026-11-13 [--sample 250000] [--min-cluster 60]

Reads data/cluster/posts-<day>.parquet, sample-<day - 1>.parquet, labels-*/authors-*
(fetch.py). Writes data/cluster/stories-<day>.parquet (uri, cluster, story), stories-<day>.md and
clusters-<day>.npz/.parquet (per-cluster centroids, timing, words; merge.py reads them).

Pipeline:
- Leave out replies (without their parent they cluster by tone: "yes, exactly"),
  adult posts (filters.py), accounts posting over 300 times that day, and
  copy-paste spam (one text 20+ times from 3 or fewer accounts).
- Collapse near-identical texts (weighted by how many posts share them),
  PCA 1024 -> 64, UMAP 64 -> 8 on a sample, HDBSCAN on that, then every text group
  goes to its nearest centroid in PCA space (or to none).
- Merge clusters that are one story (merge.py): same topic in the full embedding
  space, across languages, unless their timing is opposite.
- Rank stories by posts above yesterday's rate, size x (1 - 1/growth): growth =
  share of today's posts / share of a 10% sample of yesterday's (placed on today's
  centroids), at least 1.5x. Also reported: burst = share of the story's posts
  in its busiest 3 hours over its language's share in those hours.
"""

from __future__ import annotations

import argparse
import collections
import datetime as dt
import logging
import re
import time
from urllib.parse import urlparse

import numpy as np
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq
from sklearn.cluster import HDBSCAN
from sklearn.decomposition import PCA

from bsky_cluster import filters, merge
from bsky_cluster.fetch import DATA
from bsky_cluster.labels import background_sample, label_words, lang_of
from bsky_embed import spec

log = logging.getLogger("bsky_cluster.stories")
URL = re.compile(r"https?://\S+|\b[\w-]+(\.[\w-]+)+/\S*|\b(www\.)[\w.-]+\S*")
SPACE = re.compile(r"\s+")
NOISE_DOMAINS = {"static.klipy.com", "media.tenor.com", "bsky.app", "youtu.be", "youtube.com", "bit.ly", "dlvr.it",
                 "buff.ly"}
SPAM_COPIES, SPAM_AUTHORS = 20, 3


def vectors(col: pa.ChunkedArray) -> np.ndarray:
    """The binary float16 column as an (n, DIMS) float16 array, without a Python loop."""
    arr = col.combine_chunks()
    data = np.frombuffer(arr.buffers()[2], dtype=spec.DTYPE)
    start = arr.offset * spec.DIMS
    return data[start:start + len(arr) * spec.DIMS].reshape(len(arr), spec.DIMS)


def key(text: str) -> str:
    """Near-duplicate key: the embedded text without URLs, case or spacing."""
    return SPACE.sub(" ", URL.sub(" ", text.lower())).strip()


def domain(u: str | None) -> str | None:
    if not u:
        return None
    host = (urlparse(u).hostname or "").removeprefix("www.")
    return host if host and host not in NOISE_DOMAINS else None


def normalize(x: np.ndarray) -> np.ndarray:
    return x / np.linalg.norm(x, axis=1, keepdims=True).clip(1e-12)


def bsky_url(uri: str) -> str:
    _, _, did, _, rkey = uri.split("/", 4)
    return f"https://bsky.app/profile/{did}/post/{rkey}"


def leave_out(t: pa.Table, day: dt.date, adult_uris: set[str], adult_dids: set[str]) -> tuple[pa.Table, dict]:
    """Drop replies, adult posts and busy accounts; returns the rest and what was dropped."""
    dropped = {}
    n = len(t)
    t = t.filter(pc.is_null(t["reply_root_uri"]))
    dropped["replies"], n = n - len(t), len(t)
    adult = pc.or_(pc.is_in(t["uri"], pa.array(list(adult_uris))), pc.is_in(t["did"], pa.array(list(adult_dids))))
    t = t.filter(pc.invert(adult))
    dropped["adult"], n = n - len(t), len(t)
    t = t.filter(pc.invert(pc.is_in(t["did"], pa.array(list(filters.busy_accounts(day))))))
    dropped["busy accounts"] = n - len(t)
    return t, dropped


def placed(reduced: np.ndarray, cent: np.ndarray, floor: np.ndarray, chunk: int = 100_000):
    story = np.full(len(reduced), -1, dtype=np.int64)
    sim = np.zeros(len(reduced), dtype=np.float32)
    for s in range(0, len(reduced), chunk):
        sims = reduced[s:s + chunk] @ cent.T
        best = sims.argmax(axis=1)
        bsim = sims[np.arange(len(best)), best]
        story[s:s + chunk] = np.where(bsim >= floor[best], best, -1)
        sim[s:s + chunk] = bsim
    return story, sim


def run(day: dt.date, sample: int, min_cluster: int, seed: int = 0) -> None:
    import umap  # slow import

    t0 = time.monotonic()
    week = (day - dt.timedelta(days=6), day)
    adult_uris = filters.adult_posts(*week)
    adult_dids = filters.adult_accounts(*week, adult_uris)
    posts = pq.read_table(DATA / f"posts-{day}.parquet")
    total = len(posts)
    posts, dropped = leave_out(posts, day, adult_uris, adult_dids)

    texts = [spec.text(r) for r in posts.select(["text", "image_alts", "external_title"]).to_pylist()]
    dids = posts["did"].to_pylist()
    keys = [key(t) for t in texts]
    group_of: dict[str, int] = {}
    gid = np.fromiter((group_of.setdefault(k, len(group_of)) for k in keys), dtype=np.int64, count=len(keys))
    weight = np.bincount(gid)
    authors = collections.defaultdict(set)
    for g, d in zip(gid, dids):
        if weight[g] >= SPAM_COPIES:
            authors[g].add(d)
    spam = {g for g, a in authors.items() if len(a) <= SPAM_AUTHORS}
    keep = np.array([g not in spam for g in gid])
    dropped["copy-paste spam"] = int((~keep).sum())
    posts = posts.filter(pa.array(keep))
    texts = [t for t, k in zip(texts, keep) if k]
    keys = [k for k, ok in zip(keys, keep) if ok]
    n = len(posts)
    log.info("%d posts; left out %s; %d remain", total, dropped, n)

    group_of = {}
    gid = np.fromiter((group_of.setdefault(k, len(group_of)) for k in keys), dtype=np.int64, count=n)
    first = np.full(len(group_of), -1, dtype=np.int64)
    first[gid[::-1]] = np.arange(n - 1, -1, -1)
    g = len(group_of)
    log.info("%d near-duplicate groups (%.1f%% of posts collapsed)", g, 100 * (1 - g / n))

    vec = vectors(posts["vector"])
    rng = np.random.default_rng(seed)
    fit = rng.choice(g, size=min(sample, g), replace=False)
    pca = PCA(n_components=64, svd_solver="randomized", random_state=seed).fit(vec[first[fit]].astype(np.float32))

    def project(v: np.ndarray) -> np.ndarray:
        out = np.empty((len(v), 64), dtype=np.float32)
        for s in range(0, len(v), 200_000):
            out[s:s + 200_000] = pca.transform(v[s:s + 200_000].astype(np.float32))
        return normalize(out)

    reduced = project(vec[first])
    t = time.monotonic()
    emb = umap.UMAP(n_neighbors=30, n_components=8, min_dist=0.0, metric="euclidean", random_state=seed,
                    low_memory=True, n_jobs=-1).fit_transform(reduced[fit])
    labels = HDBSCAN(min_cluster_size=min_cluster, min_samples=10, cluster_selection_method="leaf",
                     copy=True).fit_predict(emb)
    k = labels.max() + 1
    log.info("UMAP + HDBSCAN: %d clusters, %.0f%% of the sample unclustered, %.0f s", k, 100 * (labels < 0).mean(),
             time.monotonic() - t)

    cent = normalize(np.stack([reduced[fit[labels == c]].mean(axis=0) for c in range(k)]))
    floor = np.array([np.quantile(reduced[fit[labels == c]] @ cent[c], 0.1) for c in range(k)], dtype=np.float32)
    story, sim = placed(reduced, cent, floor)
    post_story = story[gid]
    log.info("assigned: %.0f%% of posts to a cluster", 100 * (post_story >= 0).mean())

    # Yesterday's sample, filtered the same way, on today's centroids.
    yday = day - dt.timedelta(days=1)
    ys, _ = leave_out(pq.read_table(DATA / f"sample-{yday}.parquet"), yday, adult_uris, adult_dids)
    ystory, _ = placed(project(vectors(ys["vector"])), cent, floor)
    yday_count = np.bincount(ystory[ystory >= 0], minlength=k)

    full = full_centroids(vec, post_story, k)
    report(day, posts, texts, gid, first, sim, post_story, k, yday_count, len(ys), full, total, dropped,
           time.monotonic() - t0)


def full_centroids(vec: np.ndarray, post_story: np.ndarray, k: int) -> np.ndarray:
    """Each cluster's mean in the full embedding space, where the model aligns languages."""
    out = np.zeros((k, vec.shape[1]), dtype=np.float32)
    order = np.argsort(post_story, kind="stable")
    bounds = np.searchsorted(post_story[order], np.arange(k + 1))
    for c in range(k):
        idx = order[bounds[c]:bounds[c + 1]]
        for s in range(0, len(idx), 50_000):
            out[c] += vec[idx[s:s + 50_000]].astype(np.float32).sum(axis=0)
    return normalize(out)


BASELINE_MIN = 20_000  # posts a language needs for its own hourly baseline; others use everyone's


def growth_of(today: np.ndarray, yday: np.ndarray, n: int, n_yday: int) -> np.ndarray:
    """Share of today's posts over share of yesterday's sample (smoothed so tiny clusters don't explode)."""
    return (today / n + 2e-5) / (yday / max(n_yday, 1) + 2e-5)


def report(day, posts, texts, gid, first, sim, post_story, k, yday_count, n_yday, full, total, dropped, seconds,
           top_stories=30, top_topics=15) -> None:
    langs = [lang_of(x) for x in posts["langs"].to_pylist()]
    urls = posts["external_uri"].to_pylist()
    titles = posts["external_title"].to_pylist()
    dids = posts["did"].to_pylist()
    uris = posts["uri"].to_pylist()
    hours = pc.hour(posts["created_at"]).to_numpy(zero_copy_only=False)
    n = len(posts)
    order = np.argsort(post_story, kind="stable")
    bounds = np.searchsorted(post_story[order], np.arange(k + 1))
    members = [order[bounds[c]:bounds[c + 1]] for c in range(k)]
    size = np.array([len(m) for m in members])
    dom = [collections.Counter(langs[i] for i in m).most_common(1)[0][0] if len(m) else "?" for m in members]

    # Timing is judged against each language's own day: Japanese posts peak in Japanese daytime.
    lang_arr = np.array(langs)
    every = np.bincount(hours, minlength=24) / n
    by_lang = {l: np.bincount(hours[lang_arr == l], minlength=24) / cnt
               for l, cnt in collections.Counter(langs).items() if cnt >= BASELINE_MIN}
    baseline = np.stack([by_lang.get(dom[c], every) for c in range(k)])
    hourly = np.stack([np.bincount(hours[m], minlength=24) for m in members])
    growth = growth_of(size, yday_count, n, n_yday)
    burst = np.array([merge.relative_burst(hourly[c], baseline[c])[0] for c in range(k)])
    links = [{u for i in m if (u := urls[i]) and domain(u)} for m in members]
    group = merge.groups(full, hourly, baseline, links, merge.eventful(size, growth, burst))

    ng = group.max() + 1
    gm = [np.flatnonzero(group == g) for g in range(ng)]
    gposts = [np.concatenate([members[c] for c in cs]) for cs in gm]
    gsize = np.array([len(p) for p in gposts])
    ggrowth = growth_of(gsize, np.array([yday_count[cs].sum() for cs in gm]), n, n_yday)
    gbursts = [merge.relative_burst(hourly[cs].sum(axis=0),
                                    (baseline[cs] * size[cs, None]).sum(axis=0) / max(size[cs].sum(), 1))
               for cs in gm]
    # Rank by excess: posts above yesterday's rate. A rate (growth x burst) favours small sharp clusters
    # and penalises merging a story with its chatter; excess counts how much more was said about it.
    excess = gsize * (1 - 1 / ggrowth)
    score = np.where((gsize >= 500) & (ggrowth >= merge.EVENTFUL_GROWTH), excess, 0)

    rng = np.random.default_rng(0)

    def sample_docs(idx):
        return [texts[i] for i in (rng.choice(idx, size=min(len(idx), 3000), replace=False) if len(idx) else [])]

    def dominant(idx):
        return collections.Counter(langs[i] for i in idx).most_common(1)[0][0] if len(idx) else "?"

    multi = [g for g in range(ng) if len(gm[g]) > 1]
    every_words = label_words([sample_docs(m) for m in members] + [sample_docs(gposts[g]) for g in multi],
                              dom + [dominant(gposts[g]) for g in multi], background_sample(texts, langs))
    words = every_words[:k]
    gwords = [words[cs[0]] for cs in gm]
    for g, w in zip(multi, every_words[k:]):
        gwords[g] = w

    np.savez(DATA / f"clusters-{day}.npz", centroid=full, hourly=hourly, baseline=baseline, size=size,
             growth=growth, burst=burst, lang=np.array(dom), group=group)
    pq.write_table(pa.table({"cluster": pa.array(range(k)), "words": pa.array(words),
                             "links": pa.array([sorted(x) for x in links])}), DATA / f"clusters-{day}.parquet")
    pq.write_table(pa.table({"uri": posts["uri"], "cluster": pa.array(post_story),
                             "story": pa.array(np.where(post_story >= 0, group[post_story.clip(0)], -1))}),
                   DATA / f"stories-{day}.parquet")

    def line(g):
        parts = f" in {len(gm[g])} clusters" if len(gm[g]) > 1 else ""
        b, h = gbursts[g]
        return (f"{', '.join(gwords[g][:6]) or '(no words)'} | {gsize[g]:,} posts{parts}, "
                f"{max(excess[g], 0):,.0f} above yesterday's rate | growth {ggrowth[g]:.1f}x | burst {b:.1f}x from {h:02d}Z")

    stories = [g for g in np.argsort(-score) if score[g] > 0][:top_stories]
    topics = list(np.argsort(-gsize)[:top_topics])
    lines = [f"# Stories, {day}", "",
             f"{total:,} embedded posts; left out {', '.join(f'{v:,} {k_}' for k_, v in dropped.items())}; "
             f"{n:,} clustered into {k} clusters, {100 * (post_story >= 0).mean():.0f}% assigned; "
             f"{len(multi)} stories merged from {sum(len(gm[g]) for g in multi)} clusters; {seconds / 60:.0f} min.", "",
             "Ranked by posts above yesterday's rate: size x (1 - 1/growth), where growth is the story's share of "
             "today's posts over its share of yesterday's; stories of 500+ posts growing 1.5x+. Burst is the share of posts in "
             "the busiest 3 hours over the language's share in those hours.", "", "## Stories", ""]
    lines += [f"{r}. {line(g)}" for r, g in enumerate(stories, 1)]
    lines += ["", "## Largest (standing topics)", ""]
    lines += [f"- {line(g)}" for g in topics]
    lines += ["", "## Story details", ""]
    for r, g in enumerate(stories, 1):
        idx = gposts[g]
        lc = collections.Counter(langs[i] for i in idx)
        dc = collections.Counter(d for i in idx if (d := domain(urls[i])))
        tc = collections.Counter(titles[i] for i in idx if titles[i])
        lines += [f"### {r}. {', '.join(gwords[g][:6])}", "",
                  f"- {gsize[g]:,} posts, {len(np.unique(gid[idx])):,} distinct texts, "
                  f"{len({dids[i] for i in idx}):,} authors; growth {ggrowth[g]:.1f}x, "
                  f"burst {gbursts[g][0]:.1f}x from {gbursts[g][1]:02d}Z",
                  f"- languages: {', '.join(f'{l} {100 * v / len(idx):.0f}%' for l, v in lc.most_common(5))}",
                  f"- words: {', '.join(gwords[g])}"]
        if dc:
            lines.append(f"- link domains: {', '.join(f'{d} ({v})' for d, v in dc.most_common(5))}")
        if tc:
            lines.append(f"- top link titles: {' | '.join(f'{t[:80]} ({v})' for t, v in tc.most_common(3))}")
        cs = sorted(gm[g], key=lambda c: -size[c])
        if len(cs) > 1:
            lines.append("- clusters: " + "; ".join(f"{dom[c]} {size[c]:,} ({', '.join(words[c][:4])})" for c in cs))
        lines.append("- nearest the centre:")
        per = max(1, 6 // len(cs))
        for c in cs[:6]:
            for gg in sorted(np.unique(gid[members[c]]), key=lambda gg: -sim[gg])[:per]:
                i = first[gg]
                lines.append(f"  - {texts[i][:200].replace(chr(10), ' / ')!r} ({bsky_url(uris[i])})")
        lines.append("")
    out = DATA / f"stories-{day}.md"
    out.write_text("\n".join(lines))
    log.info("report: %s", out)


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)s: %(message)s")
    p = argparse.ArgumentParser()
    p.add_argument("day", type=dt.date.fromisoformat)
    p.add_argument("--sample", type=int, default=250_000)
    p.add_argument("--min-cluster", type=int, default=60)
    a = p.parse_args()
    run(a.day, a.sample, a.min_cluster)


if __name__ == "__main__":
    main()

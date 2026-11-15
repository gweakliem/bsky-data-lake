"""Step 2: communities of accounts from an SVD of the likes graph.

    uv run python -m bsky_cluster.communities 2026-11-07 2026-11-13 [--k 120]

Reads data/cluster/likes-<day>.parquet and texts-<day>.parquet (fetch.py). Writes
data/cluster/communities-<start>_<end>.parquet (did, community), the 64-d account and
post embeddings (.npy with their id lists) and communities-<start>_<end>.md.

The matrix is accounts x posts (an account liked a post), keeping accounts with at
least --min-account likes and posts with at least --min-post. Columns are weighted
by log(accounts / likes), so viral posts count less, and rows are L2-normalized.
A truncated SVD gives the embeddings; k-means on the accounts gives communities,
labelled by the posts their members like far more than everyone else (lift).
"""

from __future__ import annotations

import argparse
import collections
import datetime as dt
import logging
import time

import duckdb
import numpy as np
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq
import scipy.sparse as sp
from sklearn.cluster import MiniBatchKMeans
from sklearn.decomposition import TruncatedSVD

from bsky_cluster import filters
from bsky_cluster.fetch import DATA, days
from bsky_cluster.labels import background_sample, label_words, lang_of
from bsky_cluster.stories import bsky_url, normalize

ADULT_FLAG = 0.3

log = logging.getLogger("bsky_cluster.communities")


def edges(start: dt.date, end: dt.date, min_account: int, min_post: int):
    files = [str(DATA / f"likes-{d}.parquet") for d in days(start, end)]
    con = duckdb.connect()
    con.execute("SET preserve_insertion_order = false")
    con.execute(f"CREATE TABLE e AS SELECT DISTINCT did, subject_uri AS uri FROM read_parquet({files!r})")
    total = con.execute("SELECT count(*), count(DISTINCT did), count(DISTINCT uri) FROM e").fetchone()
    log.info("%d distinct likes by %d accounts on %d posts", *total)
    for _ in range(2):  # pruning one side changes the other's degrees
        con.execute(f"""
            DELETE FROM e WHERE did IN (SELECT did FROM e GROUP BY did HAVING count(*) < {min_account})
                             OR uri IN (SELECT uri FROM e GROUP BY uri HAVING count(*) < {min_post})""")
    con.execute("CREATE TABLE a AS SELECT did, row_number() OVER (ORDER BY did) - 1 AS i FROM (SELECT DISTINCT did FROM e)")
    con.execute("CREATE TABLE p AS SELECT uri, row_number() OVER (ORDER BY uri) - 1 AS j FROM (SELECT DISTINCT uri FROM e)")
    ij = con.execute("SELECT a.i, p.j FROM e JOIN a USING (did) JOIN p USING (uri)").fetch_arrow_table()
    accounts = con.execute("SELECT did FROM a ORDER BY i").fetch_arrow_table()["did"]
    posts = con.execute("SELECT uri FROM p ORDER BY j").fetch_arrow_table()["uri"]
    return ij["i"].to_numpy(), ij["j"].to_numpy(), accounts, posts


def run(start: dt.date, end: dt.date, k: int, dims: int, min_account: int, min_post: int, seed: int = 0) -> None:
    t0 = time.monotonic()
    i, j, accounts, posts = edges(start, end, min_account, min_post)
    na, npo = len(accounts), len(posts)
    log.info("kept %d likes: %d accounts x %d posts (%.0f s)", len(i), na, npo, time.monotonic() - t0)

    m = sp.csr_matrix((np.ones(len(i), dtype=np.float32), (i, j)), shape=(na, npo))
    post_likes = np.asarray(m.sum(axis=0)).ravel()
    idf = np.log(na / post_likes).astype(np.float32)
    w = m @ sp.diags(idf)
    w = sp.diags(1 / np.sqrt(np.asarray(w.multiply(w).sum(axis=1)).ravel()).clip(1e-12)) @ w

    t = time.monotonic()
    svd = TruncatedSVD(n_components=dims, n_iter=7, random_state=seed)
    acc = normalize(svd.fit_transform(w).astype(np.float32))
    post = normalize((svd.components_.T * svd.singular_values_).astype(np.float32))
    log.info("SVD %d: %.0f s", dims, time.monotonic() - t)

    t = time.monotonic()
    km = MiniBatchKMeans(n_clusters=k, batch_size=8192, n_init=3, random_state=seed).fit(acc)
    community = km.labels_
    log.info("k-means %d: %.0f s; sizes %s", k, time.monotonic() - t, sorted(np.bincount(community), reverse=True)[:10])

    stem = f"{start}_{end}"
    pq.write_table(pa.table({"did": accounts, "community": pa.array(community)}), DATA / f"communities-{stem}.parquet")
    np.save(DATA / f"account-emb-{stem}.npy", acc)
    np.save(DATA / f"post-emb-{stem}.npy", post)
    pq.write_table(pa.table({"uri": posts}), DATA / f"post-ids-{stem}.parquet")
    report(start, end, m, community, k, posts, post_likes, time.monotonic() - t0)


def report(start, end, m, community, k, posts, post_likes, seconds, top_posts=300) -> None:
    na = m.shape[0]
    onehot = sp.csr_matrix((np.ones(na, dtype=np.float32), (community, np.arange(na))), shape=(k, na))
    by_comm = (onehot @ m).toarray() if m.shape[1] * k < 2e9 else None  # k x posts like counts
    sizes = np.bincount(community, minlength=k)
    base = post_likes / na
    texts = {}
    want: dict[int, list[int]] = {}
    for c in range(k):
        likes = by_comm[c]
        lift = (likes / sizes[c]) / base
        ok = np.flatnonzero(likes >= max(5, 0.002 * sizes[c]))
        want[c] = list(ok[np.argsort(-lift[ok])[:top_posts]])
    need = {posts[j].as_py() for c in want for j in want[c]}
    for d in days(start, end):
        t = pq.read_table(DATA / f"texts-{d}.parquet")
        t = t.filter(pc.is_in(t["uri"], value_set=pa.array(list(need))))
        for r in t.to_pylist():
            texts[r["uri"]] = r
    log.info("texts for %d of %d label posts", len(texts), len(need))

    def doc(r):
        return ((r or {}).get("text") or "") + " " + ((r or {}).get("external_title") or "")

    docs, meta, dom = [], [], []
    for c in range(k):
        rows = [texts.get(posts[j].as_py()) for j in want[c]]
        docs.append([doc(r) for r in rows if r])
        meta.append(rows)
        dom.append(collections.Counter(lang_of(r.get("langs")) for r in rows if r).most_common(1)[0][0]
                   if any(rows) else "?")
    bg = pq.read_table(DATA / f"texts-{end}.parquet").to_pylist()
    words = label_words(docs, dom, background_sample([doc(r) for r in bg], [lang_of(r["langs"]) for r in bg]))

    # Adult share of what each community over-likes: self-labelled posts, or posts by flagged accounts.
    adult_uris = filters.adult_posts(start, end)
    adult_dids = filters.adult_accounts(start, end, adult_uris)
    adult = []
    for c in range(k):
        us = [posts[j].as_py() for j in want[c]]
        adult.append(sum(u in adult_uris or filters.did_of(u) in adult_dids for u in us) / max(len(us), 1))

    lines = [f"# Communities, {start} to {end}", "",
             f"{na:,} accounts x {m.shape[1]:,} posts, {m.nnz:,} likes; {k} communities; {seconds / 60:.0f} min.",
             f"[adult] marks communities where {ADULT_FLAG:.0%}+ of the posts they over-like are adult "
             "(self-labelled, or by an account that self-labels).", ""]
    for rank, c in enumerate(np.argsort(-sizes), 1):
        lc = collections.Counter(l for r in meta[c] if r for l in (r.get("langs") or ["?"]))
        n = sum(lc.values()) or 1
        flag = " [adult]" if adult[c] >= ADULT_FLAG else ""
        lines += [f"## {rank}. {', '.join(words[c][:6])}{flag}", "",
                  f"- **{sizes[c]:,} accounts**; adult share of over-liked posts {100 * adult[c]:.0f}%",
                  f"- languages of the posts they over-like: {', '.join(f'{l} {100 * v / n:.0f}%' for l, v in lc.most_common(4))}",
                  f"- words: {', '.join(words[c])}", "- most over-liked:"]
        for j, r in list(zip(want[c], meta[c]))[:4]:
            if r and r.get("text"):
                lines.append(f"  - {r['text'][:160].replace(chr(10), ' / ')!r} ({bsky_url(posts[j].as_py())})")
        lines.append("")
    out = DATA / f"communities-{start}_{end}.md"
    out.write_text("\n".join(lines))
    log.info("report: %s", out)


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)s: %(message)s")
    p = argparse.ArgumentParser()
    p.add_argument("start", type=dt.date.fromisoformat)
    p.add_argument("end", type=dt.date.fromisoformat)
    p.add_argument("--k", type=int, default=120)
    p.add_argument("--dims", type=int, default=64)
    p.add_argument("--min-account", type=int, default=10)
    p.add_argument("--min-post", type=int, default=5)
    a = p.parse_args()
    run(a.start, a.end, a.k, a.dims, a.min_account, a.min_post)


if __name__ == "__main__":
    main()

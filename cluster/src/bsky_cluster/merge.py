"""Merge clusters that are one story.

    uv run python -m bsky_cluster.merge 2026-11-13     # print the groups from clusters-<day>.* (for tuning)

HDBSCAN splits a big story by angle ("FAA ground stop", "planes grounded", "Trump
blames...") and by language (the Microsoft outage in English, French, Portuguese
and Japanese). The full 1024-d embedding aligns languages, so the clusters' mean
vectors are close; what tells two "outage" stories apart is timing.

Timing is each cluster's hourly profile minus its language's (Japanese posts peak
in Japanese daytime whatever they are about), so a news spike shows as excess at
the hour it broke. Two clusters are linked when either
  - their centroids are close and they peak together (SAME_TOPIC, SAME_TIME),
  - their centroids are close and they share links to the same pages (SHARED_LINKS),
  - their centroids are very close and timing doesn't contradict (CLOSE_TOPIC):
    the same story in another time zone can peak hours apart,
and never when their timing is opposite (OPPOSITE_TIME). Groups are the connected
components. Only eventful clusters (growing or bursting) are merged; standing
topics stay as they are.
"""

from __future__ import annotations

import argparse
import datetime as dt

import numpy as np
import pyarrow.parquet as pq

from bsky_cluster.fetch import DATA

SAME_TOPIC, SAME_TIME = 0.85, 0.3
SHARED_LINKS, SHARED_TOPIC = 3, 0.80
CLOSE_TOPIC = 0.88
OPPOSITE_TIME = -0.4
EVENTFUL_GROWTH, EVENTFUL_BURST, EVENTFUL_SIZE = 1.5, 2.0, 200


def window(h: np.ndarray, hours: int = 3) -> np.ndarray:
    """Sums over each run of `hours` consecutive hours (wrapping), along the last axis."""
    return sum(np.roll(h, -i, axis=-1) for i in range(hours))


def relative_burst(hourly: np.ndarray, baseline: np.ndarray) -> tuple[float, int]:
    """Busiest 3 hours' share of a cluster's posts over its language's share in those hours, and the first hour."""
    share = hourly / max(hourly.sum(), 1)
    r = window(share) / window(baseline).clip(1e-9)
    i = int(r.argmax())
    return float(r[i]), i


def timing(hourly: np.ndarray, baseline: np.ndarray) -> np.ndarray:
    """Unit vectors of each row's hourly share in excess of its baseline; their dot product is a correlation."""
    e = hourly / hourly.sum(axis=1, keepdims=True).clip(1) - baseline
    return e / np.linalg.norm(e, axis=1, keepdims=True).clip(1e-9)


def eventful(size: np.ndarray, growth: np.ndarray, burst: np.ndarray) -> np.ndarray:
    return (size >= EVENTFUL_SIZE) & ((growth >= EVENTFUL_GROWTH) | (burst >= EVENTFUL_BURST))


def groups(centroid: np.ndarray, hourly: np.ndarray, baseline: np.ndarray, links: list[set[str]],
           candidates: np.ndarray) -> np.ndarray:
    """A group id per cluster: linked candidates share one, everything else is on its own."""
    k = len(centroid)
    parent = np.arange(k)

    def root(i: int) -> int:
        while parent[i] != i:
            parent[i] = parent[parent[i]]
            i = parent[i]
        return i

    ev = np.flatnonzero(candidates)
    sem = centroid[ev] @ centroid[ev].T
    tim = timing(hourly[ev], baseline[ev]) @ timing(hourly[ev], baseline[ev]).T
    for a in range(len(ev)):
        for b in range(a + 1, len(ev)):
            s, t = sem[a, b], tim[a, b]
            if t <= OPPOSITE_TIME:
                continue
            shared = len(links[ev[a]] & links[ev[b]])
            if (s >= SAME_TOPIC and t >= SAME_TIME) or (s >= SHARED_TOPIC and shared >= SHARED_LINKS) \
                    or s >= CLOSE_TOPIC:
                parent[root(ev[a])] = root(ev[b])
    roots = np.array([root(i) for i in range(k)])
    _, gid = np.unique(roots, return_inverse=True)
    return gid


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("day", type=dt.date.fromisoformat)
    a = p.parse_args()
    z = np.load(DATA / f"clusters-{a.day}.npz")
    meta = pq.read_table(DATA / f"clusters-{a.day}.parquet").to_pylist()
    links = [set(r["links"]) for r in meta]
    cand = eventful(z["size"], z["growth"], z["burst"])
    gid = groups(z["centroid"], z["hourly"], z["baseline"], links, cand)
    for g in np.unique(gid):
        cs = np.flatnonzero(gid == g)
        if len(cs) < 2:
            continue
        print(f"-- {z['size'][cs].sum():,} posts")
        for c in cs:
            print(f"   [{c} {z['lang'][c]} {z['size'][c]:,} growth {z['growth'][c]:.1f} burst {z['burst'][c]:.1f}] "
                  f"{', '.join(meta[c]['words'][:5])}")
    print(f"{cand.sum()} eventful clusters -> {len(np.unique(gid[cand]))} groups")


if __name__ == "__main__":
    main()

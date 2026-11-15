import numpy as np

from bsky_cluster import merge


def unit(v):
    v = np.asarray(v, dtype=np.float32)
    return v / np.linalg.norm(v)


def spike(hour, n=1000):
    h = np.full(24, 10.0)
    h[hour:hour + 3] += n
    return h


def half_day(start):
    """Most posts in the 12 hours from `start`: a story's broad peak."""
    return np.roll(np.array([100.0] * 12 + [5.0] * 12), start)


FLAT = np.full(24, 1 / 24)


def test_same_topic_and_time_merge_but_opposite_time_does_not():
    # 0 and 1: one story in the afternoon. 2: same topic, busy in the other half of the day (another "outage").
    topic = unit([1, 0.1, 0])
    cent = np.stack([topic, unit([1, 0.12, 0]), unit([1, 0.11, 0.01])])
    hourly = np.stack([half_day(12), half_day(13), half_day(0)])
    g = merge.groups(cent, hourly, np.stack([FLAT] * 3), [set(), set(), set()], np.ones(3, bool), np.ones(3, bool))
    assert g[0] == g[1] and g[2] != g[0]


def test_shared_links_merge_a_story_across_time_zones():
    # Same story in another language: further apart in topic, timing neither aligned nor opposite.
    cent = np.stack([unit([1, 0, 0]), unit([1, 0.7, 0])])  # cosine ~0.82
    hourly = np.stack([spike(8), spike(14)])
    link = {"https://example.com/outage"}
    others = {"https://example.com/a", "https://example.com/b"}
    assert merge.groups(cent, hourly, np.stack([FLAT] * 2), [set(), set()], np.ones(2, bool), np.ones(2, bool)).tolist() == [0, 1]
    shared = [link | others, link | others]
    g = merge.groups(cent, hourly, np.stack([FLAT] * 2), shared, np.ones(2, bool), np.ones(2, bool))
    assert g[0] == g[1]


def test_only_candidates_merge():
    cent = np.stack([unit([1, 0, 0])] * 2)
    hourly = np.stack([spike(5)] * 2)
    g = merge.groups(cent, hourly, np.stack([FLAT] * 2), [set(), set()], np.array([True, False]), np.ones(2, bool))
    assert g[0] != g[1]


def test_relative_burst_is_against_the_language_baseline():
    # A cluster that just follows its language's day has no burst.
    day = np.array([1] * 8 + [10] * 8 + [1] * 8, dtype=float)
    b, _ = merge.relative_burst(day * 50, day / day.sum())
    assert abs(b - 1) < 1e-6
    assert merge.relative_burst(spike(3), FLAT)[1] == 3


def test_two_flat_clusters_do_not_chain_into_a_story():
    # 0 is a growing story; 1 is flat and joins it; 2 is flat and only linked to 1.
    cent = np.stack([unit([1, 0, 0]), unit([1, 0.45, 0]), unit([1, 0.9, 0])])  # 0-1 0.91, 1-2 0.95, 0-2 0.74
    hourly = np.stack([half_day(12)] * 3)
    g = merge.groups(cent, hourly, np.stack([FLAT] * 3), [set()] * 3, np.ones(3, bool),
                     np.array([True, False, False]))
    assert g[0] == g[1] and g[2] != g[1]

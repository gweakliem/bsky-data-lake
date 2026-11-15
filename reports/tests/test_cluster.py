"""cluster.py: where the clustering snapshot comes from, and what a missing one looks like."""

import io
import json

import cluster


class FakeS3:
    class exceptions:
        class NoSuchKey(Exception):
            pass

    def __init__(self, objects):
        self.objects = objects

    def get_object(self, Bucket, Key):
        if (Bucket, Key) not in self.objects:
            raise self.exceptions.NoSuchKey(Key)
        return {"Body": io.BytesIO(self.objects[(Bucket, Key)])}


def env(monkeypatch, **values):
    for k in ("BSKY_REPORTS_FIXTURE", "BSKY_CLUSTER_SNAPSHOT", "BSKY_REPORTS_SITE_URL"):
        monkeypatch.delenv(k, raising=False)
    for k, v in values.items():
        monkeypatch.setenv(k, v)


def test_no_location_or_no_file_is_unavailable(monkeypatch, tmp_path):
    env(monkeypatch)
    assert cluster.snapshot("stories") == {"available": False}
    env(monkeypatch, BSKY_CLUSTER_SNAPSHOT=str(tmp_path))
    assert cluster.snapshot("stories") == {"available": False}


def test_reads_a_local_directory(monkeypatch, tmp_path):
    (tmp_path / "stories.json").write_text(json.dumps({"day": "2026-11-13", "stories": []}))
    env(monkeypatch, BSKY_CLUSTER_SNAPSHOT=str(tmp_path))
    assert cluster.snapshot("stories") == {"available": True, "day": "2026-11-13", "stories": []}


def test_in_cluster_build_reads_under_the_site_prefix(monkeypatch):
    env(monkeypatch, BSKY_REPORTS_SITE_URL="s3://bucket/reports")
    s3 = FakeS3({("bucket", "reports/inputs/cluster/communities.json"): b'{"communities": []}'})
    assert cluster.snapshot("communities", s3) == {"available": True, "communities": []}
    assert cluster.snapshot("stories", s3) == {"available": False}


def test_fixture_has_the_pages_fields(monkeypatch):
    env(monkeypatch, BSKY_REPORTS_FIXTURE="1")
    s = cluster.snapshot("stories")
    assert s["available"] and {"hourly", "examples", "communities", "clusters"} <= set(s["stories"][0])
    c = cluster.snapshot("communities")
    assert not any(x["adult"] for x in c["communities"])

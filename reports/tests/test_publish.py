"""publish.py and sync_site.py against an in-memory S3 stand-in."""

import datetime as dt
import io
from pathlib import Path

import publish
import sync_site

URL = "s3://bucket/reports"


class FakeS3:
    class exceptions:
        class NoSuchKey(Exception):
            pass

    def __init__(self):
        self.objects: dict[str, bytes] = {}

    def upload_file(self, path, bucket, key, ExtraArgs=None):
        self.objects[key] = Path(path).read_bytes()

    def put_object(self, Bucket, Key, Body, **_):
        self.objects[Key] = Body

    def get_object(self, Bucket, Key):
        if Key not in self.objects:
            raise self.exceptions.NoSuchKey(Key)
        return {"Body": io.BytesIO(self.objects[Key])}

    def download_file(self, bucket, key, path):
        Path(path).write_bytes(self.objects[key])

    def delete_objects(self, Bucket, Delete):
        for o in Delete["Objects"]:
            del self.objects[o["Key"]]

    def get_paginator(self, _):
        s3 = self

        class Paginator:
            def paginate(self, Bucket, Prefix, Delimiter=None):
                keys = sorted(k for k in s3.objects if k.startswith(Prefix))
                if Delimiter:
                    prefixes = sorted({Prefix + k[len(Prefix):].split(Delimiter)[0] + Delimiter for k in keys})
                    yield {"CommonPrefixes": [{"Prefix": p} for p in prefixes]}
                else:
                    yield {"Contents": [{"Key": k} for k in keys]}

        return Paginator()


def site(tmp_path: Path, text: str) -> Path:
    dist = tmp_path / f"dist-{text}"
    (dist / "_file").mkdir(parents=True)
    (dist / "index.html").write_text(text)
    (dist / "_file" / "data.csv").write_text("a\n1\n")
    return dist


def at(minute: int) -> dt.datetime:
    return dt.datetime(2026, 11, 6, 16, minute, tzinfo=dt.timezone.utc)


def test_publish_uploads_then_points_current_at_the_build(tmp_path):
    s3 = FakeS3()
    build = publish.publish(s3, URL, site(tmp_path, "one"), now=at(30))
    assert build == "20261106T163000Z"
    assert s3.objects["reports/builds/20261106T163000Z/index.html"] == b"one"
    assert s3.objects["reports/builds/20261106T163000Z/_file/data.csv"] == b"a\n1\n"
    assert s3.objects["reports/current"] == build.encode()


def test_publish_keeps_the_newest_builds(tmp_path):
    s3 = FakeS3()
    for minute in range(5):
        publish.publish(s3, URL, site(tmp_path, str(minute)), now=at(minute))
    assert publish.build_ids(s3, "bucket", "reports") == [f"20261106T160{m}00Z" for m in (2, 3, 4)]


def test_sync_serves_current_and_switches_on_a_new_build(tmp_path):
    s3, root = FakeS3(), tmp_path / "site"
    (root / "builds").mkdir(parents=True)
    # Nothing published yet: nothing to serve.
    assert sync_site.sync_once(s3, URL, root, None) is None
    assert not (root / "current").exists()

    first = publish.publish(s3, URL, site(tmp_path, "one"), now=at(30))
    assert sync_site.sync_once(s3, URL, root, None) == first
    assert (root / "current" / "index.html").read_text() == "one"
    assert (root / "current" / "_file" / "data.csv").exists()
    # Unchanged: no work.
    assert sync_site.sync_once(s3, URL, root, first) == first

    second = publish.publish(s3, URL, site(tmp_path, "two"), now=at(31))
    assert sync_site.sync_once(s3, URL, root, first) == second
    assert (root / "current" / "index.html").read_text() == "two"
    assert [p.name for p in (root / "builds").iterdir()] == [second]


def test_an_unfinished_upload_is_never_served(tmp_path):
    s3, root = FakeS3(), tmp_path / "site"
    (root / "builds").mkdir(parents=True)
    first = publish.publish(s3, URL, site(tmp_path, "one"), now=at(30))
    sync_site.sync_once(s3, URL, root, None)
    # A build that died after uploading part of its files: `current` still names the first.
    publish.upload(s3, "bucket", "reports", site(tmp_path, "half"), "20261106T163100Z")
    assert sync_site.sync_once(s3, URL, root, first) == first
    assert (root / "current" / "index.html").read_text() == "one"

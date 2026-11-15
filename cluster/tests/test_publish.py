import json

import pytest

from bsky_cluster import publish


def test_refuses_adult_communities_and_non_default_runs():
    with pytest.raises(SystemExit):
        publish.check("communities.json", json.dumps({"communities": [{"adult": True}]}).encode())
    with pytest.raises(SystemExit):
        publish.check("stories.json", json.dumps({"adult": "only", "stories": []}).encode())
    publish.check("communities.json", json.dumps({"communities": [{"adult": False}]}).encode())
    publish.check("stories.json", json.dumps({"adult": "exclude", "stories": []}).encode())


def test_refuses_files_too_big_for_the_site():
    with pytest.raises(SystemExit):
        publish.check("stories.json", b" " * publish.MAX_BYTES)


def test_uploads_under_inputs_cluster(tmp_path, monkeypatch):
    monkeypatch.setattr(publish, "SITE", tmp_path)
    (tmp_path / "stories.json").write_text(json.dumps({"adult": "exclude", "stories": []}))
    (tmp_path / "communities.json").write_text(json.dumps({"communities": []}))
    put = []

    class S3:
        def put_object(self, **kw):
            put.append((kw["Bucket"], kw["Key"]))

    publish.publish("s3://bucket/reports", S3())
    assert put == [("bucket", "reports/inputs/cluster/stories.json"), ("bucket", "reports/inputs/cluster/communities.json")]

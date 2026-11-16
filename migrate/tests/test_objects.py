import pytest

from bsky_migrate import objects


class FakeS3:
    def __init__(self, buckets: dict[str, dict[str, int]]):
        self.buckets = buckets
        self.copies: list[tuple[str, str, str, str]] = []

    def get_paginator(self, name):
        assert name == "list_objects_v2"
        fake = self

        class P:
            def paginate(self, Bucket, Prefix):
                keys = sorted(k for k in fake.buckets.get(Bucket, {}) if k.startswith(Prefix))
                for i in range(0, len(keys), 2):  # two per page: exercises paging
                    yield {"Contents": [{"Key": k, "Size": fake.buckets[Bucket][k]} for k in keys[i:i + 2]]}
                if not keys:
                    yield {}

        return P()

    def copy(self, source, bucket, key):
        self.copies.append((source["Bucket"], source["Key"], bucket, key))
        self.buckets.setdefault(bucket, {})[key] = self.buckets[source["Bucket"]][source["Key"]]


def test_copies_under_the_new_prefix_and_skips_what_is_already_there():
    s3 = FakeS3({
        "old": {"lance/a": 10, "lance/sub/b": 20, "lance/c": 30, "lance-test/x": 1, "reports/y": 2},
        "new": {"lance/a": 10, "lance/c": 29},  # c is a partial copy: different size
    })
    r = objects.copy_prefix(s3, "s3://old/lance/", "s3://new/lance/", workers=2)
    assert (r.objects, r.copied, r.skipped, r.bytes_copied) == (3, 2, 1, 50)
    assert sorted(c[3] for c in s3.copies) == ["lance/c", "lance/sub/b"]
    assert "lance-test/x" not in s3.buckets["new"]
    again = objects.copy_prefix(s3, "s3://old/lance/", "s3://new/lance/")
    assert again.copied == 0


def test_prefixes_must_be_directories():
    with pytest.raises(ValueError):
        objects.copy_prefix(FakeS3({}), "s3://old/lance", "s3://new/lance/")

"""Checks the stack's resources using Pulumi mocks (no AWS calls)."""

import importlib.util
import json
import os
from pathlib import Path

import pulumi
import pytest


IAM_TYPES = ("aws:iam/role:Role", "aws:iam/policy:Policy")


class Mocks(pulumi.runtime.Mocks):
    def __init__(self):
        # Inputs of every IAM role and policy registered, by type.
        self.iam: dict[str, list[dict]] = {t: [] for t in IAM_TYPES}

    def new_resource(self, args: pulumi.runtime.MockResourceArgs):
        state = dict(args.inputs)
        name = args.inputs.get("name", args.name)
        if args.typ == "aws:sqs/queue:Queue":
            state["arn"] = f"arn:aws:sqs:us-west-2:123456789012:{name}"
            state["url"] = f"https://sqs.us-west-2.amazonaws.com/123456789012/{name}"
        elif args.typ in IAM_TYPES:
            self.iam[args.typ].append(state)
            kind = args.typ.rsplit(":", 1)[1].lower()
            state["arn"] = f"arn:aws:iam::123456789012:{kind}{args.inputs.get('path', '/')}{name}"
        return f"{args.name}-id", state

    def call(self, args: pulumi.runtime.MockCallArgs):
        if args.token == "aws:index/getCallerIdentity:getCallerIdentity":
            return {"accountId": "123456789012", "arn": "arn", "id": "id", "userId": "user"}
        if args.token == "aws:index/getRegion:getRegion":
            return {"region": "us-west-2", "name": "us-west-2", "id": "us-west-2"}
        return {}


os.environ["BSKY_BUCKET"] = "test-bucket"
os.environ["BSKY_WAREHOUSE_BUCKET"] = "test-warehouse"
os.environ.pop("BSKY_PREFIX", None)
mocks = Mocks()
pulumi.runtime.set_mocks(mocks, preview=False)

_spec = importlib.util.spec_from_file_location("infra", Path(__file__).with_name("__main__.py"))
infra = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(infra)

import bsky_iam  # noqa: E402  (imported by __main__.py; needs the mocks set first)


@pulumi.runtime.test
def test_notification_only_for_raw_parquet():
    def check(queues):
        (q,) = queues
        assert q.filter_prefix == "jetstream/"
        assert q.filter_suffix == ".parquet"
        assert q.events == ["s3:ObjectCreated:*"]
        assert q.queue_arn.endswith(":bsky-raw-files")

    return infra.notification.queues.apply(check)


@pulumi.runtime.test
def test_queue_redrives_to_dlq():
    def check(args):
        redrive, timeout = args
        assert json.loads(redrive) == {
            "deadLetterTargetArn": "arn:aws:sqs:us-west-2:123456789012:bsky-raw-files-dlq",
            "maxReceiveCount": 5,
        }
        assert timeout == 600

    return pulumi.Output.all(infra.queue.redrive_policy, infra.queue.visibility_timeout_seconds).apply(check)


@pulumi.runtime.test
def test_queue_accepts_only_this_bucket():
    def check(policy):
        (stmt,) = json.loads(policy)["Statement"]
        assert stmt["Principal"] == {"Service": "s3.amazonaws.com"}
        assert stmt["Condition"]["ArnLike"]["aws:SourceArn"] == "arn:aws:s3:::test-bucket"
        assert stmt["Condition"]["StringEquals"]["aws:SourceAccount"] == "123456789012"

    return infra.queue_policy.policy.apply(check)


@pulumi.runtime.test
def test_glue_database_location():
    def check(uri):
        assert uri == "s3://test-warehouse/iceberg/bsky_raw"

    return infra.database.location_uri.apply(check)


@pulumi.runtime.test
def test_loader_policy_scopes():
    def check(policy):
        stmts = {s["Sid"]: s for s in json.loads(policy)["Statement"]}
        assert stmts["ReadRawFiles"]["Resource"] == "arn:aws:s3:::test-bucket/jetstream/*"
        assert stmts["WriteIcebergTables"]["Resource"] == "arn:aws:s3:::test-warehouse/iceberg/*"
        assert stmts["ListBuckets"]["Resource"] == ["arn:aws:s3:::test-bucket", "arn:aws:s3:::test-warehouse"]
        assert "glue:CreateDatabase" not in stmts["GlueCatalog"]["Action"]
        assert "arn:aws:glue:us-west-2:123456789012:table/bsky_raw/*" in stmts["GlueCatalog"]["Resource"]

    return infra.loader_policy_document.apply(check)


@pulumi.runtime.test
def test_loader_policy_created_by_default():
    return infra.loader_policy.policy.apply(lambda p: None)


@pulumi.runtime.test
def test_loader_policy_in_delegated_path():
    def check(args):
        path, arn = args
        assert path == "/bsky/"
        assert arn == "arn:aws:iam::123456789012:policy/bsky/bsky-iceberg-loader"

    return pulumi.Output.all(infra.loader_policy.path, infra.loader_policy.arn).apply(check)


_TRUST = json.dumps(
    {
        "Version": "2012-10-17",
        "Statement": [{"Effect": "Allow", "Principal": {"Service": "pods.eks.amazonaws.com"}, "Action": "sts:AssumeRole"}],
    }
)


@pulumi.runtime.test
def test_role_helper_sets_path_and_boundary():
    role = bsky_iam.role("helper-check", assume_role_policy=_TRUST)

    def check(args):
        path, boundary = args
        assert path == "/bsky/"
        assert boundary == "arn:aws:iam::123456789012:policy/bsky-workload-boundary"

    return pulumi.Output.all(role.path, role.permissions_boundary).apply(check)


def test_helpers_refuse_scope_overrides():
    with pytest.raises(ValueError):
        bsky_iam.role("bad", assume_role_policy=_TRUST, path="/")
    with pytest.raises(ValueError):
        bsky_iam.role("bad", assume_role_policy=_TRUST, permissions_boundary=None)
    with pytest.raises(ValueError):
        bsky_iam.policy("bad", policy="{}", path="/")


def test_every_iam_resource_in_delegated_scope():
    # Registration is async, so this runs after the resource tests above; it
    # also catches any Role or Policy created without bsky_iam.
    roles, policies = (mocks.iam[t] for t in IAM_TYPES)
    assert policies, "expected at least the loader policy"
    assert roles, "expected at least the helper-check role"
    for p in policies:
        assert p.get("path") == "/bsky/", p
    for r in roles:
        assert r.get("path") == "/bsky/", r
        assert r.get("permissionsBoundary") == "arn:aws:iam::123456789012:policy/bsky-workload-boundary", r


@pulumi.runtime.test
def test_loader_role_for_pod_identity():
    def check(args):
        trust, arn = args
        (stmt,) = json.loads(trust)["Statement"]
        assert stmt["Principal"] == {"Service": "pods.eks.amazonaws.com"}
        assert stmt["Action"] == ["sts:AssumeRole", "sts:TagSession"]
        assert arn == "arn:aws:iam::123456789012:role/bsky/bsky-iceberg-loader"

    return pulumi.Output.all(infra.loader_role.assume_role_policy, infra.loader_role.arn).apply(check)


def test_ingestor_policy_scopes():
    stmts = {s["Sid"]: s for s in json.loads(infra.ingestor_policy_document)["Statement"]}
    assert stmts["ReadWriteRawFiles"]["Resource"] == "arn:aws:s3:::test-bucket/jetstream/*"
    assert sorted(stmts["ReadWriteRawFiles"]["Action"]) == ["s3:GetObject", "s3:PutObject"]
    # Listing (for resume) only within the raw prefix, and nothing can delete.
    assert stmts["ListRawPrefix"]["Condition"] == {"StringLike": {"s3:prefix": ["jetstream/*"]}}
    assert not any("Delete" in a for s in stmts.values() for a in s["Action"] if isinstance(s["Action"], list))


@pulumi.runtime.test
def test_ingestor_role_for_pod_identity():
    def check(args):
        trust, arn = args
        (stmt,) = json.loads(trust)["Statement"]
        assert stmt["Principal"] == {"Service": "pods.eks.amazonaws.com"}
        assert arn == "arn:aws:iam::123456789012:role/bsky/bsky-ingestor"

    return pulumi.Output.all(infra.ingestor_role.assume_role_policy, infra.ingestor_role.arn).apply(check)

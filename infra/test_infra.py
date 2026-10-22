"""Checks the stack's resources using Pulumi mocks (no AWS calls)."""

import importlib.util
import json
import os
from pathlib import Path

import pulumi


class Mocks(pulumi.runtime.Mocks):
    def new_resource(self, args: pulumi.runtime.MockResourceArgs):
        state = dict(args.inputs)
        name = args.inputs.get("name", args.name)
        if args.typ == "aws:sqs/queue:Queue":
            state["arn"] = f"arn:aws:sqs:us-west-2:123456789012:{name}"
            state["url"] = f"https://sqs.us-west-2.amazonaws.com/123456789012/{name}"
        elif args.typ == "aws:iam/policy:Policy":
            state["arn"] = f"arn:aws:iam::123456789012:policy/{name}"
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
pulumi.runtime.set_mocks(Mocks(), preview=False)

_spec = importlib.util.spec_from_file_location("infra", Path(__file__).with_name("__main__.py"))
infra = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(infra)


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

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
        self.optimizers: list[dict] = []
        # Inputs of every resource, by type.
        self.by_type: dict[str, list[dict]] = {}

    def new_resource(self, args: pulumi.runtime.MockResourceArgs):
        state = dict(args.inputs)
        name = args.inputs.get("name", args.name)
        self.by_type.setdefault(args.typ, []).append(state)
        if args.typ == "aws:sns/topic:Topic":
            state["arn"] = f"arn:aws:sns:us-west-2:123456789012:{name}"
        if args.typ == "aws:cloudwatch/logGroup:LogGroup":
            state["arn"] = f"arn:aws:logs:us-west-2:123456789012:log-group:{name}"
        if args.typ == "aws:sqs/queue:Queue":
            state["arn"] = f"arn:aws:sqs:us-west-2:123456789012:{name}"
            state["url"] = f"https://sqs.us-west-2.amazonaws.com/123456789012/{name}"
        elif args.typ == "aws:glue/catalogTableOptimizer:CatalogTableOptimizer":
            self.optimizers.append(state)
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
os.environ["BSKY_ALERT_EMAIL"] = "alerts@example.com"
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
def test_silver_database_location():
    def check(args):
        name, uri = args
        assert name == "bsky_silver"
        assert uri == "s3://test-warehouse/iceberg/bsky_silver"

    return pulumi.Output.all(infra.silver_database.name, infra.silver_database.location_uri).apply(check)


@pulumi.runtime.test
def test_loader_policy_scopes():
    def check(policy):
        stmts = {s["Sid"]: s for s in json.loads(policy)["Statement"]}
        assert stmts["ReadRawFiles"]["Resource"] == "arn:aws:s3:::test-bucket/jetstream/*"
        assert "sqs:ChangeMessageVisibility" in stmts["ConsumeQueue"]["Action"]
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


@pulumi.runtime.test
def test_container_logs_expire():
    def check(args):
        name, retention = args
        assert name == "/bsky/containers"
        assert retention == 14

    return pulumi.Output.all(infra.container_logs.name, infra.container_logs.retention_in_days).apply(check)


@pulumi.runtime.test
def test_fluent_bit_writes_only_container_logs():
    def check(args):
        policy, trust, arn = args
        (stmt,) = json.loads(policy)["Statement"]
        assert stmt["Resource"] == "arn:aws:logs:us-west-2:123456789012:log-group:/bsky/containers:*"
        # The group is managed here, so the shipper can't create (or recreate) it.
        assert "logs:CreateLogGroup" not in stmt["Action"]
        (trust_stmt,) = json.loads(trust)["Statement"]
        assert trust_stmt["Principal"] == {"Service": "pods.eks.amazonaws.com"}
        assert arn == "arn:aws:iam::123456789012:role/bsky/bsky-fluent-bit"

    return pulumi.Output.all(
        infra.fluent_bit_policy_document, infra.fluent_bit_role.assume_role_policy, infra.fluent_bit_role.arn
    ).apply(check)



@pulumi.runtime.test
def test_table_optimizer_policy_scopes():
    def check(policy):
        stmts = {s["Sid"]: s for s in json.loads(policy)["Statement"]}
        # Only the Iceberg tables, never the raw bucket.
        assert stmts["RewriteTableFiles"]["Resource"] == "arn:aws:s3:::test-warehouse/iceberg/*"
        assert stmts["ListTableFiles"]["Condition"] == {"StringLike": {"s3:prefix": ["iceberg/*"]}}
        assert "arn:aws:glue:us-west-2:123456789012:table/bsky_raw/*" in stmts["CommitToCatalog"]["Resource"]
        assert all("/aws-glue/iceberg-" in r for r in stmts["WriteOptimizerLogs"]["Resource"])

    return infra.table_optimizer_policy_document.apply(check)


@pulumi.runtime.test
def test_table_optimizers_for_raw_table():
    def check(args):
        trust, role_arn = args
        (stmt,) = json.loads(trust)["Statement"]
        assert stmt["Principal"] == {"Service": "glue.amazonaws.com"}
        by_type = {o["type"]: o for o in mocks.optimizers}
        assert sorted(by_type) == ["compaction", "orphan_file_deletion", "retention"]
        for o in by_type.values():
            assert (o["databaseName"], o["tableName"]) == ("bsky_raw", "jetstream")
            assert o["configuration"]["roleArn"] == role_arn
            assert o["configuration"]["enabled"] is True
        compaction = by_type["compaction"]["configuration"]["compactionConfiguration"]["icebergConfiguration"]
        # The default (100) never triggers for an hour partition.
        assert compaction["minInputFiles"] == 10
        retention = by_type["retention"]["configuration"]["retentionConfiguration"]["icebergConfiguration"]
        assert retention["cleanExpiredFiles"] is True

    role = infra.optimizer_role
    return pulumi.Output.all(role.assume_role_policy, role.arn).apply(check)



def test_raw_files_expire_after_a_week():
    lifecycles = mocks.by_type["aws:s3/bucketLifecycleConfiguration:BucketLifecycleConfiguration"]
    (lifecycle,) = [lc for lc in lifecycles if lc["bucket"] == "test-bucket"]
    expiring = {r["filter"]["prefix"]: r["expiration"]["days"] for r in lifecycle["rules"] if "expiration" in r}
    assert expiring == {"jetstream/": 7, "jetstream-test/": 7}
    assert all(r["status"] == "Enabled" for r in lifecycle["rules"])


@pulumi.runtime.test
def test_pipeline_alarms_notify_the_alert_topic():
    def check(topic_arn):
        alarms = {a["name"]: a for a in mocks.by_type["aws:cloudwatch/metricAlarm:MetricAlarm"]}
        assert sorted(alarms) == ["bsky-dead-letters", "bsky-ingest-stalled", "bsky-loader-behind"]
        for a in alarms.values():
            assert a["alarmActions"] == [topic_arn] and a["okActions"] == [topic_arn]
        stalled = alarms["bsky-ingest-stalled"]
        assert (stalled["metricName"], stalled["dimensions"]) == ("NumberOfMessagesSent", {"QueueName": "bsky-raw-files"})
        # An idle queue stops reporting, which must count as stalled.
        assert stalled["treatMissingData"] == "breaching"
        assert alarms["bsky-dead-letters"]["dimensions"] == {"QueueName": "bsky-raw-files-dlq"}
        assert alarms["bsky-loader-behind"]["threshold"] == 900
        (sub,) = mocks.by_type["aws:sns/topicSubscription:TopicSubscription"]
        assert (sub["protocol"], sub["endpoint"]) == ("email", "alerts@example.com")

    return infra.alerts.arn.apply(check)


def test_monthly_budget_alerts():
    (budget,) = mocks.by_type["aws:budgets/budget:Budget"]
    assert (budget["limitAmount"], budget["timeUnit"]) == ("250", "MONTHLY")
    kinds = sorted((n["notificationType"], n["threshold"]) for n in budget["notifications"])
    assert kinds == [("ACTUAL", 50), ("ACTUAL", 80), ("ACTUAL", 100), ("FORECASTED", 100)]


@pulumi.runtime.test
def test_silver_reads_bronze_and_writes_only_silver():
    def check(args):
        policy, arn = args
        stmts = {s["Sid"]: s for s in json.loads(policy)["Statement"]}
        assert stmts["ReadBronze"]["Action"] == "s3:GetObject"
        assert stmts["ReadBronze"]["Resource"] == "arn:aws:s3:::test-warehouse/iceberg/bsky_raw/*"
        assert stmts["WriteSilver"]["Resource"] == "arn:aws:s3:::test-warehouse/iceberg/bsky_silver/*"
        # Bronze's catalog entries are read-only; tables are created only in silver.
        assert not any(a in stmts["ReadBronzeCatalog"]["Action"] for a in ["glue:CreateTable", "glue:UpdateTable"])
        assert all("bsky_raw" not in r for r in stmts["WriteSilverCatalog"]["Resource"])
        # No access to the raw-file bucket at all.
        assert "test-bucket" not in policy
        assert arn == "arn:aws:iam::123456789012:role/bsky/bsky-silver"

    return pulumi.Output.all(infra.silver_policy_document, infra.silver_role.arn).apply(check)


@pulumi.runtime.test
def test_athena_workgroup_writes_to_its_own_bucket():
    def check(name):
        assert name == "bsky-athena-results-123456789012-us-west-2-an"
        (bucket,) = mocks.by_type["aws:s3/bucket:Bucket"]
        assert bucket["bucketNamespace"] == "account-regional"
        (block,) = mocks.by_type["aws:s3/bucketPublicAccessBlock:BucketPublicAccessBlock"]
        assert block["bucket"] == name
        assert all(block[k] for k in ["blockPublicAcls", "blockPublicPolicy", "ignorePublicAcls", "restrictPublicBuckets"])
        (lifecycle,) = [
            lc
            for lc in mocks.by_type["aws:s3/bucketLifecycleConfiguration:BucketLifecycleConfiguration"]
            if lc["bucket"] == name
        ]
        (rule,) = lifecycle["rules"]
        assert (rule["filter"]["prefix"], rule["expiration"]["days"]) == ("", 7)
        (wg,) = mocks.by_type["aws:athena/workgroup:Workgroup"]
        conf = wg["configuration"]
        assert conf["enforceWorkgroupConfiguration"] is True
        assert conf["resultConfiguration"]["outputLocation"] == f"s3://{name}/"
        assert conf["engineVersion"]["selectedEngineVersion"] == "Athena engine version 3"

    return infra.athena_results.bucket.apply(check)

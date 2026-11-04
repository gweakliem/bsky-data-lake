"""Checks the apps stack's resources using Pulumi mocks (no cluster or AWS calls)."""

import importlib.util
from pathlib import Path

import pulumi

REPO_URL = "123456789012.dkr.ecr.us-west-2.amazonaws.com/bsky/iceberg-loader"
INGEST_REPO_URL = "123456789012.dkr.ecr.us-west-2.amazonaws.com/bsky/ingest"
SILVER_REPO_URL = "123456789012.dkr.ecr.us-west-2.amazonaws.com/bsky/silver"
STACK_OUTPUTS = {
    "kubeconfig": "{}",
    "namespace": "bsky",
    "repository_urls": {"iceberg-loader": REPO_URL, "ingest": INGEST_REPO_URL, "silver": SILVER_REPO_URL},
    "queue_url": "https://sqs.us-west-2.amazonaws.com/123456789012/bsky-raw-files",
    "raw_table_location": "s3://test-warehouse/iceberg/bsky_raw/jetstream",
    "raw_location": "s3://test-bucket/jetstream",
    "logging_namespace": "logging",
    "container_log_group": "/bsky/containers",
    "silver_location": "s3://test-warehouse/iceberg/bsky_silver",
}


class Mocks(pulumi.runtime.Mocks):
    def new_resource(self, args: pulumi.runtime.MockResourceArgs):
        if args.typ == "pulumi:pulumi:StackReference":
            return f"{args.name}-id", {"outputs": STACK_OUTPUTS}
        return f"{args.name}-id", dict(args.inputs)

    def call(self, args: pulumi.runtime.MockCallArgs):
        return {}


pulumi.runtime.set_mocks(Mocks(), project="bsky-apps", stack="dev", preview=False)
pulumi.runtime.set_config("bsky-apps:loaderImageTag", "abc1234")
pulumi.runtime.set_config("bsky-apps:ingestImageTag", "def5678")
pulumi.runtime.set_config("bsky-apps:silverImageTag", "fed4321")

_spec = importlib.util.spec_from_file_location("apps_stack", Path(__file__).with_name("__main__.py"))
apps = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(apps)


@pulumi.runtime.test
def test_loader_deployment():
    def check(spec):
        assert spec["replicas"] == 1
        assert spec["strategy"]["type"] == "Recreate"
        pod = spec["template"]["spec"]
        # Must match the platform stack's Pod Identity association.
        assert pod["service_account_name"] == "iceberg-loader"
        assert pod["termination_grace_period_seconds"] >= 60
        (container,) = pod["containers"]
        assert container["image"] == f"{REPO_URL}:abc1234"
        env = {e["name"]: e["value"] for e in container["env"]}
        assert env["BSKY_LOADER_QUEUE_URL"] == STACK_OUTPUTS["queue_url"]
        assert env["BSKY_ICEBERG_LOCATION"] == STACK_OUTPUTS["raw_table_location"]
        # No credentials: they come from Pod Identity.
        assert not any(k.startswith("AWS_ACCESS") or k.startswith("AWS_SECRET") for k in env)

    return apps.loader.spec.apply(check)


@pulumi.runtime.test
def test_loader_in_namespace():
    def check(args):
        deployment_ns, sa_ns = args
        assert deployment_ns == sa_ns == "bsky"

    return pulumi.Output.all(apps.loader.metadata.namespace, apps.loader_sa.metadata.namespace).apply(check)


@pulumi.runtime.test
def test_ingestor_deployment():
    def check(spec):
        assert spec["replicas"] == 1
        assert spec["strategy"]["type"] == "Recreate"
        pod = spec["template"]["spec"]
        assert pod["service_account_name"] == "ingestor"
        assert pod["termination_grace_period_seconds"] >= 60
        (container,) = pod["containers"]
        assert container["image"] == f"{INGEST_REPO_URL}:def5678"
        env = {e["name"]: e["value"] for e in container["env"]}
        assert env["BSKY_OUTPUT"] == STACK_OUTPUTS["raw_location"]
        # Batches must fill before the interval flushes them.
        assert int(env["BSKY_FLUSH_INTERVAL"]) >= 300
        assert not any(k.startswith("AWS_ACCESS") or k.startswith("AWS_SECRET") for k in env)
        assert container["security_context"]["run_as_user"] == 65532

    return apps.ingestor.spec.apply(check)


@pulumi.runtime.test
def test_fluent_bit_ships_workload_logs_to_data_stack_group():
    def check(args):
        conf, ns = args
        assert ns == "logging"
        assert "Path              /var/log/containers/*_bsky_*.log" in conf
        assert "log_group_name       /bsky/containers" in conf
        # The data stack owns the group (and its retention).
        assert "auto_create_group    Off" in conf

    return pulumi.Output.all(apps.fluent_bit_conf, apps.fluent_bit.metadata.namespace).apply(check)


@pulumi.runtime.test
def test_fluent_bit_daemonset():
    def check(spec):
        pod = spec["template"]["spec"]
        # Must match the platform stack's Pod Identity association.
        assert pod["service_account_name"] == "fluent-bit"
        assert spec["template"]["metadata"]["annotations"]["bsky/config-sha256"]
        (container,) = pod["containers"]
        sc = container["security_context"]
        assert sc["read_only_root_filesystem"] and sc["capabilities"]["drop"] == ["ALL"]
        mounts = {m["name"]: m for m in container["volume_mounts"]}
        assert mounts["varlog"]["read_only"]

    return apps.fluent_bit.spec.apply(check)


@pulumi.runtime.test
def test_silver_cronjob():
    def check(spec):
        assert spec["schedule"] == "5 * * * *"
        assert spec["concurrency_policy"] == "Forbid"
        job = spec["job_template"]["spec"]
        assert job["backoff_limit"] == 2
        assert job["active_deadline_seconds"] < 3600
        pod = job["template"]["spec"]
        # Must match the platform stack's Pod Identity association.
        assert pod["service_account_name"] == "silver"
        assert pod["restart_policy"] == "Never"
        (container,) = pod["containers"]
        assert container["image"] == f"{SILVER_REPO_URL}:fed4321"
        env = {e["name"]: e["value"] for e in container["env"]}
        assert env["BSKY_SILVER_LOCATION"] == STACK_OUTPUTS["silver_location"]
        assert env["BSKY_SILVER_STAGING_DIR"] == container["volume_mounts"][0]["mount_path"]
        assert not any(k.startswith("AWS_ACCESS") or k.startswith("AWS_SECRET") for k in env)

    return apps.silver.spec.apply(check)


@pulumi.runtime.test
def test_reports_service_account():
    def check(args):
        name, namespace = args
        # Must match the platform stack's Pod Identity association.
        assert (name, namespace) == ("reports", "bsky")

    return pulumi.Output.all(apps.reports_sa.metadata.name, apps.reports_sa.metadata.namespace).apply(check)

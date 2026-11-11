"""Checks the platform stack's resources using Pulumi mocks (no AWS calls)."""

import importlib.util
import json
import os
from collections import defaultdict
from pathlib import Path

import pulumi

ACCOUNT = "123456789012"
LOADER_ROLE_ARN = f"arn:aws:iam::{ACCOUNT}:role/bsky/bsky-iceberg-loader"
INGESTOR_ROLE_ARN = f"arn:aws:iam::{ACCOUNT}:role/bsky/bsky-ingestor"
FLUENT_BIT_ROLE_ARN = f"arn:aws:iam::{ACCOUNT}:role/bsky/bsky-fluent-bit"
SILVER_ROLE_ARN = f"arn:aws:iam::{ACCOUNT}:role/bsky/bsky-silver"
REPORTS_ROLE_ARN = f"arn:aws:iam::{ACCOUNT}:role/bsky/bsky-reports"
EMBED_ROLE_ARN = f"arn:aws:iam::{ACCOUNT}:role/bsky/bsky-embed"
BOUNDARY_ARN = f"arn:aws:iam::{ACCOUNT}:policy/bsky-workload-boundary"


class Mocks(pulumi.runtime.Mocks):
    def __init__(self):
        # Inputs of every resource registered, by type.
        self.resources: dict[str, list[dict]] = defaultdict(list)

    def new_resource(self, args: pulumi.runtime.MockResourceArgs):
        state = dict(args.inputs)
        name = args.inputs.get("name", args.name)
        if args.typ == "pulumi:pulumi:StackReference":
            return f"{args.name}-id", {
                "outputs": {
                    "loader_role_arn": LOADER_ROLE_ARN,
                    "ingestor_role_arn": INGESTOR_ROLE_ARN,
                    "fluent_bit_role_arn": FLUENT_BIT_ROLE_ARN,
                    "silver_role_arn": SILVER_ROLE_ARN,
                    "reports_role_arn": REPORTS_ROLE_ARN,
                    "embed_role_arn": EMBED_ROLE_ARN,
                }
            }
        self.resources[args.typ].append(state)
        if args.typ == "aws:iam/role:Role":
            state["arn"] = f"arn:aws:iam::{ACCOUNT}:role{args.inputs.get('path', '/')}{name}"
        elif args.typ == "aws:eks/cluster:Cluster":
            state["endpoint"] = "https://example.eks.amazonaws.com"
            state["certificateAuthority"] = {"data": "Y2E="}
        elif args.typ == "aws:ecr/repository:Repository":
            state["repositoryUrl"] = f"{ACCOUNT}.dkr.ecr.us-west-2.amazonaws.com/{name}"
        return f"{args.name}-id", state

    def call(self, args: pulumi.runtime.MockCallArgs):
        if args.token == "aws:index/getCallerIdentity:getCallerIdentity":
            return {"accountId": ACCOUNT, "arn": "arn", "id": "id", "userId": "user"}
        if args.token == "aws:index/getRegion:getRegion":
            return {"region": "us-west-2", "name": "us-west-2", "id": "us-west-2"}
        if args.token == "aws:index/getAvailabilityZones:getAvailabilityZones":
            return {"names": ["us-west-2a", "us-west-2b", "us-west-2c"], "zoneIds": [], "id": "us-west-2"}
        return {}


os.environ["BSKY_EKS_PUBLIC_CIDRS"] = "203.0.113.7/32"
mocks = Mocks()
pulumi.runtime.set_mocks(mocks, preview=False)

_spec = importlib.util.spec_from_file_location("platform_stack", Path(__file__).with_name("__main__.py"))
platform = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(platform)


@pulumi.runtime.test
def test_single_nat_gateway_for_private_subnets():
    def check(args):
        # Inside the check: resources are registered asynchronously.
        assert len(mocks.resources["aws:ec2/natGateway:NatGateway"]) == 1
        nat_id, routes = args
        (route,) = routes
        assert route["cidr_block"] == "0.0.0.0/0" and route["nat_gateway_id"] == nat_id

    return pulumi.Output.all(platform.nat.id, platform.private_rt.routes).apply(check)


@pulumi.runtime.test
def test_s3_gateway_endpoint_on_both_route_tables():
    def check(args):
        service, kind, table_ids, private_id, public_id = args
        assert service == "com.amazonaws.us-west-2.s3"
        assert kind == "Gateway"
        assert sorted(table_ids) == sorted([private_id, public_id])

    e = platform.s3_endpoint
    return pulumi.Output.all(
        e.service_name, e.vpc_endpoint_type, e.route_table_ids, platform.private_rt.id, platform.public_rt.id
    ).apply(check)


@pulumi.runtime.test
def test_subnets_in_two_azs():
    def check(azs):
        assert azs == ["us-west-2a", "us-west-2b"]

    return pulumi.Output.all(*[s.availability_zone for s in platform.private_subnets]).apply(check)


@pulumi.runtime.test
def test_cluster_api_access():
    def check(args):
        vpc, subnet_ids = args
        assert vpc["endpoint_private_access"] is True
        assert vpc["endpoint_public_access"] is True
        assert vpc["public_access_cidrs"] == ["203.0.113.7/32"]
        assert sorted(vpc["subnet_ids"]) == sorted(subnet_ids)

    return pulumi.Output.all(platform.cluster.vpc_config, *[s.id for s in platform.private_subnets]).apply(
        lambda a: check((a[0], a[1:]))
    )


@pulumi.runtime.test
def test_cluster_auto_mode():
    def check(args):
        compute, bootstrap_addons, access, node_role_arn = args
        assert compute["enabled"] is True
        assert compute["node_pools"] == ["general-purpose", "system"]
        assert compute["node_role_arn"] == node_role_arn
        assert bootstrap_addons is False
        assert access["authentication_mode"] == "API"

    c = platform.cluster
    return pulumi.Output.all(
        c.compute_config, c.bootstrap_self_managed_addons, c.access_config, platform.node_role.arn
    ).apply(check)


@pulumi.runtime.test
def test_loader_pod_identity_uses_data_stack_role():
    def check(args):
        namespace, service_account, role_arn = args
        assert (namespace, service_account) == ("bsky", "iceberg-loader")
        assert role_arn == LOADER_ROLE_ARN

    a = platform.pod_identities["iceberg-loader"]
    return pulumi.Output.all(a.namespace, a.service_account, a.role_arn).apply(check)


@pulumi.runtime.test
def test_ingestor_pod_identity_uses_data_stack_role():
    def check(args):
        namespace, service_account, role_arn = args
        assert (namespace, service_account) == ("bsky", "ingestor")
        assert role_arn == INGESTOR_ROLE_ARN

    a = platform.pod_identities["ingestor"]
    return pulumi.Output.all(a.namespace, a.service_account, a.role_arn).apply(check)


@pulumi.runtime.test
def test_fluent_bit_pod_identity_uses_data_stack_role():
    def check(args):
        namespace, service_account, role_arn = args
        assert (namespace, service_account) == ("logging", "fluent-bit")
        assert role_arn == FLUENT_BIT_ROLE_ARN

    a = platform.pod_identities["fluent-bit"]
    return pulumi.Output.all(a.namespace, a.service_account, a.role_arn).apply(check)


@pulumi.runtime.test
def test_ecr_scans_and_expires():
    def check(args):
        scanning, policy = args
        assert scanning["scan_on_push"] is True
        rules = json.loads(policy)["rules"]
        assert [r["selection"]["tagStatus"] for r in rules] == ["untagged", "tagged"]

    repo = platform.repositories["iceberg-loader"]
    assert sorted(platform.repositories) == ["embed", "iceberg-loader", "ingest", "reports", "silver"]
    lifecycle = mocks.resources["aws:ecr/lifecyclePolicy:LifecyclePolicy"][0]
    return pulumi.Output.all(repo.image_scanning_configuration, lifecycle["policy"]).apply(check)


def test_every_role_in_delegated_scope():
    # Runs after the resource tests above, by which point every role is registered.
    roles = mocks.resources["aws:iam/role:Role"]
    assert {r["name"] for r in roles} == {"bsky-eks-cluster", "bsky-eks-node"}
    for r in roles:
        assert r.get("path") == "/bsky/", r
        assert r.get("permissionsBoundary") == BOUNDARY_ARN, r
    assert not mocks.resources["aws:iam/policy:Policy"]


@pulumi.runtime.test
def test_silver_pod_identity_uses_data_stack_role():
    def check(args):
        namespace, service_account, role_arn = args
        assert (namespace, service_account) == ("bsky", "silver")
        assert role_arn == SILVER_ROLE_ARN

    a = platform.pod_identities["silver"]
    return pulumi.Output.all(a.namespace, a.service_account, a.role_arn).apply(check)


@pulumi.runtime.test
def test_embed_pod_identity_uses_data_stack_role():
    def check(args):
        namespace, service_account, role_arn = args
        assert (namespace, service_account) == ("bsky", "embed")
        assert role_arn == EMBED_ROLE_ARN

    a = platform.pod_identities["embed"]
    return pulumi.Output.all(a.namespace, a.service_account, a.role_arn).apply(check)


@pulumi.runtime.test
def test_reports_pod_identity_uses_data_stack_role():
    def check(args):
        namespace, service_account, role_arn = args
        assert (namespace, service_account) == ("bsky", "reports")
        assert role_arn == REPORTS_ROLE_ARN

    a = platform.pod_identities["reports"]
    return pulumi.Output.all(a.namespace, a.service_account, a.role_arn).apply(check)

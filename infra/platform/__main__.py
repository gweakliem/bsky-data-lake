"""Shared platform for the data lake's workloads: VPC, EKS Auto Mode and ECR.

IAM roles go through ../bsky_iam.py so they stay inside the delegated IAM scope
(path /bsky/, bsky-workload-boundary); the pulumi-eks component isn't used
because it creates its own roles outside that scope. Workload roles live in the
data stack (..), which this stack reads through a StackReference to associate
them with Kubernetes service accounts.
"""

import base64
import json
import os
import sys
from pathlib import Path

import pulumi
import pulumi_aws as aws
from dotenv import find_dotenv, load_dotenv

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
import bsky_iam  # noqa: E402

load_dotenv(find_dotenv(usecwd=True))

config = pulumi.Config()
cluster_name = config.get("clusterName") or "bsky"
namespace = config.get("namespace") or "bsky"
logging_namespace = config.get("loggingNamespace") or "logging"
# Who may reach the public API endpoint. Kept out of the committed stack config
# because it's the admin's own address.
public_access_cidrs = config.get_object("publicAccessCidrs") or [
    c.strip() for c in os.environ.get("BSKY_EKS_PUBLIC_CIDRS", "").split(",") if c.strip()
]
if not public_access_cidrs:
    raise ValueError("set BSKY_EKS_PUBLIC_CIDRS in .env or `pulumi config set --path publicAccessCidrs[0] <cidr>`")

# Outbound internet for the private subnets: "gateway" (the managed NAT gateway) or
# "instance" (an fck-nat instance, WP M1: no per-GB charge, a few dollars a month).
nat_mode = config.get("natMode") or "gateway"
if nat_mode not in ("gateway", "instance"):
    raise ValueError(f"natMode must be gateway or instance, not {nat_mode!r}")
# Peak egress through the old gateway was 36 Mbit/s over a week (2026-11-09..15), p99 17,
# average 2.7: t4g.nano's 32 Mbit/s baseline plus burst credits covers it.
nat_instance_type = config.get("natInstanceType") or "t4g.nano"
FCK_NAT_AMI_OWNER = "568608671756"  # fck-nat's published AMIs (https://fck-nat.dev/)

data = pulumi.StackReference(config.get("dataStack") or f"organization/bsky-data-lake/{pulumi.get_stack()}")
region = aws.get_region_output().region
azs = aws.get_availability_zones_output(state="available").names



def _trust_ec2() -> str:
    return json.dumps({"Version": "2012-10-17", "Statement": [
        {"Effect": "Allow", "Principal": {"Service": "ec2.amazonaws.com"}, "Action": "sts:AssumeRole"}]})


# --- VPC: 2 AZs, one NAT (a managed gateway or an fck-nat instance; cost over HA for dev) ---

vpc = aws.ec2.Vpc(
    "bsky",
    cidr_block="10.20.0.0/16",
    enable_dns_hostnames=True,
    enable_dns_support=True,
    tags={"Name": "bsky"},
)
igw = aws.ec2.InternetGateway("bsky", vpc_id=vpc.id, tags={"Name": "bsky"})

public_rt = aws.ec2.RouteTable(
    "public",
    vpc_id=vpc.id,
    routes=[aws.ec2.RouteTableRouteArgs(cidr_block="0.0.0.0/0", gateway_id=igw.id)],
    tags={"Name": "bsky-public"},
)

public_subnets = []
private_subnets = []
for i, (public_cidr, private_cidr) in enumerate([("10.20.0.0/20", "10.20.128.0/18"), ("10.20.16.0/20", "10.20.192.0/18")]):
    az = azs.apply(lambda names, i=i: names[i])
    public = aws.ec2.Subnet(
        f"public-{i}",
        vpc_id=vpc.id,
        cidr_block=public_cidr,
        availability_zone=az,
        tags={"Name": f"bsky-public-{i}", "kubernetes.io/role/elb": "1"},
    )
    aws.ec2.RouteTableAssociation(f"public-{i}", subnet_id=public.id, route_table_id=public_rt.id)
    public_subnets.append(public)
    # Large private subnets: Auto Mode gives every pod a VPC address.
    private_subnets.append(
        aws.ec2.Subnet(
            f"private-{i}",
            vpc_id=vpc.id,
            cidr_block=private_cidr,
            availability_zone=az,
            tags={"Name": f"bsky-private-{i}", "kubernetes.io/role/internal-elb": "1"},
        )
    )

nat_eip = aws.ec2.Eip("nat", domain="vpc", tags={"Name": "bsky-nat"})
nat = None
if nat_mode == "gateway":
    nat = aws.ec2.NatGateway(
        "bsky",
        subnet_id=public_subnets[0].id,
        allocation_id=nat_eip.id,
        tags={"Name": "bsky"},
        opts=pulumi.ResourceOptions(depends_on=[igw]),
    )
    private_route = aws.ec2.RouteTableRouteArgs(cidr_block="0.0.0.0/0", nat_gateway_id=nat.id)
else:
    # One instance in an Auto Scaling group of one, so a failed instance is replaced.
    # fck-nat at boot (/etc/fck-nat.conf):
    #   eni_id: attaches the fixed interface, the private route's target, where
    #           traffic from the VPC comes in;
    #   eip_id: moves the Elastic IP onto the instance's own interface, which the
    #           traffic leaves through (masqueraded); it also turns off that
    #           interface's source/destination check.
    # So a replacement takes over without route or egress-address changes.
    # One zone, like the gateway was.
    nat_sg = aws.ec2.SecurityGroup(
        "nat",
        name="bsky-nat",
        description="fck-nat: forward anything from the VPC",
        vpc_id=vpc.id,
        ingress=[aws.ec2.SecurityGroupIngressArgs(protocol="-1", from_port=0, to_port=0, cidr_blocks=[vpc.cidr_block])],
        egress=[aws.ec2.SecurityGroupEgressArgs(protocol="-1", from_port=0, to_port=0, cidr_blocks=["0.0.0.0/0"])],
        tags={"Name": "bsky-nat"},
    )
    nat_eni = aws.ec2.NetworkInterface(
        "nat",
        subnet_id=public_subnets[0].id,
        security_groups=[nat_sg.id],
        source_dest_check=False,  # it forwards other hosts' traffic
        description="bsky NAT: target of the private route",  # EC2 rejects apostrophes here
        tags={"Name": "bsky-nat"},
    )
    nat_role = bsky_iam.role(
        "nat",
        name=f"{cluster_name}-nat",
        description="fck-nat instance: attach the NAT interface; SSM for a shell",  # changing it needs iam:UpdateRoleDescription
        assume_role_policy=_trust_ec2(),
    )
    aws.iam.RolePolicyAttachment(
        "nat-ssm", role=nat_role.name, policy_arn="arn:aws:iam::aws:policy/AmazonSSMManagedInstanceCore"
    )
    nat_eni_policy = aws.iam.RolePolicy(
        "nat-eni",
        role=nat_role.id,
        policy=pulumi.Output.all(nat_eni.arn, nat_eip.arn).apply(lambda a: json.dumps({
            "Version": "2012-10-17",
            "Statement": [
                {"Sid": "AttachTheNatInterface", "Effect": "Allow", "Action": "ec2:AttachNetworkInterface",
                 "Resource": [a[0], "arn:aws:ec2:*:*:instance/*"]},
                # Its own interface, whose ID it only learns at boot.
                {"Sid": "OwnInterfaceSourceDestCheck", "Effect": "Allow",
                 "Action": "ec2:ModifyNetworkInterfaceAttribute", "Resource": "arn:aws:ec2:*:*:network-interface/*"},
                {"Sid": "TakeTheElasticIp", "Effect": "Allow", "Action": "ec2:AssociateAddress",
                 "Resource": [a[1], "arn:aws:ec2:*:*:network-interface/*"]},
                {"Effect": "Allow", "Action": "ec2:DescribeNetworkInterfaces", "Resource": "*"},
            ],
        })),
    )
    nat_profile = bsky_iam.instance_profile("nat", name=f"{cluster_name}-nat", role=nat_role.name)
    nat_ami = aws.ec2.get_ami_output(
        most_recent=True,
        owners=[FCK_NAT_AMI_OWNER],
        filters=[
            aws.ec2.GetAmiFilterArgs(name="name", values=["fck-nat-al2023-*"]),
            aws.ec2.GetAmiFilterArgs(name="architecture", values=["arm64"]),
        ],
    )
    nat_template = aws.ec2.LaunchTemplate(
        "nat",
        name="bsky-nat",
        image_id=nat_ami.id,
        instance_type=nat_instance_type,
        iam_instance_profile=aws.ec2.LaunchTemplateIamInstanceProfileArgs(arn=nat_profile.arn),
        # A public address until fck-nat moves the Elastic IP onto this interface.
        network_interfaces=[aws.ec2.LaunchTemplateNetworkInterfaceArgs(
            device_index=0, associate_public_ip_address="true", security_groups=[nat_sg.id],
        )],
        metadata_options=aws.ec2.LaunchTemplateMetadataOptionsArgs(http_tokens="required", http_endpoint="enabled"),
        user_data=pulumi.Output.all(nat_eni.id, nat_eip.allocation_id).apply(lambda a: base64.b64encode(
            f"#!/bin/sh\n: > /etc/fck-nat.conf\necho 'eni_id={a[0]}' >> /etc/fck-nat.conf\n"
            f"echo 'eip_id={a[1]}' >> /etc/fck-nat.conf\nservice fck-nat restart\n".encode()
        ).decode()),
        tag_specifications=[aws.ec2.LaunchTemplateTagSpecificationArgs(
            resource_type="instance", tags={"Name": "bsky-nat", "project": "bsky-data-lake", "stack": "platform"},
        )],
        update_default_version=True,
    )
    nat_asg = aws.autoscaling.Group(
        "nat",
        name="bsky-nat",
        min_size=1,
        max_size=1,
        desired_capacity=1,
        vpc_zone_identifiers=[public_subnets[0].id],  # the fixed interface's zone
        launch_template=aws.autoscaling.GroupLaunchTemplateArgs(id=nat_template.id, version="$Latest"),
        health_check_type="EC2",
        # A new AMI or instance type replaces the instance; the gap is a few minutes.
        instance_refresh=aws.autoscaling.GroupInstanceRefreshArgs(
            strategy="Rolling",
            preferences=aws.autoscaling.GroupInstanceRefreshPreferencesArgs(min_healthy_percentage=0),
        ),
    )
    private_route = aws.ec2.RouteTableRouteArgs(cidr_block="0.0.0.0/0", network_interface_id=nat_eni.id)
private_rt = aws.ec2.RouteTable(
    "private",
    vpc_id=vpc.id,
    routes=[private_route],
    tags={"Name": "bsky-private"},
)
for i, subnet in enumerate(private_subnets):
    aws.ec2.RouteTableAssociation(f"private-{i}", subnet_id=subnet.id, route_table_id=private_rt.id)

# Free, and keeps lake traffic (the bulk of it) off the NAT gateway.
s3_endpoint = aws.ec2.VpcEndpoint(
    "s3",
    vpc_id=vpc.id,
    service_name=region.apply(lambda r: f"com.amazonaws.{r}.s3"),
    vpc_endpoint_type="Gateway",
    route_table_ids=[private_rt.id, public_rt.id],
    tags={"Name": "bsky-s3"},
)

# --- IAM: cluster and node roles for EKS Auto Mode ---


def _trust(service: str, *actions: str) -> str:
    return json.dumps(
        {
            "Version": "2012-10-17",
            "Statement": [
                {"Effect": "Allow", "Principal": {"Service": service}, "Action": ["sts:AssumeRole", *actions]}
            ],
        }
    )


def _attach(role: aws.iam.Role, prefix: str, policies: list[str]) -> list[aws.iam.RolePolicyAttachment]:
    return [
        aws.iam.RolePolicyAttachment(f"{prefix}-{p}", role=role.name, policy_arn=f"arn:aws:iam::aws:policy/{p}")
        for p in policies
    ]


cluster_role = bsky_iam.role(
    "eks-cluster",
    name=f"{cluster_name}-eks-cluster",
    description="EKS Auto Mode cluster role",
    assume_role_policy=_trust("eks.amazonaws.com", "sts:TagSession"),
)
cluster_attachments = _attach(
    cluster_role,
    "eks-cluster",
    [
        "AmazonEKSClusterPolicy",
        "AmazonEKSComputePolicy",
        "AmazonEKSBlockStoragePolicy",
        "AmazonEKSLoadBalancingPolicy",
        "AmazonEKSNetworkingPolicy",
    ],
)

node_role = bsky_iam.role(
    "eks-node",
    name=f"{cluster_name}-eks-node",
    description="EKS Auto Mode node role",
    assume_role_policy=_trust("ec2.amazonaws.com"),
)
node_attachments = _attach(node_role, "eks-node", ["AmazonEKSWorkerNodeMinimalPolicy", "AmazonEC2ContainerRegistryPullOnly"])

# --- EKS cluster (Auto Mode) ---

# Created up front so the control-plane logs get a retention period.
control_plane_logs = aws.cloudwatch.LogGroup(
    "eks-control-plane",
    name=f"/aws/eks/{cluster_name}/cluster",
    retention_in_days=7,
)

cluster = aws.eks.Cluster(
    "bsky",
    name=cluster_name,
    version=config.get("kubernetesVersion"),
    role_arn=cluster_role.arn,
    access_config=aws.eks.ClusterAccessConfigArgs(
        authentication_mode="API",
        # Whoever runs `pulumi up` (the BskyDataLakeDeveloper SSO role) gets cluster admin.
        bootstrap_cluster_creator_admin_permissions=True,
    ),
    bootstrap_self_managed_addons=False,
    compute_config=aws.eks.ClusterComputeConfigArgs(
        enabled=True,
        node_pools=["general-purpose", "system"],
        node_role_arn=node_role.arn,
    ),
    kubernetes_network_config=aws.eks.ClusterKubernetesNetworkConfigArgs(
        elastic_load_balancing=aws.eks.ClusterKubernetesNetworkConfigElasticLoadBalancingArgs(enabled=True),
    ),
    storage_config=aws.eks.ClusterStorageConfigArgs(
        block_storage=aws.eks.ClusterStorageConfigBlockStorageArgs(enabled=True),
    ),
    vpc_config=aws.eks.ClusterVpcConfigArgs(
        subnet_ids=[s.id for s in private_subnets],
        endpoint_private_access=True,
        endpoint_public_access=True,
        public_access_cidrs=public_access_cidrs,
    ),
    # api + audit sent ~1.8 GB/day to CloudWatch (~$26/mo, 2026-11). Off by
    # default; set clusterLogTypes (e.g. ["audit"]) to look into something.
    enabled_cluster_log_types=config.get_object("clusterLogTypes") or [],
    opts=pulumi.ResourceOptions(depends_on=[control_plane_logs, *cluster_attachments, *node_attachments]),
)

# --- ECR: one repository per image ---

ecr_lifecycle = json.dumps(
    {
        "rules": [
            {
                "rulePriority": 1,
                "description": "Expire untagged images after a week",
                "selection": {"tagStatus": "untagged", "countType": "sinceImagePushed", "countUnit": "days", "countNumber": 7},
                "action": {"type": "expire"},
            },
            # Tagged only: a multi-arch push is one tagged index plus several
            # untagged children, which ECR won't expire while the index exists.
            {
                "rulePriority": 2,
                "description": "Keep the last 20 tagged images",
                "selection": {
                    "tagStatus": "tagged",
                    "tagPatternList": ["*"],
                    "countType": "imageCountMoreThan",
                    "countNumber": 20,
                },
                "action": {"type": "expire"},
            },
        ]
    }
)

# WP M5: the one-off lake copy (migrate/), in the new account only.
migration = config.get_bool("migration", False)

repositories = {}
for image in ["iceberg-loader", "ingest", "silver", "reports", "embed", "search", "labels",
              *(["migrate"] if migration else [])]:
    repo = aws.ecr.Repository(
        image,
        name=f"bsky/{image}",
        image_scanning_configuration=aws.ecr.RepositoryImageScanningConfigurationArgs(scan_on_push=True),
        image_tag_mutability="MUTABLE",
    )
    aws.ecr.LifecyclePolicy(image, repository=repo.name, policy=ecr_lifecycle)
    repositories[image] = repo

# --- Pod Identity: service account -> role (roles from the data stack) ---

pod_identities = {
    "iceberg-loader": aws.eks.PodIdentityAssociation(
        "iceberg-loader",
        cluster_name=cluster.name,
        namespace=namespace,
        service_account="iceberg-loader",
        role_arn=data.require_output("loader_role_arn"),
    ),
    "ingestor": aws.eks.PodIdentityAssociation(
        "ingestor",
        cluster_name=cluster.name,
        namespace=namespace,
        service_account="ingestor",
        role_arn=data.require_output("ingestor_role_arn"),
    ),
    "silver": aws.eks.PodIdentityAssociation(
        "silver",
        cluster_name=cluster.name,
        namespace=namespace,
        service_account="silver",
        role_arn=data.require_output("silver_role_arn"),
    ),
    "reports": aws.eks.PodIdentityAssociation(
        "reports",
        cluster_name=cluster.name,
        namespace=namespace,
        service_account="reports",
        role_arn=data.require_output("reports_role_arn"),
    ),
    "embed": aws.eks.PodIdentityAssociation(
        "embed",
        cluster_name=cluster.name,
        namespace=namespace,
        service_account="embed",
        role_arn=data.require_output("embed_role_arn"),
    ),
    "labels": aws.eks.PodIdentityAssociation(
        "labels",
        cluster_name=cluster.name,
        namespace=namespace,
        service_account="labels",
        role_arn=data.require_output("labels_role_arn"),
    ),
    # One image (bsky/search) for both: the index CronJob writes, the API reads.
    "index": aws.eks.PodIdentityAssociation(
        "index",
        cluster_name=cluster.name,
        namespace=namespace,
        service_account="index",
        role_arn=data.require_output("index_role_arn"),
    ),
    "search": aws.eks.PodIdentityAssociation(
        "search",
        cluster_name=cluster.name,
        namespace=namespace,
        service_account="search",
        role_arn=data.require_output("search_role_arn"),
    ),
    # Cluster-wide log shipping, so it lives outside the workloads' namespace.
    "fluent-bit": aws.eks.PodIdentityAssociation(
        "fluent-bit",
        cluster_name=cluster.name,
        namespace=logging_namespace,
        service_account="fluent-bit",
        role_arn=data.require_output("fluent_bit_role_arn"),
    ),
}
if migration:
    pod_identities["migration"] = aws.eks.PodIdentityAssociation(
        "migration",
        cluster_name=cluster.name,
        namespace=namespace,
        service_account="migration",
        role_arn=data.require_output("migration_role_arn"),
    )

# --- Observability add-ons: pod and node metrics ---

# Live CPU and memory (`kubectl top`); no history.
metrics_server = aws.eks.Addon(
    "metrics-server",
    cluster_name=cluster.name,
    addon_name="metrics-server",
    addon_version=config.get("metricsServerVersion") or "v0.9.1-eksbuild.1",
)

# Container Insights (enhanced observability): pod and node metrics kept in
# CloudWatch, behind the data stack's pod alarms. Only the agent's metrics are
# wanted: our own Fluent Bit already ships container logs, and the rest
# (Application Signals tracing, GPU/Neuron monitors, the OTEL pipeline's
# exporters) is off. The agent's IAM role comes from the data stack.
CLOUDWATCH_OBSERVABILITY_CONFIG = {
    "containerLogs": {"enabled": False},
    "applicationSignals": {"enabled": False},
    "dcgmExporter": {"enabled": False},
    "neuronMonitor": {"enabled": False},
    "kubeStateMetrics": {"enabled": False},
    "nodeExporter": {"enabled": False},
}
cloudwatch_observability = aws.eks.Addon(
    "cloudwatch-observability",
    cluster_name=cluster.name,
    addon_name="amazon-cloudwatch-observability",
    addon_version=config.get("cloudwatchObservabilityVersion") or "v7.0.1-eksbuild.1",
    configuration_values=json.dumps(CLOUDWATCH_OBSERVABILITY_CONFIG),
    pod_identity_associations=[
        aws.eks.AddonPodIdentityAssociationArgs(
            service_account="cloudwatch-agent", role_arn=data.require_output("cloudwatch_agent_role_arn")
        )
    ],
)


def _kubeconfig(args: list[str]) -> str:
    name, endpoint, ca, region = args
    user = {
        "exec": {
            "apiVersion": "client.authentication.k8s.io/v1beta1",
            "command": "aws",
            "args": ["eks", "get-token", "--cluster-name", name, "--region", region],
        }
    }
    return json.dumps(
        {
            "apiVersion": "v1",
            "kind": "Config",
            "clusters": [{"name": name, "cluster": {"server": endpoint, "certificate-authority-data": ca}}],
            "users": [{"name": name, "user": user}],
            "contexts": [{"name": name, "context": {"cluster": name, "user": name}}],
            "current-context": name,
        }
    )


pulumi.export("cluster_name", cluster.name)
pulumi.export("namespace", namespace)
pulumi.export("logging_namespace", logging_namespace)
# Credentials come from `aws eks get-token` under the caller's AWS_PROFILE; no secrets in here.
pulumi.export(
    "kubeconfig",
    pulumi.Output.all(cluster.name, cluster.endpoint, cluster.certificate_authority.data, region).apply(_kubeconfig),
)
pulumi.export("vpc_id", vpc.id)
pulumi.export("private_subnet_ids", [s.id for s in private_subnets])
pulumi.export("repository_urls", {name: repo.repository_url for name, repo in repositories.items()})

"""Shared platform for the data lake's workloads: VPC, EKS Auto Mode and ECR.

IAM roles go through ../bsky_iam.py so they stay inside the delegated IAM scope
(path /bsky/, bsky-workload-boundary); the pulumi-eks component isn't used
because it creates its own roles outside that scope. Workload roles live in the
data stack (..), which this stack reads through a StackReference to associate
them with Kubernetes service accounts.
"""

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

data = pulumi.StackReference(config.get("dataStack") or f"organization/bsky-data-lake/{pulumi.get_stack()}")
region = aws.get_region_output().region
azs = aws.get_availability_zones_output(state="available").names

# --- VPC: 2 AZs, one NAT gateway (cost over HA for dev) ---

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
nat = aws.ec2.NatGateway(
    "bsky",
    subnet_id=public_subnets[0].id,
    allocation_id=nat_eip.id,
    tags={"Name": "bsky"},
    opts=pulumi.ResourceOptions(depends_on=[igw]),
)
private_rt = aws.ec2.RouteTable(
    "private",
    vpc_id=vpc.id,
    routes=[aws.ec2.RouteTableRouteArgs(cidr_block="0.0.0.0/0", nat_gateway_id=nat.id)],
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
    enabled_cluster_log_types=["api", "audit"],
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

repositories = {}
for image in ["iceberg-loader", "ingest", "silver"]:
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
    # Cluster-wide log shipping, so it lives outside the workloads' namespace.
    "fluent-bit": aws.eks.PodIdentityAssociation(
        "fluent-bit",
        cluster_name=cluster.name,
        namespace=logging_namespace,
        service_account="fluent-bit",
        role_arn=data.require_output("fluent_bit_role_arn"),
    ),
}


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

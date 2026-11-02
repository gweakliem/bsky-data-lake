"""Kubernetes workloads for the data lake, on the platform stack's EKS cluster.

AWS credentials come from EKS Pod Identity: the platform stack associates each
service account here with its role from the data stack, so nothing AWS-side is
configured in this project. Deploying a new image is
`pulumi config set loaderImageTag <tag>` (or `ingestImageTag`) + `pulumi up`;
rolling back is the same with the previous tag.
"""

import pulumi
import pulumi_kubernetes as k8s

config = pulumi.Config()
stack = pulumi.get_stack()
data = pulumi.StackReference(config.get("dataStack") or f"organization/bsky-data-lake/{stack}")
platform = pulumi.StackReference(config.get("platformStack") or f"organization/bsky-platform/{stack}")
region = config.get("region") or "us-west-2"
loader_image_tag = config.require("loaderImageTag")
ingest_image_tag = config.require("ingestImageTag")

provider = k8s.Provider("eks", kubeconfig=platform.require_output("kubeconfig"))
opts = pulumi.ResourceOptions(provider=provider)

namespace_name = platform.require_output("namespace")
namespace = k8s.core.v1.Namespace(
    "bsky",
    metadata=k8s.meta.v1.ObjectMetaArgs(name=namespace_name),
    opts=opts,
)
ns = namespace.metadata.name
repository_urls = platform.require_output("repository_urls")

pod_security = k8s.core.v1.PodSecurityContextArgs(
    run_as_non_root=True,
    seccomp_profile=k8s.core.v1.SeccompProfileArgs(type="RuntimeDefault"),
)
container_security = k8s.core.v1.SecurityContextArgs(
    allow_privilege_escalation=False,
    capabilities=k8s.core.v1.CapabilitiesArgs(drop=["ALL"]),
)

# --- Iceberg loader ---

loader_labels = {"app.kubernetes.io/name": "iceberg-loader", "app.kubernetes.io/part-of": "bsky-data-lake"}

# The name must match the platform stack's Pod Identity association.
loader_sa = k8s.core.v1.ServiceAccount(
    "iceberg-loader",
    metadata=k8s.meta.v1.ObjectMetaArgs(name="iceberg-loader", namespace=ns),
    opts=opts,
)

loader_image = repository_urls.apply(lambda urls: f"{urls['iceberg-loader']}:{loader_image_tag}")

loader = k8s.apps.v1.Deployment(
    "iceberg-loader",
    metadata=k8s.meta.v1.ObjectMetaArgs(name="iceberg-loader", namespace=ns, labels=loader_labels),
    spec=k8s.apps.v1.DeploymentSpecArgs(
        replicas=1,
        # Two loaders would only race on Iceberg commits; SQS redelivery makes a
        # short gap during a deploy harmless.
        strategy=k8s.apps.v1.DeploymentStrategyArgs(type="Recreate"),
        selector=k8s.meta.v1.LabelSelectorArgs(match_labels=loader_labels),
        template=k8s.core.v1.PodTemplateSpecArgs(
            metadata=k8s.meta.v1.ObjectMetaArgs(labels=loader_labels),
            spec=k8s.core.v1.PodSpecArgs(
                service_account_name=loader_sa.metadata.name,
                # On SIGTERM the loader finishes its current commit (~35 s for
                # 128 MB) and releases the rest of the batch to the queue.
                termination_grace_period_seconds=120,
                security_context=pod_security,
                containers=[
                    k8s.core.v1.ContainerArgs(
                        name="loader",
                        image=loader_image,
                        env=[
                            k8s.core.v1.EnvVarArgs(name="BSKY_LOADER_QUEUE_URL", value=data.require_output("queue_url")),
                            k8s.core.v1.EnvVarArgs(
                                name="BSKY_ICEBERG_LOCATION", value=data.require_output("raw_table_location")
                            ),
                            k8s.core.v1.EnvVarArgs(name="AWS_REGION", value=region),
                            k8s.core.v1.EnvVarArgs(name="LOG_LEVEL", value=config.get("loaderLogLevel") or "INFO"),
                        ],
                        resources=k8s.core.v1.ResourceRequirementsArgs(
                            requests={"cpu": "500m", "memory": "1Gi"},
                            limits={"memory": "2Gi"},
                        ),
                        security_context=container_security,
                    )
                ],
            ),
        ),
    ),
    opts=opts,
)

# --- Jetstream ingestor ---

ingestor_labels = {"app.kubernetes.io/name": "ingestor", "app.kubernetes.io/part-of": "bsky-data-lake"}

# The name must match the platform stack's Pod Identity association.
ingestor_sa = k8s.core.v1.ServiceAccount(
    "ingestor",
    metadata=k8s.meta.v1.ObjectMetaArgs(name="ingestor", namespace=ns),
    opts=opts,
)

ingest_image = repository_urls.apply(lambda urls: f"{urls['ingest']}:{ingest_image_tag}")

# Large batches make fewer, bigger raw files (~15 MB at 100k messages, about
# every 3.5 min). The flush interval sits above the fill time so it doesn't cut
# batches short. What's buffered when the pod dies is replayed on restart: the
# ingestor resumes from the newest file it wrote.
ingest_batch_size = config.get_int("ingestBatchSize") or 100_000
ingest_flush_interval = config.get_int("ingestFlushInterval") or 300

ingestor = k8s.apps.v1.Deployment(
    "ingestor",
    metadata=k8s.meta.v1.ObjectMetaArgs(name="ingestor", namespace=ns, labels=ingestor_labels),
    spec=k8s.apps.v1.DeploymentSpecArgs(
        replicas=1,
        # Two ingestors would write every event twice.
        strategy=k8s.apps.v1.DeploymentStrategyArgs(type="Recreate"),
        selector=k8s.meta.v1.LabelSelectorArgs(match_labels=ingestor_labels),
        template=k8s.core.v1.PodTemplateSpecArgs(
            metadata=k8s.meta.v1.ObjectMetaArgs(labels=ingestor_labels),
            spec=k8s.core.v1.PodSpecArgs(
                service_account_name=ingestor_sa.metadata.name,
                # On SIGTERM it encodes and uploads what's buffered: one batch.
                termination_grace_period_seconds=60,
                security_context=pod_security,
                containers=[
                    k8s.core.v1.ContainerArgs(
                        name="ingestor",
                        image=ingest_image,
                        env=[
                            k8s.core.v1.EnvVarArgs(name="BSKY_OUTPUT", value=data.require_output("raw_location")),
                            k8s.core.v1.EnvVarArgs(name="BSKY_BATCH_SIZE", value=str(ingest_batch_size)),
                            k8s.core.v1.EnvVarArgs(name="BSKY_FLUSH_INTERVAL", value=str(ingest_flush_interval)),
                            k8s.core.v1.EnvVarArgs(name="AWS_REGION", value=region),
                            k8s.core.v1.EnvVarArgs(name="RUST_LOG", value=config.get("ingestLogLevel") or "info"),
                        ],
                        # A batch is held as JSON strings and copied once into Arrow to encode.
                        resources=k8s.core.v1.ResourceRequirementsArgs(
                            requests={"cpu": "250m", "memory": "512Mi"},
                            limits={"memory": "1Gi"},
                        ),
                        security_context=k8s.core.v1.SecurityContextArgs(
                            allow_privilege_escalation=False,
                            capabilities=k8s.core.v1.CapabilitiesArgs(drop=["ALL"]),
                            # distroless's nonroot user, numeric so runAsNonRoot can verify it.
                            run_as_user=65532,
                        ),
                    )
                ],
            ),
        ),
    ),
    opts=opts,
)

pulumi.export("namespace", ns)
pulumi.export("loader_image", loader_image)
pulumi.export("ingest_image", ingest_image)

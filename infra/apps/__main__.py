"""Kubernetes workloads for the data lake, on the platform stack's EKS cluster.

AWS credentials come from EKS Pod Identity: the platform stack associates each
service account here with its role from the data stack, so nothing AWS-side is
configured in this project. Deploying a new image is
`pulumi config set loaderImageTag <tag>` (or `ingestImageTag`) + `pulumi up`;
rolling back is the same with the previous tag.
"""

import hashlib

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

# --- Silver job: hourly, processes new bronze rows into the silver tables ---

silver_labels = {"app.kubernetes.io/name": "silver", "app.kubernetes.io/part-of": "bsky-data-lake"}
silver_image_tag = config.require("silverImageTag")
# Memory grows with the rows in one append, about 1 GB + 2 GB per million (see
# silver/CLAUDE.md); appends are split at the row cap, so the two are set together.
silver_max_rows = config.get_int("silverMaxRows") or 500_000
silver_memory_limit = config.get("silverMemoryLimit") or "3Gi"

# The name must match the platform stack's Pod Identity association.
silver_sa = k8s.core.v1.ServiceAccount(
    "silver",
    metadata=k8s.meta.v1.ObjectMetaArgs(name="silver", namespace=ns),
    opts=opts,
)

# The report build's and web pod's identity (bsky-reports via Pod Identity).
reports_sa = k8s.core.v1.ServiceAccount(
    "reports",
    metadata=k8s.meta.v1.ObjectMetaArgs(name="reports", namespace=ns),
    opts=opts,
)

silver_image = repository_urls.apply(lambda urls: f"{urls['silver']}:{silver_image_tag}")

silver = k8s.batch.v1.CronJob(
    "silver",
    metadata=k8s.meta.v1.ObjectMetaArgs(name="silver", namespace=ns, labels=silver_labels),
    spec=k8s.batch.v1.CronJobSpecArgs(
        # :05, after the loader's commits for the previous hour have settled.
        schedule="5 * * * *",
        time_zone="Etc/UTC",
        # A run works until caught up, so a slow one (a backlog) just delays
        # the next instead of racing it on the same window.
        concurrency_policy="Forbid",
        successful_jobs_history_limit=3,
        failed_jobs_history_limit=3,
        job_template=k8s.batch.v1.JobTemplateSpecArgs(
            metadata=k8s.meta.v1.ObjectMetaArgs(labels=silver_labels),
            spec=k8s.batch.v1.JobSpecArgs(
                backoff_limit=2,
                # Under an hour, so a stuck run can't block the next schedule.
                active_deadline_seconds=55 * 60,
                template=k8s.core.v1.PodTemplateSpecArgs(
                    metadata=k8s.meta.v1.ObjectMetaArgs(
                        labels=silver_labels,
                        # Auto Mode's consolidation evicted a run mid-backfill ("Underutilized",
                        # 2026-11-04), using up a retry. A run lasts under an hour, so it never
                        # pins a node for long.
                        annotations={"karpenter.sh/do-not-disrupt": "true"},
                    ),
                    spec=k8s.core.v1.PodSpecArgs(
                        service_account_name=silver_sa.metadata.name,
                        restart_policy="Never",
                        security_context=pod_security,
                        containers=[
                            k8s.core.v1.ContainerArgs(
                                name="silver",
                                image=silver_image,
                                env=[
                                    k8s.core.v1.EnvVarArgs(
                                        name="BSKY_SILVER_LOCATION", value=data.require_output("silver_location")
                                    ),
                                    k8s.core.v1.EnvVarArgs(name="BSKY_SILVER_STAGING_DIR", value="/staging"),
                                    k8s.core.v1.EnvVarArgs(name="AWS_REGION", value=region),
                                    k8s.core.v1.EnvVarArgs(name="LOG_LEVEL", value=config.get("silverLogLevel") or "INFO"),
                                ],
                                args=["--max-rows", str(silver_max_rows)],
                                resources=k8s.core.v1.ResourceRequirementsArgs(
                                    requests={"cpu": "500m", "memory": silver_memory_limit},
                                    limits={"memory": silver_memory_limit},
                                ),
                                volume_mounts=[k8s.core.v1.VolumeMountArgs(name="staging", mount_path="/staging")],
                                security_context=container_security,
                            )
                        ],
                        # One window's parsed rows; removed after each window.
                        volumes=[
                            k8s.core.v1.VolumeArgs(
                                name="staging", empty_dir=k8s.core.v1.EmptyDirVolumeSourceArgs(size_limit="4Gi")
                            )
                        ],
                    ),
                ),
            ),
        ),
    ),
    opts=opts,
)

# --- Embed job: hourly after silver, embeds new posts into post_embeddings (embed/) ---

embed_labels = {"app.kubernetes.io/name": "embed", "app.kubernetes.io/part-of": "bsky-data-lake"}
embed_image_tag = config.require("embedImageTag")
embed_image = repository_urls.apply(lambda urls: f"{urls['embed']}:{embed_image_tag}")
# Memory is mostly one silver window's posts: about 0.9 GB for the largest (backlog)
# windows, 1.2-2.0 GB with a chunk of vectors in flight (measured 2026-11-11).
embed_memory_limit = config.get("embedMemoryLimit") or "2560Mi"
embed_concurrency = config.get_int("embedConcurrency") or 60
embed_chunk_posts = config.get_int("embedChunkPosts") or 20_000
# Suspended while a backfill runs elsewhere: two writers would embed the same
# posts twice (no corruption, the views dedupe, but double the API spend).
embed_suspend = config.get_bool("embedSuspend", True)

# The name must match the platform stack's Pod Identity association.
embed_sa = k8s.core.v1.ServiceAccount(
    "embed",
    metadata=k8s.meta.v1.ObjectMetaArgs(name="embed", namespace=ns),
    opts=opts,
)

# The production OpenRouter key: `pulumi config set --secret openrouterApiKey`, so the
# stack file holds only ciphertext. Rotating it is the same command plus `pulumi up`;
# each run reads the Secret when its pod starts.
embed_key = k8s.core.v1.Secret(
    "embed-openrouter",
    metadata=k8s.meta.v1.ObjectMetaArgs(name="bsky-embed-openrouter", namespace=ns, labels=embed_labels),
    type="Opaque",
    string_data={"OPENROUTER_API_KEY": config.require_secret("openrouterApiKey")},
    opts=opts,
)

embed = k8s.batch.v1.CronJob(
    "embed",
    metadata=k8s.meta.v1.ObjectMetaArgs(name="embed", namespace=ns, labels=embed_labels),
    spec=k8s.batch.v1.CronJobSpecArgs(
        # :20, after silver's :05 run has committed the previous hour's windows.
        schedule="20 * * * *",
        time_zone="Etc/UTC",
        suspend=embed_suspend,
        concurrency_policy="Forbid",
        successful_jobs_history_limit=3,
        failed_jobs_history_limit=3,
        job_template=k8s.batch.v1.JobTemplateSpecArgs(
            metadata=k8s.meta.v1.ObjectMetaArgs(labels=embed_labels),
            spec=k8s.batch.v1.JobSpecArgs(
                backoff_limit=2,
                # Under an hour, so a stuck run can't block the next schedule. A run
                # cut short resumes from its last committed chunk.
                active_deadline_seconds=55 * 60,
                template=k8s.core.v1.PodTemplateSpecArgs(
                    metadata=k8s.meta.v1.ObjectMetaArgs(
                        labels=embed_labels,
                        annotations={"karpenter.sh/do-not-disrupt": "true"},
                    ),
                    spec=k8s.core.v1.PodSpecArgs(
                        service_account_name=embed_sa.metadata.name,
                        restart_policy="Never",
                        security_context=pod_security,
                        containers=[
                            k8s.core.v1.ContainerArgs(
                                name="embed",
                                image=embed_image,
                                env=[
                                    k8s.core.v1.EnvVarArgs(
                                        name="BSKY_SILVER_LOCATION", value=data.require_output("silver_location")
                                    ),
                                    k8s.core.v1.EnvVarArgs(name="AWS_REGION", value=region),
                                    k8s.core.v1.EnvVarArgs(
                                        name="OPENROUTER_API_KEY",
                                        value_from=k8s.core.v1.EnvVarSourceArgs(
                                            secret_key_ref=k8s.core.v1.SecretKeySelectorArgs(
                                                name=embed_key.metadata.name, key="OPENROUTER_API_KEY"
                                            )
                                        ),
                                    ),
                                ],
                                args=["--concurrency", str(embed_concurrency), "--chunk-posts", str(embed_chunk_posts)],
                                resources=k8s.core.v1.ResourceRequirementsArgs(
                                    requests={"cpu": "250m", "memory": embed_memory_limit},
                                    limits={"memory": embed_memory_limit},
                                ),
                                security_context=container_security,
                            )
                        ],
                    ),
                ),
            ),
        ),
    ),
    opts=opts,
)

# --- Reports: hourly static-site build, served by nginx (see reports/CLAUDE.md) ---

reports_labels = {"app.kubernetes.io/name": "reports", "app.kubernetes.io/part-of": "bsky-data-lake"}
reports_web_labels = {"app.kubernetes.io/name": "reports-web", "app.kubernetes.io/part-of": "bsky-data-lake"}
reports_image_tag = config.require("reportsImageTag")
reports_image = repository_urls.apply(lambda urls: f"{urls['reports']}:{reports_image_tag}")
reports_site_url = data.require_output("reports_site_url")
# DuckDB sizes itself from the node's memory, so its limit is set below the pod's.
# posts_current's window over 14 days of posts (about 50M rows) is the largest
# operator: languages ran out of memory at 4GB and fits in 6GB (2026-11-06).
reports_memory_limit = config.get("reportsMemoryLimit") or "8Gi"
reports_duckdb_memory = config.get("reportsDuckdbMemory") or "6GB"
reports_duckdb_threads = config.get("reportsDuckdbThreads") or "4"
nginx_image = config.get("nginxImage") or "nginxinc/nginx-unprivileged:1.29.3-alpine"
# The ATTACH names the catalog by account ID; the role ARN carries it.
account_id = data.require_output("reports_role_arn").apply(lambda arn: arn.split(":")[4])

reports_build = k8s.batch.v1.CronJob(
    "reports",
    metadata=k8s.meta.v1.ObjectMetaArgs(name="reports", namespace=ns, labels=reports_labels),
    spec=k8s.batch.v1.CronJobSpecArgs(
        # :30, after silver's :05 run has caught up (about 5 minutes when it isn't behind).
        schedule="30 * * * *",
        time_zone="Etc/UTC",
        concurrency_policy="Forbid",
        successful_jobs_history_limit=3,
        failed_jobs_history_limit=3,
        job_template=k8s.batch.v1.JobTemplateSpecArgs(
            metadata=k8s.meta.v1.ObjectMetaArgs(labels=reports_labels),
            spec=k8s.batch.v1.JobSpecArgs(
                backoff_limit=2,
                active_deadline_seconds=50 * 60,
                template=k8s.core.v1.PodTemplateSpecArgs(
                    metadata=k8s.meta.v1.ObjectMetaArgs(
                        labels=reports_labels, annotations={"karpenter.sh/do-not-disrupt": "true"}
                    ),
                    spec=k8s.core.v1.PodSpecArgs(
                        service_account_name=reports_sa.metadata.name,
                        restart_policy="Never",
                        security_context=pod_security,
                        containers=[
                            k8s.core.v1.ContainerArgs(
                                name="build",
                                image=reports_image,
                                args=[reports_site_url],
                                env=[
                                    k8s.core.v1.EnvVarArgs(name="AWS_REGION", value=region),
                                    k8s.core.v1.EnvVarArgs(name="BSKY_AWS_ACCOUNT_ID", value=account_id),
                                    k8s.core.v1.EnvVarArgs(name="BSKY_DUCKDB_MEMORY_LIMIT", value=reports_duckdb_memory),
                                    k8s.core.v1.EnvVarArgs(name="BSKY_DUCKDB_THREADS", value=reports_duckdb_threads),
                                    k8s.core.v1.EnvVarArgs(name="BSKY_DUCKDB_TEMP_DIR", value="/scratch/duckdb"),
                                ],
                                resources=k8s.core.v1.ResourceRequirementsArgs(
                                    requests={"cpu": "2", "memory": reports_memory_limit},
                                    limits={"memory": reports_memory_limit},
                                ),
                                volume_mounts=[k8s.core.v1.VolumeMountArgs(name="scratch", mount_path="/scratch")],
                                security_context=container_security,
                            )
                        ],
                        # DuckDB spills here when a query outgrows its memory limit.
                        volumes=[
                            k8s.core.v1.VolumeArgs(
                                name="scratch", empty_dir=k8s.core.v1.EmptyDirVolumeSourceArgs(size_limit="8Gi")
                            )
                        ],
                    ),
                ),
            ),
        ),
    ),
    opts=opts,
)

# Framework links pages without .html (/activity), hence try_files.
reports_nginx_conf = """server {
    listen 8080;
    root /site/current;
    location / {
        try_files $uri $uri.html $uri/ =404;
    }
}
"""
reports_nginx_config = k8s.core.v1.ConfigMap(
    "reports-web",
    metadata=k8s.meta.v1.ObjectMetaArgs(name="reports-web", namespace=ns, labels=reports_web_labels),
    data={"default.conf": reports_nginx_conf},
    opts=opts,
)

reports_web = k8s.apps.v1.Deployment(
    "reports-web",
    metadata=k8s.meta.v1.ObjectMetaArgs(name="reports-web", namespace=ns, labels=reports_web_labels),
    spec=k8s.apps.v1.DeploymentSpecArgs(
        replicas=1,
        selector=k8s.meta.v1.LabelSelectorArgs(match_labels=reports_web_labels),
        template=k8s.core.v1.PodTemplateSpecArgs(
            metadata=k8s.meta.v1.ObjectMetaArgs(
                labels=reports_web_labels,
                # Roll the pod when the nginx config changes.
                annotations={"bsky/nginx-conf-sha256": hashlib.sha256(reports_nginx_conf.encode()).hexdigest()},
            ),
            spec=k8s.core.v1.PodSpecArgs(
                service_account_name=reports_sa.metadata.name,
                security_context=pod_security,
                containers=[
                    k8s.core.v1.ContainerArgs(
                        name="nginx",
                        image=nginx_image,
                        ports=[k8s.core.v1.ContainerPortArgs(name="http", container_port=8080)],
                        resources=k8s.core.v1.ResourceRequirementsArgs(
                            requests={"cpu": "50m", "memory": "32Mi"}, limits={"memory": "64Mi"}
                        ),
                        volume_mounts=[
                            k8s.core.v1.VolumeMountArgs(name="site", mount_path="/site", read_only=True),
                            k8s.core.v1.VolumeMountArgs(name="nginx-conf", mount_path="/etc/nginx/conf.d", read_only=True),
                        ],
                        security_context=container_security,
                    ),
                    # Downloads each new published build and repoints /site/current at it.
                    k8s.core.v1.ContainerArgs(
                        name="sync",
                        image=reports_image,
                        command=["uv", "run", "--frozen", "--no-dev", "python", "lib/sync_site.py"],
                        args=[reports_site_url, "/site"],
                        env=[k8s.core.v1.EnvVarArgs(name="AWS_REGION", value=region)],
                        resources=k8s.core.v1.ResourceRequirementsArgs(
                            requests={"cpu": "25m", "memory": "128Mi"}, limits={"memory": "256Mi"}
                        ),
                        volume_mounts=[k8s.core.v1.VolumeMountArgs(name="site", mount_path="/site")],
                        security_context=container_security,
                    ),
                ],
                volumes=[
                    k8s.core.v1.VolumeArgs(name="site", empty_dir=k8s.core.v1.EmptyDirVolumeSourceArgs(size_limit="1Gi")),
                    k8s.core.v1.VolumeArgs(
                        name="nginx-conf",
                        config_map=k8s.core.v1.ConfigMapVolumeSourceArgs(name=reports_nginx_config.metadata.name),
                    ),
                ],
            ),
        ),
    ),
    opts=opts,
)

# ClusterIP only: viewed through `kubectl port-forward`, never exposed outside the cluster.
reports_service = k8s.core.v1.Service(
    "reports-web",
    metadata=k8s.meta.v1.ObjectMetaArgs(name="reports-web", namespace=ns, labels=reports_web_labels),
    spec=k8s.core.v1.ServiceSpecArgs(
        type="ClusterIP",
        selector=reports_web_labels,
        ports=[k8s.core.v1.ServicePortArgs(name="http", port=80, target_port="http")],
    ),
    opts=opts,
)

# --- Fluent Bit: container logs to CloudWatch ---

# One pod per node (a DaemonSet) tails the node's container log files and ships
# them to the data stack's log group. Only the shipping is AWS-specific: on
# another cloud, swap the [OUTPUT] section.
log_namespaces = config.get_object("logNamespaces") or ["bsky"]
fluent_bit_image = config.get("fluentBitImage") or "cr.fluentbit.io/fluent/fluent-bit:5.1.2"
logging_labels = {"app.kubernetes.io/name": "fluent-bit", "app.kubernetes.io/part-of": "bsky-data-lake"}

logging_namespace = k8s.core.v1.Namespace(
    "logging",
    metadata=k8s.meta.v1.ObjectMetaArgs(name=platform.require_output("logging_namespace")),
    opts=opts,
)
logging_ns = logging_namespace.metadata.name

# The name must match the platform stack's Pod Identity association.
fluent_bit_sa = k8s.core.v1.ServiceAccount(
    "fluent-bit",
    metadata=k8s.meta.v1.ObjectMetaArgs(name="fluent-bit", namespace=logging_ns),
    opts=opts,
)
# The kubernetes filter looks up each log line's pod to name its stream.
fluent_bit_role = k8s.rbac.v1.ClusterRole(
    "fluent-bit",
    metadata=k8s.meta.v1.ObjectMetaArgs(name="fluent-bit"),
    rules=[
        k8s.rbac.v1.PolicyRuleArgs(api_groups=[""], resources=["pods", "namespaces"], verbs=["get", "list", "watch"])
    ],
    opts=opts,
)
k8s.rbac.v1.ClusterRoleBinding(
    "fluent-bit",
    metadata=k8s.meta.v1.ObjectMetaArgs(name="fluent-bit"),
    role_ref=k8s.rbac.v1.RoleRefArgs(api_group="rbac.authorization.k8s.io", kind="ClusterRole", name=fluent_bit_role.metadata.name),
    subjects=[k8s.rbac.v1.SubjectArgs(kind="ServiceAccount", name=fluent_bit_sa.metadata.name, namespace=logging_ns)],
    opts=opts,
)


def _fluent_bit_conf(log_group: str) -> str:
    """The tail DB keeps read positions across restarts, so nothing is shipped
    twice or skipped. Each pod's container gets its own stream, holding just
    the log line (log_key), not the metadata Fluent Bit wraps around it."""
    # Container log files are named <pod>_<namespace>_<container>-<id>.log.
    paths = ",".join(f"/var/log/containers/*_{n}_*.log" for n in log_namespaces)
    return f"""\
[SERVICE]
    Flush         5
    Log_Level     info
    HTTP_Server   On
    HTTP_Listen   0.0.0.0
    HTTP_Port     2020
    Health_Check  On

[INPUT]
    Name              tail
    Tag               kube.*
    Path              {paths}
    multiline.parser  cri
    DB                /var/fluent-bit/state/tail.db
    Mem_Buf_Limit     5MB
    Skip_Long_Lines   On

[FILTER]
    Name         kubernetes
    Match        kube.*
    Labels       Off
    Annotations  Off

[OUTPUT]
    Name                 cloudwatch_logs
    Match                kube.*
    region               {region}
    log_group_name       {log_group}
    auto_create_group    Off
    log_stream_template  $kubernetes['pod_name'].$kubernetes['container_name']
    log_stream_prefix    unknown-
    log_key              log
"""


fluent_bit_conf = data.require_output("container_log_group").apply(_fluent_bit_conf)
fluent_bit_config = k8s.core.v1.ConfigMap(
    "fluent-bit",
    metadata=k8s.meta.v1.ObjectMetaArgs(name="fluent-bit", namespace=logging_ns),
    data={"fluent-bit.conf": fluent_bit_conf},
    opts=opts,
)

fluent_bit = k8s.apps.v1.DaemonSet(
    "fluent-bit",
    metadata=k8s.meta.v1.ObjectMetaArgs(name="fluent-bit", namespace=logging_ns, labels=logging_labels),
    spec=k8s.apps.v1.DaemonSetSpecArgs(
        selector=k8s.meta.v1.LabelSelectorArgs(match_labels=logging_labels),
        template=k8s.core.v1.PodTemplateSpecArgs(
            metadata=k8s.meta.v1.ObjectMetaArgs(
                labels=logging_labels,
                # Pods don't reload a changed ConfigMap; a new hash rolls them.
                annotations={"bsky/config-sha256": fluent_bit_conf.apply(lambda c: hashlib.sha256(c.encode()).hexdigest())},
            ),
            spec=k8s.core.v1.PodSpecArgs(
                service_account_name=fluent_bit_sa.metadata.name,
                # No tolerations: Auto Mode's tainted system nodes run only its
                # own add-ons, which don't log to the namespaces shipped here.
                containers=[
                    k8s.core.v1.ContainerArgs(
                        name="fluent-bit",
                        image=fluent_bit_image,
                        args=["-c", "/fluent-bit/etc/bsky/fluent-bit.conf"],
                        resources=k8s.core.v1.ResourceRequirementsArgs(
                            requests={"cpu": "50m", "memory": "64Mi"},
                            limits={"memory": "128Mi"},
                        ),
                        liveness_probe=k8s.core.v1.ProbeArgs(
                            http_get=k8s.core.v1.HTTPGetActionArgs(path="/api/v1/health", port=2020),
                            initial_delay_seconds=10,
                            period_seconds=30,
                        ),
                        volume_mounts=[
                            k8s.core.v1.VolumeMountArgs(name="config", mount_path="/fluent-bit/etc/bsky"),
                            k8s.core.v1.VolumeMountArgs(name="varlog", mount_path="/var/log", read_only=True),
                            k8s.core.v1.VolumeMountArgs(name="state", mount_path="/var/fluent-bit/state"),
                        ],
                        # Root to read the node's root-owned log files, but with
                        # no capabilities and a read-only filesystem.
                        security_context=k8s.core.v1.SecurityContextArgs(
                            run_as_user=0,
                            read_only_root_filesystem=True,
                            allow_privilege_escalation=False,
                            capabilities=k8s.core.v1.CapabilitiesArgs(drop=["ALL"]),
                        ),
                    )
                ],
                volumes=[
                    k8s.core.v1.VolumeArgs(
                        name="config",
                        config_map=k8s.core.v1.ConfigMapVolumeSourceArgs(name=fluent_bit_config.metadata.name),
                    ),
                    k8s.core.v1.VolumeArgs(name="varlog", host_path=k8s.core.v1.HostPathVolumeSourceArgs(path="/var/log")),
                    k8s.core.v1.VolumeArgs(
                        name="state",
                        host_path=k8s.core.v1.HostPathVolumeSourceArgs(
                            path="/var/fluent-bit/state", type="DirectoryOrCreate"
                        ),
                    ),
                ],
            ),
        ),
    ),
    opts=opts,
)

pulumi.export("namespace", ns)
pulumi.export("loader_image", loader_image)
pulumi.export("ingest_image", ingest_image)

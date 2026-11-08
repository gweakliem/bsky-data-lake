"""AWS resources for the Bluesky data lake.

The raw-file bucket and the warehouse bucket already exist and are not managed
here; this stack wires the raw bucket's ObjectCreated notifications to the
queue the Iceberg loader consumes and creates the Glue database the raw
Iceberg table lives in, located in the warehouse bucket. Athena's results
bucket is the one bucket it creates.
"""

import json
import os

import pulumi
import pulumi_aws as aws
from dotenv import find_dotenv, load_dotenv

import bsky_iam

load_dotenv(find_dotenv(usecwd=True))

config = pulumi.Config()
bucket = config.get("bucket") or os.environ.get("BSKY_BUCKET")
if not bucket:
    raise ValueError("set BSKY_BUCKET in .env or `pulumi config set bucket <name>`")
warehouse_bucket = config.get("warehouseBucket") or os.environ.get("BSKY_WAREHOUSE_BUCKET")
if not warehouse_bucket:
    raise ValueError("set BSKY_WAREHOUSE_BUCKET in .env or `pulumi config set warehouseBucket <name>`")
raw_prefix = (config.get("rawPrefix") or os.environ.get("BSKY_PREFIX") or "jetstream").strip("/") + "/"
glue_database = config.get("glueDatabase") or "bsky_raw"
silver_database_name = config.get("silverGlueDatabase") or "bsky_silver"
# Must exceed the loader's batch window plus the time to load a batch.
visibility_timeout = config.get_int("visibilityTimeout") or 600
max_receive_count = config.get_int("maxReceiveCount") or 5
manage_iam = config.get_bool("manageIam", True)
# Iceberg tables in the raw database that Glue's table optimizers maintain.
# The loader creates the tables, so add one here only once it exists.
optimized_tables = config.get_object("optimizedTables") or ["jetstream"]
# Likewise in the silver database: silver's TABLES plus its run log (silver/tables.py).
optimized_silver_tables = config.get_object("optimizedSilverTables") or [
    "posts", "likes", "reposts", "follows", "blocks", "profiles",
    "identities", "accounts", "deletes", "rejects", "silver_runs",
]
# Raw files are a replay source once loaded, so they expire.
raw_retention_days = config.get_int("rawRetentionDays") or 7
# Where alarms and budget alerts go. Kept out of committed config: it's personal.
alert_email = config.get("alertEmail") or os.environ.get("BSKY_ALERT_EMAIL")
if not alert_email:
    raise ValueError("set BSKY_ALERT_EMAIL in .env or `pulumi config set alertEmail <address>`")
budget_monthly_usd = config.get("budgetMonthlyUsd") or "250"
container_log_group = config.get("containerLogGroup") or "/bsky/containers"
container_log_retention_days = config.get_int("containerLogRetentionDays") or 14
athena_results_retention_days = config.get_int("athenaResultsRetentionDays") or 7

account_id = aws.get_caller_identity_output().account_id
region = aws.get_region_output().region
bucket_arn = f"arn:aws:s3:::{bucket}"
warehouse_bucket_arn = f"arn:aws:s3:::{warehouse_bucket}"
warehouse = f"s3://{warehouse_bucket}/iceberg"

# --- SQS: S3 notifications for raw files, with a dead-letter queue ---

dlq = aws.sqs.Queue(
    "raw-files-dlq",
    name="bsky-raw-files-dlq",
    message_retention_seconds=14 * 24 * 3600,
)

queue = aws.sqs.Queue(
    "raw-files",
    name="bsky-raw-files",
    visibility_timeout_seconds=visibility_timeout,
    receive_wait_time_seconds=20,
    redrive_policy=dlq.arn.apply(
        lambda arn: json.dumps({"deadLetterTargetArn": arn, "maxReceiveCount": max_receive_count})
    ),
)

queue_policy = aws.sqs.QueuePolicy(
    "raw-files",
    queue_url=queue.url,
    policy=pulumi.Output.all(queue.arn, account_id).apply(
        lambda args: json.dumps(
            {
                "Version": "2012-10-17",
                "Statement": [
                    {
                        "Effect": "Allow",
                        "Principal": {"Service": "s3.amazonaws.com"},
                        "Action": "sqs:SendMessage",
                        "Resource": args[0],
                        "Condition": {
                            "ArnLike": {"aws:SourceArn": bucket_arn},
                            "StringEquals": {"aws:SourceAccount": args[1]},
                        },
                    }
                ],
            }
        )
    ),
)

# This owns the bucket's entire notification configuration: any other
# notification targets on the bucket must be added here too.
notification = aws.s3.BucketNotification(
    "raw-files",
    bucket=bucket,
    queues=[
        aws.s3.BucketNotificationQueueArgs(
            queue_arn=queue.arn,
            events=["s3:ObjectCreated:*"],
            # Only the ingestor's raw files.
            filter_prefix=raw_prefix,
            filter_suffix=".parquet",
        )
    ],
    opts=pulumi.ResourceOptions(depends_on=[queue_policy]),
)

# Like the notification, this owns the bucket's entire lifecycle configuration.
raw_lifecycle = aws.s3.BucketLifecycleConfiguration(
    "raw-files",
    bucket=bucket,
    rules=[
        aws.s3.BucketLifecycleConfigurationRuleArgs(
            id=f"expire-{prefix.strip('/')}",
            status="Enabled",
            filter=aws.s3.BucketLifecycleConfigurationRuleFilterArgs(prefix=prefix),
            expiration=aws.s3.BucketLifecycleConfigurationRuleExpirationArgs(days=raw_retention_days),
        )
        # jetstream-test/ is the scratch prefix for manual runs.
        for prefix in dict.fromkeys([raw_prefix, "jetstream-test/"])
    ]
    + [
        aws.s3.BucketLifecycleConfigurationRuleArgs(
            id="abort-incomplete-uploads",
            status="Enabled",
            filter=aws.s3.BucketLifecycleConfigurationRuleFilterArgs(prefix=""),
            abort_incomplete_multipart_upload=aws.s3.BucketLifecycleConfigurationRuleAbortIncompleteMultipartUploadArgs(
                days_after_initiation=1
            ),
        )
    ],
)

# --- Athena: a workgroup for ad hoc queries, with its own results bucket ---

athena_results = aws.s3.Bucket(
    "athena-results",
    bucket=pulumi.Output.concat("bsky-athena-results-", account_id, "-", region, "-an"),
    bucket_namespace="account-regional",
    # Query results are disposable.
    force_destroy=True,
)
aws.s3.BucketPublicAccessBlock(
    "athena-results",
    bucket=athena_results.bucket,
    block_public_acls=True,
    block_public_policy=True,
    ignore_public_acls=True,
    restrict_public_buckets=True,
)
athena_results_lifecycle = aws.s3.BucketLifecycleConfiguration(
    "athena-results",
    bucket=athena_results.bucket,
    rules=[
        aws.s3.BucketLifecycleConfigurationRuleArgs(
            id="expire-results",
            status="Enabled",
            filter=aws.s3.BucketLifecycleConfigurationRuleFilterArgs(prefix=""),
            expiration=aws.s3.BucketLifecycleConfigurationRuleExpirationArgs(days=athena_results_retention_days),
            abort_incomplete_multipart_upload=aws.s3.BucketLifecycleConfigurationRuleAbortIncompleteMultipartUploadArgs(
                days_after_initiation=1
            ),
        )
    ],
)
# Enforced, so every query in it writes to the bucket above whatever the
# client's own settings say.
athena_workgroup = aws.athena.Workgroup(
    "bsky",
    name="bsky",
    description="Ad hoc queries over the Bluesky data lake",
    force_destroy=True,
    configuration=aws.athena.WorkgroupConfigurationArgs(
        enforce_workgroup_configuration=True,
        engine_version=aws.athena.WorkgroupConfigurationEngineVersionArgs(
            selected_engine_version="Athena engine version 3"
        ),
        result_configuration=aws.athena.WorkgroupConfigurationResultConfigurationArgs(
            output_location=pulumi.Output.concat("s3://", athena_results.bucket, "/"),
        ),
    ),
)

# --- Glue: database for the raw (bronze) Iceberg tables ---

database = aws.glue.CatalogDatabase(
    "raw",
    name=glue_database,
    description="Raw (bronze) layer of the Bluesky data lake",
    location_uri=f"{warehouse}/{glue_database}",
)

# The silver job creates its own tables here (see silver/).
silver_database = aws.glue.CatalogDatabase(
    "silver",
    name=silver_database_name,
    description="Silver layer of the Bluesky data lake: one table per record type",
    location_uri=f"{warehouse}/{silver_database_name}",
)

# --- IAM: permissions the loader (and its Sail server) need ---

loader_policy_document = pulumi.Output.all(queue.arn, account_id, region).apply(
    lambda args: json.dumps(
        {
            "Version": "2012-10-17",
            "Statement": [
                {
                    "Sid": "ConsumeQueue",
                    "Effect": "Allow",
                    # ChangeMessageVisibility releases unprocessed messages on shutdown.
                    "Action": [
                        "sqs:ReceiveMessage",
                        "sqs:DeleteMessage",
                        "sqs:ChangeMessageVisibility",
                        "sqs:GetQueueAttributes",
                    ],
                    "Resource": args[0],
                },
                {
                    "Sid": "ListBuckets",
                    "Effect": "Allow",
                    "Action": "s3:ListBucket",
                    "Resource": [bucket_arn, warehouse_bucket_arn],
                },
                {
                    "Sid": "ReadRawFiles",
                    "Effect": "Allow",
                    "Action": "s3:GetObject",
                    "Resource": f"{bucket_arn}/{raw_prefix}*",
                },
                {
                    "Sid": "WriteIcebergTables",
                    "Effect": "Allow",
                    "Action": ["s3:GetObject", "s3:PutObject", "s3:DeleteObject"],
                    "Resource": f"{warehouse_bucket_arn}/iceberg/*",
                },
                {
                    "Sid": "GlueCatalog",
                    "Effect": "Allow",
                    "Action": [
                        "glue:GetDatabase",
                        "glue:GetTable",
                        "glue:GetTables",
                        "glue:CreateTable",
                        "glue:UpdateTable",
                    ],
                    "Resource": [
                        f"arn:aws:glue:{args[2]}:{args[1]}:catalog",
                        f"arn:aws:glue:{args[2]}:{args[1]}:database/{glue_database}",
                        f"arn:aws:glue:{args[2]}:{args[1]}:table/{glue_database}/*",
                    ],
                },
            ],
        }
    )
)

# --- IAM: the ingestor writes raw files, and lists and reads them to resume ---

ingestor_policy_document = json.dumps(
    {
        "Version": "2012-10-17",
        "Statement": [
            {
                "Sid": "ListRawPrefix",
                "Effect": "Allow",
                "Action": "s3:ListBucket",
                "Resource": bucket_arn,
                "Condition": {"StringLike": {"s3:prefix": [f"{raw_prefix}*"]}},
            },
            {
                "Sid": "ReadWriteRawFiles",
                "Effect": "Allow",
                "Action": ["s3:GetObject", "s3:PutObject"],
                "Resource": f"{bucket_arn}/{raw_prefix}*",
            },
        ],
    }
)


table_optimizer_policy_document = pulumi.Output.all(account_id, region).apply(
    lambda args: json.dumps(
        {
            "Version": "2012-10-17",
            "Statement": [
                {
                    "Sid": "RewriteTableFiles",
                    "Effect": "Allow",
                    "Action": ["s3:GetObject", "s3:PutObject", "s3:DeleteObject"],
                    "Resource": f"{warehouse_bucket_arn}/iceberg/*",
                },
                {
                    "Sid": "ListTableFiles",
                    "Effect": "Allow",
                    "Action": "s3:ListBucket",
                    "Resource": warehouse_bucket_arn,
                    "Condition": {"StringLike": {"s3:prefix": ["iceberg/*"]}},
                },
                {
                    "Sid": "CommitToCatalog",
                    "Effect": "Allow",
                    "Action": ["glue:GetTable", "glue:UpdateTable"],
                    "Resource": [
                        f"arn:aws:glue:{args[1]}:{args[0]}:catalog",
                        f"arn:aws:glue:{args[1]}:{args[0]}:database/{glue_database}",
                        f"arn:aws:glue:{args[1]}:{args[0]}:table/{glue_database}/*",
                        f"arn:aws:glue:{args[1]}:{args[0]}:database/{silver_database_name}",
                        f"arn:aws:glue:{args[1]}:{args[0]}:table/{silver_database_name}/*",
                    ],
                },
                {
                    "Sid": "WriteOptimizerLogs",
                    "Effect": "Allow",
                    "Action": ["logs:CreateLogGroup", "logs:CreateLogStream", "logs:PutLogEvents"],
                    "Resource": [
                        f"arn:aws:logs:{args[1]}:{args[0]}:log-group:/aws-glue/iceberg-{kind}/logs:*"
                        for kind in ["compaction", "retention", "orphan-file-deletion"]
                    ],
                },
            ],
        }
    )
)


# --- IAM: the silver job reads the bronze table and writes the silver tables ---

silver_policy_document = pulumi.Output.all(account_id, region).apply(
    lambda args: json.dumps(
        {
            "Version": "2012-10-17",
            "Statement": [
                {
                    "Sid": "ListTableFiles",
                    "Effect": "Allow",
                    "Action": "s3:ListBucket",
                    "Resource": warehouse_bucket_arn,
                    "Condition": {"StringLike": {"s3:prefix": [f"iceberg/{glue_database}/*", f"iceberg/{silver_database_name}/*"]}},
                },
                {
                    "Sid": "ReadBronze",
                    "Effect": "Allow",
                    "Action": "s3:GetObject",
                    "Resource": f"{warehouse_bucket_arn}/iceberg/{glue_database}/*",
                },
                {
                    "Sid": "WriteSilver",
                    "Effect": "Allow",
                    "Action": ["s3:GetObject", "s3:PutObject", "s3:DeleteObject"],
                    "Resource": f"{warehouse_bucket_arn}/iceberg/{silver_database_name}/*",
                },
                {
                    "Sid": "ReadBronzeCatalog",
                    "Effect": "Allow",
                    "Action": ["glue:GetDatabase", "glue:GetTable", "glue:GetTables"],
                    "Resource": [
                        f"arn:aws:glue:{args[1]}:{args[0]}:catalog",
                        f"arn:aws:glue:{args[1]}:{args[0]}:database/{glue_database}",
                        f"arn:aws:glue:{args[1]}:{args[0]}:table/{glue_database}/*",
                    ],
                },
                {
                    "Sid": "WriteSilverCatalog",
                    "Effect": "Allow",
                    "Action": ["glue:GetDatabase", "glue:GetTable", "glue:GetTables", "glue:CreateTable", "glue:UpdateTable"],
                    "Resource": [
                        f"arn:aws:glue:{args[1]}:{args[0]}:catalog",
                        f"arn:aws:glue:{args[1]}:{args[0]}:database/{silver_database_name}",
                        f"arn:aws:glue:{args[1]}:{args[0]}:table/{silver_database_name}/*",
                    ],
                },
            ],
        }
    )
)

# --- IAM: the report build reads the silver tables, nothing else ---
# Through Glue's Iceberg REST endpoint (DuckDB attaches the catalog with it),
# which also needs glue:GetCatalog.

# Prefix in the warehouse bucket for the built report site (see reports/lib/publish.py).
reports_site_prefix = config.get("reportsSitePrefix") or "reports"

reports_policy_document = pulumi.Output.all(account_id, region).apply(
    lambda args: json.dumps(
        {
            "Version": "2012-10-17",
            "Statement": [
                # Bronze only for the health page's freshness reading, max(ingested_at)
                # over recent partitions; reports never read its payloads.
                {
                    "Sid": "ListLakeFiles",
                    "Effect": "Allow",
                    "Action": "s3:ListBucket",
                    "Resource": warehouse_bucket_arn,
                    "Condition": {
                        "StringLike": {
                            "s3:prefix": [f"iceberg/{silver_database_name}/*", f"iceberg/{glue_database}/*", f"{reports_site_prefix}/*"]
                        }
                    },
                },
                # The built site: the CronJob uploads builds here and the web pod's
                # sidecar downloads them. The lake itself stays read-only.
                {
                    "Sid": "PublishSite",
                    "Effect": "Allow",
                    "Action": ["s3:PutObject", "s3:GetObject", "s3:DeleteObject"],
                    "Resource": f"{warehouse_bucket_arn}/{reports_site_prefix}/*",
                },
                {
                    "Sid": "ReadLake",
                    "Effect": "Allow",
                    "Action": "s3:GetObject",
                    "Resource": [
                        f"{warehouse_bucket_arn}/iceberg/{silver_database_name}/*",
                        f"{warehouse_bucket_arn}/iceberg/{glue_database}/*",
                    ],
                },
                {
                    "Sid": "ReadLakeCatalog",
                    "Effect": "Allow",
                    "Action": ["glue:GetCatalog", "glue:GetDatabase", "glue:GetDatabases", "glue:GetTable", "glue:GetTables"],
                    "Resource": [
                        f"arn:aws:glue:{args[1]}:{args[0]}:catalog",
                        f"arn:aws:glue:{args[1]}:{args[0]}:database/{silver_database_name}",
                        f"arn:aws:glue:{args[1]}:{args[0]}:table/{silver_database_name}/*",
                        f"arn:aws:glue:{args[1]}:{args[0]}:database/{glue_database}",
                        f"arn:aws:glue:{args[1]}:{args[0]}:table/{glue_database}/*",
                    ],
                },
            ],
        }
    )
)

# --- Container logs: Fluent Bit on the cluster ships pod logs here ---

# Created here, not by Fluent Bit, so it gets a retention period and the shipper
# needs no logs:CreateLogGroup.
container_logs = aws.cloudwatch.LogGroup(
    "containers",
    name=container_log_group,
    retention_in_days=container_log_retention_days,
)

fluent_bit_policy_document = container_logs.arn.apply(
    lambda arn: json.dumps(
        {
            "Version": "2012-10-17",
            "Statement": [
                {
                    "Sid": "WriteContainerLogs",
                    "Effect": "Allow",
                    "Action": ["logs:CreateLogStream", "logs:PutLogEvents", "logs:DescribeLogStreams"],
                    # The group ARN ends in ":*", which also covers its streams.
                    "Resource": arn if arn.endswith(":*") else f"{arn}:*",
                }
            ],
        }
    )
)


def _pod_role(resource_name: str, name: str, description: str) -> aws.iam.Role:
    """A role assumed through EKS Pod Identity. The association to a service
    account lives in the platform stack."""
    return bsky_iam.role(
        resource_name,
        name=name,
        description=description,
        assume_role_policy=json.dumps(
            {
                "Version": "2012-10-17",
                "Statement": [
                    {
                        "Effect": "Allow",
                        "Principal": {"Service": "pods.eks.amazonaws.com"},
                        "Action": ["sts:AssumeRole", "sts:TagSession"],
                    }
                ],
            }
        ),
    )


# Created through bsky_iam, which keeps it inside the delegated IAM scope (see
# iam-bootstrap/). In an account without that delegation, turn manageIam off and
# hand the exported document to an admin instead.
if manage_iam:
    loader_policy = bsky_iam.policy(
        "iceberg-loader",
        name="bsky-iceberg-loader",
        description="Consume raw-file notifications and append to the raw Iceberg tables",
        policy=loader_policy_document,
    )
    pulumi.export("loader_policy_arn", loader_policy.arn)

    loader_role = _pod_role("iceberg-loader", "bsky-iceberg-loader", "EKS pod role for the Iceberg loader")
    aws.iam.RolePolicyAttachment("iceberg-loader", role=loader_role.name, policy_arn=loader_policy.arn)
    pulumi.export("loader_role_arn", loader_role.arn)

    ingestor_policy = bsky_iam.policy(
        "ingestor",
        name="bsky-ingestor",
        description="Write raw Jetstream files and read them back to resume",
        policy=ingestor_policy_document,
    )
    ingestor_role = _pod_role("ingestor", "bsky-ingestor", "EKS pod role for the Jetstream ingestor")
    aws.iam.RolePolicyAttachment("ingestor", role=ingestor_role.name, policy_arn=ingestor_policy.arn)
    pulumi.export("ingestor_role_arn", ingestor_role.arn)

    fluent_bit_policy = bsky_iam.policy(
        "fluent-bit",
        name="bsky-fluent-bit",
        description="Ship container logs to the bsky container log group",
        policy=fluent_bit_policy_document,
    )
    fluent_bit_role = _pod_role("fluent-bit", "bsky-fluent-bit", "EKS pod role for the Fluent Bit log shipper")
    aws.iam.RolePolicyAttachment("fluent-bit", role=fluent_bit_role.name, policy_arn=fluent_bit_policy.arn)
    pulumi.export("fluent_bit_role_arn", fluent_bit_role.arn)

    silver_policy = bsky_iam.policy(
        "silver",
        name="bsky-silver",
        description="Read the bronze Iceberg table and write the silver tables",
        policy=silver_policy_document,
    )
    silver_role = _pod_role("silver", "bsky-silver", "EKS pod role for the silver job")
    aws.iam.RolePolicyAttachment("silver", role=silver_role.name, policy_arn=silver_policy.arn)
    pulumi.export("silver_role_arn", silver_role.arn)

    reports_policy = bsky_iam.policy(
        "reports",
        name="bsky-reports",
        description="Read the silver Iceberg tables for the report build",
        policy=reports_policy_document,
    )
    reports_role = _pod_role("reports", "bsky-reports", "EKS pod role for the report build")
    aws.iam.RolePolicyAttachment("reports", role=reports_role.name, policy_arn=reports_policy.arn)
    pulumi.export("reports_role_arn", reports_role.arn)

    # --- Glue table optimizers: compaction, snapshot retention, orphan files ---
    # Maintenance runs in the catalog rather than the pipeline, so moving to
    # another catalog (R2 Data Catalog, BigLake, ...) swaps it with the catalog.

    optimizer_log_groups = {
        kind: aws.cloudwatch.LogGroup(
            f"glue-{kind}",
            name=f"/aws-glue/iceberg-{kind}/logs",
            retention_in_days=14,
        )
        for kind in ["compaction", "retention", "orphan-file-deletion"]
    }

    optimizer_policy = bsky_iam.policy(
        "table-optimizer",
        name="bsky-table-optimizer",
        description="Compact and expire the lake's Iceberg tables",
        policy=table_optimizer_policy_document,
    )
    optimizer_role = bsky_iam.role(
        "table-optimizer",
        name="bsky-table-optimizer",
        description="Assumed by Glue's Iceberg table optimizers",
        assume_role_policy=json.dumps(
            {
                "Version": "2012-10-17",
                "Statement": [
                    {"Effect": "Allow", "Principal": {"Service": "glue.amazonaws.com"}, "Action": "sts:AssumeRole"}
                ],
            }
        ),
    )
    optimizer_attachment = aws.iam.RolePolicyAttachment(
        "table-optimizer", role=optimizer_role.name, policy_arn=optimizer_policy.arn
    )

    def _optimizer_configs(role_arn: pulumi.Output[str]) -> dict[str, aws.glue.CatalogTableOptimizerConfigurationArgs]:
        return {
            "compaction": aws.glue.CatalogTableOptimizerConfigurationArgs(
                role_arn=role_arn,
                enabled=True,
                compaction_configuration=aws.glue.CatalogTableOptimizerConfigurationCompactionConfigurationArgs(
                    iceberg_configuration=aws.glue.CatalogTableOptimizerConfigurationCompactionConfigurationIcebergConfigurationArgs(
                        strategy="binpack",
                        # The default of 100 never triggers: an hour partition
                        # gets ~17 files of ~16 MB.
                        min_input_files=config.get_int("compactionMinInputFiles") or 10,
                    )
                ),
            ),
            "retention": aws.glue.CatalogTableOptimizerConfigurationArgs(
                role_arn=role_arn,
                enabled=True,
                retention_configuration=aws.glue.CatalogTableOptimizerConfigurationRetentionConfigurationArgs(
                    iceberg_configuration=aws.glue.CatalogTableOptimizerConfigurationRetentionConfigurationIcebergConfigurationArgs(
                        # ~400 snapshots a day (one per loader commit).
                        snapshot_retention_period_in_days=config.get_int("snapshotRetentionDays") or 3,
                        number_of_snapshots_to_retain=10,
                        # Otherwise files replaced by compaction are never deleted.
                        clean_expired_files=True,
                    )
                ),
            ),
            "orphan_file_deletion": aws.glue.CatalogTableOptimizerConfigurationArgs(
                role_arn=role_arn,
                enabled=True,
                orphan_file_deletion_configuration=aws.glue.CatalogTableOptimizerConfigurationOrphanFileDeletionConfigurationArgs(
                    iceberg_configuration=aws.glue.CatalogTableOptimizerConfigurationOrphanFileDeletionConfigurationIcebergConfigurationArgs(
                        orphan_file_retention_period_in_days=3,
                    )
                ),
            ),
        }

    # Bronze's resource names predate silver and stay unprefixed.
    table_optimizers = {
        (db.name, table, kind): aws.glue.CatalogTableOptimizer(
            f"{prefix}{table}-{kind.replace('_', '-')}",
            catalog_id=account_id,
            database_name=db.name,
            table_name=table,
            type=kind,
            configuration=configuration,
            opts=pulumi.ResourceOptions(depends_on=[optimizer_attachment, *optimizer_log_groups.values()]),
        )
        for db, prefix, tables in [
            (database, "", optimized_tables),
            (silver_database, "silver-", optimized_silver_tables),
        ]
        for table in tables
        for kind, configuration in _optimizer_configs(optimizer_role.arn).items()
    }

# --- Alerts: pipeline alarms and a cost budget, by email ---

alerts = aws.sns.Topic("alerts", name="bsky-alerts")
# Email subscriptions stay "pending confirmation" until the link AWS sends is clicked.
aws.sns.TopicSubscription("alerts-email", topic=alerts.arn, protocol="email", endpoint=alert_email)


def _queue_alarm(name: str, description: str, queue_name: pulumi.Input[str], **kwargs) -> aws.cloudwatch.MetricAlarm:
    return aws.cloudwatch.MetricAlarm(
        name,
        name=f"bsky-{name}",
        alarm_description=description,
        namespace="AWS/SQS",
        dimensions={"QueueName": queue_name},
        period=300,
        alarm_actions=[alerts.arn],
        ok_actions=[alerts.arn],
        **kwargs,
    )


# The ingestor writes a file every ~3.5 min, each one a notification. Missing
# data counts as stalled too: SQS stops reporting for an idle queue.
ingest_stalled = _queue_alarm(
    "ingest-stalled",
    "No raw files for 15 minutes: the ingestor stopped writing",
    queue.name,
    metric_name="NumberOfMessagesSent",
    statistic="Sum",
    comparison_operator="LessThanThreshold",
    threshold=1,
    evaluation_periods=3,
    treat_missing_data="breaching",
)
# A commit that falls back to the visibility timeout (10 min) shouldn't page,
# so this needs two periods over 15 min.
loader_behind = _queue_alarm(
    "loader-behind",
    "Oldest raw-file notification is over 15 minutes old: the loader is stuck or down",
    queue.name,
    metric_name="ApproximateAgeOfOldestMessage",
    statistic="Maximum",
    comparison_operator="GreaterThanThreshold",
    threshold=900,
    evaluation_periods=2,
    treat_missing_data="notBreaching",
)
poison_messages = _queue_alarm(
    "dead-letters",
    "Messages in the dead-letter queue: a file or notification keeps failing",
    dlq.name,
    metric_name="ApproximateNumberOfMessagesVisible",
    statistic="Maximum",
    comparison_operator="GreaterThanThreshold",
    threshold=0,
    evaluation_periods=1,
    treat_missing_data="notBreaching",
)

# Silver logs one line per committed window ("bsky_silver.job: run <id>: bronze
# (...], N row(s), appended {...}"), and only silver logs as bsky_silver.job, so
# the filter needs no log-stream selection. Bronze always grows, so every hourly
# run commits at least one window, and a long catch-up commits one every few
# minutes. Two clock hours without one means two runs in a row failed, hung or
# never started; a single failed run that the next one recovers doesn't alarm.
SILVER_METRIC_NAMESPACE = "bsky"
silver_windows = aws.cloudwatch.LogMetricFilter(
    "silver-windows",
    name="bsky-silver-windows",
    log_group_name=container_logs.name,
    pattern='"bsky_silver.job: run" "row(s), appended"',
    metric_transformation=aws.cloudwatch.LogMetricFilterMetricTransformationArgs(
        namespace=SILVER_METRIC_NAMESPACE,
        name="SilverWindowsCommitted",
        value="1",
        default_value="0",
    ),
)
silver_stalled = aws.cloudwatch.MetricAlarm(
    "silver-stalled",
    name="bsky-silver-stalled",
    alarm_description="No silver window committed in two clock hours: silver runs are failing, hung or not starting",
    namespace=SILVER_METRIC_NAMESPACE,
    metric_name="SilverWindowsCommitted",
    statistic="Sum",
    period=3600,
    evaluation_periods=2,
    comparison_operator="LessThanThreshold",
    threshold=1,
    # No log lines at all (Fluent Bit or the cluster down) is no news either.
    treat_missing_data="breaching",
    alarm_actions=[alerts.arn],
    ok_actions=[alerts.arn],
    opts=pulumi.ResourceOptions(depends_on=[silver_windows]),
)

# Account-wide: filtering by the project tag would need cost allocation tags
# activated by an admin.
budget = aws.budgets.Budget(
    "monthly",
    name="bsky-monthly",
    budget_type="COST",
    time_unit="MONTHLY",
    limit_amount=budget_monthly_usd,
    limit_unit="USD",
    notifications=[
        aws.budgets.BudgetNotificationArgs(
            comparison_operator="GREATER_THAN",
            threshold=pct,
            threshold_type="PERCENTAGE",
            notification_type=kind,
            subscriber_email_addresses=[alert_email],
        )
        for kind, pct in [("ACTUAL", 50), ("ACTUAL", 80), ("ACTUAL", 100), ("FORECASTED", 100)]
    ],
)

pulumi.export("queue_url", queue.url)
pulumi.export("dead_letter_queue_url", dlq.url)
pulumi.export("alerts_topic_arn", alerts.arn)
pulumi.export("container_log_group", container_logs.name)
pulumi.export("athena_workgroup", athena_workgroup.name)
pulumi.export("athena_results_bucket", athena_results.bucket)
pulumi.export("glue_database", database.name)
pulumi.export("silver_glue_database", silver_database.name)
pulumi.export("silver_location", f"{warehouse}/{silver_database_name}")
pulumi.export("reports_site_url", f"s3://{warehouse_bucket}/{reports_site_prefix}")
pulumi.export("raw_location", f"s3://{bucket}/{raw_prefix.rstrip('/')}")
pulumi.export("raw_table_location", f"{warehouse}/{glue_database}/jetstream")
pulumi.export("loader_policy_document", loader_policy_document)
pulumi.export("ingestor_policy_document", ingestor_policy_document)
pulumi.export("table_optimizer_policy_document", table_optimizer_policy_document)
pulumi.export("fluent_bit_policy_document", fluent_bit_policy_document)
pulumi.export("silver_policy_document", silver_policy_document)
pulumi.export("reports_policy_document", reports_policy_document)

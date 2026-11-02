"""AWS resources for the Bluesky data lake.

The raw-file bucket and the warehouse bucket already exist and are not managed
here; this stack wires the raw bucket's ObjectCreated notifications to the
queue the Iceberg loader consumes and creates the Glue database the raw
Iceberg table lives in, located in the warehouse bucket.
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
# Must exceed the loader's batch window plus the time to load a batch.
visibility_timeout = config.get_int("visibilityTimeout") or 600
max_receive_count = config.get_int("maxReceiveCount") or 5
manage_iam = config.get_bool("manageIam", True)
# Iceberg tables in the raw database that Glue's table optimizers maintain.
# The loader creates the tables, so add one here only once it exists.
optimized_tables = config.get_object("optimizedTables") or ["jetstream"]

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

# --- Glue: database for the raw (bronze) Iceberg tables ---

database = aws.glue.CatalogDatabase(
    "raw",
    name=glue_database,
    description="Raw (bronze) layer of the Bluesky data lake",
    location_uri=f"{warehouse}/{glue_database}",
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

    table_optimizers = {
        (table, kind): aws.glue.CatalogTableOptimizer(
            f"{table}-{kind.replace('_', '-')}",
            catalog_id=account_id,
            database_name=database.name,
            table_name=table,
            type=kind,
            configuration=configuration,
            opts=pulumi.ResourceOptions(depends_on=[optimizer_attachment, *optimizer_log_groups.values()]),
        )
        for table in optimized_tables
        for kind, configuration in _optimizer_configs(optimizer_role.arn).items()
    }

pulumi.export("queue_url", queue.url)
pulumi.export("dead_letter_queue_url", dlq.url)
pulumi.export("glue_database", database.name)
pulumi.export("raw_location", f"s3://{bucket}/{raw_prefix.rstrip('/')}")
pulumi.export("raw_table_location", f"{warehouse}/{glue_database}/jetstream")
pulumi.export("loader_policy_document", loader_policy_document)
pulumi.export("ingestor_policy_document", ingestor_policy_document)
pulumi.export("table_optimizer_policy_document", table_optimizer_policy_document)

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
                    "Action": ["sqs:ReceiveMessage", "sqs:DeleteMessage", "sqs:GetQueueAttributes"],
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

# Creating IAM policies needs IAM admin rights (PowerUserAccess can't), so this
# can be turned off and the exported document handed to an admin instead.
if manage_iam:
    loader_policy = aws.iam.Policy(
        "iceberg-loader",
        name="bsky-iceberg-loader",
        description="Consume raw-file notifications and append to the raw Iceberg tables",
        policy=loader_policy_document,
    )
    pulumi.export("loader_policy_arn", loader_policy.arn)

pulumi.export("queue_url", queue.url)
pulumi.export("dead_letter_queue_url", dlq.url)
pulumi.export("glue_database", database.name)
pulumi.export("raw_table_location", f"{warehouse}/{glue_database}/jetstream")
pulumi.export("loader_policy_document", loader_policy_document)

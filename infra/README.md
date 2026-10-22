# infra

Pulumi stack (Python, uv) for the AWS side of the data lake. The raw-file bucket (`BSKY_BUCKET`) and the warehouse bucket holding the Iceberg tables (`BSKY_WAREHOUSE_BUCKET`) already exist and are **not** managed here.

| Resource | Name | Purpose |
|---|---|---|
| SQS queue | `bsky-raw-files` | S3 ObjectCreated notifications for raw files; consumed by `iceberg-loader` |
| SQS queue | `bsky-raw-files-dlq` | Dead-letter queue; messages land here after 5 failed receives |
| SQS queue policy | | Lets S3 (this bucket and account only) send to the queue |
| S3 bucket notification | | `s3:ObjectCreated:*` for `<prefix>/*.parquet` → queue |
| Glue database | `bsky_raw` | Raw (bronze) layer, location `s3://<warehouse-bucket>/iceberg/bsky_raw` |
| IAM policy | `bsky-iceberg-loader` | What the loader and its Sail server need (SQS, S3, Glue); attach to its role/user. Only created when `manageIam` is true. The document is always exported as `loader_policy_document`. |

**The bucket notification owns the bucket's whole notification configuration.** Any other notification targets on the bucket must be added to this stack, or they'll be removed on `pulumi up`.

The notification filters on the ingestor's prefix and `.parquet`. Iceberg tables live in the separate warehouse bucket, so the loader's own writes can't trigger it.

## Configuration

Bucket names come from `BSKY_BUCKET` and `BSKY_WAREHOUSE_BUCKET`, and the raw prefix from `BSKY_PREFIX` (default `jetstream`). They're read from the repo's `.env`, so bucket names aren't committed. Stack config can override them:

| Key | Default |
|---|---|
| `aws:region` | `us-west-2` (in `Pulumi.dev.yaml`) |
| `bsky-data-lake:bucket` | `$BSKY_BUCKET` |
| `bsky-data-lake:warehouseBucket` | `$BSKY_WAREHOUSE_BUCKET` |
| `bsky-data-lake:rawPrefix` | `$BSKY_PREFIX` or `jetstream` |
| `bsky-data-lake:glueDatabase` | `bsky_raw` |
| `bsky-data-lake:visibilityTimeout` | `600` (seconds; must exceed the loader's batch window plus load time) |
| `bsky-data-lake:maxReceiveCount` | `5` |
| `bsky-data-lake:manageIam` | `true` (`false` in `dev`: the SSO PowerUserAccess role can't create IAM policies) |

## State backend

State is kept in a dedicated S3 bucket (`pulumi-state-<account>-<region>-an`). Stack secrets are encrypted with a passphrase kept outside the repo in `~/.config/bsky-data-lake/pulumi-passphrase`. Losing that file means losing any secret config values; there are none today. Rather than changing the global `pulumi login`, set:

```bash
export AWS_PROFILE=<profile>     # Pulumi and its AWS provider read profiles, including SSO
export PULUMI_BACKEND_URL="s3://<state-bucket>?region=us-west-2"
export PULUMI_CONFIG_PASSPHRASE_FILE=~/.config/bsky-data-lake/pulumi-passphrase
```

## Usage

```bash
pulumi stack select dev        # first time: pulumi stack init dev
pulumi preview
pulumi up

pulumi stack output queue_url  # → BSKY_LOADER_QUEUE_URL in .env
```

## Tests

```bash
uv run pytest    # checks resources with Pulumi mocks; no AWS or Pulumi backend needed
```

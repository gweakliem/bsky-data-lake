# infra

A Pulumi Python stack (uv toolchain) for the SQS queue + DLQ, the S3 bucket notification, the Glue database `bsky_raw` and the loader's IAM policy. The raw and warehouse buckets are pre-existing and not managed.
- **Commands:** run them from this directory. `uv run pytest` checks resources with Pulumi mocks; `pulumi preview` / `pulumi up` need `AWS_PROFILE`.
- **Bucket names** come from `BSKY_BUCKET` / `BSKY_WAREHOUSE_BUCKET` in `.env`, so they aren't committed. Iceberg tables live in the warehouse bucket.
- **`BucketNotification` owns the bucket's entire notification configuration.**
- **State and passphrase:** state is in a dedicated S3 state bucket, not Pulumi Cloud; the CLI's default login is an ephemeral agent account. Before any `pulumi` command, set `AWS_PROFILE`, `PULUMI_BACKEND_URL=s3://<state-bucket>?region=us-west-2` and `PULUMI_CONFIG_PASSPHRASE_FILE=~/.config/bsky-data-lake/pulumi-passphrase`. The `pulumi` binary is at `~/.pulumi/bin/pulumi` and isn't on PATH.
- **IAM delegation:** run Pulumi with the `BskyDataLakeDeveloper` SSO profile (`PowerUserAccess` can't touch IAM). That role may only create IAM roles and policies under `/bsky/`, and roles only with the `bsky-workload-boundary` permissions boundary. The admin-applied setup is in `iam-bootstrap/`.
  - Create every `Role`/`Policy` through `bsky_iam.role` / `bsky_iam.policy`, never `aws.iam.*` directly.
  - `test_every_iam_resource_in_delegated_scope` fails on anything that bypasses them.

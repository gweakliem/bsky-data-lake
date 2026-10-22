# infra

A Pulumi Python stack (uv toolchain) for the SQS queue + DLQ, the S3 bucket notification, the Glue database `bsky_raw` and the loader's IAM policy. The raw and warehouse buckets are pre-existing and not managed.
- **Commands:** run them from this directory. `uv run pytest` checks resources with Pulumi mocks; `pulumi preview` / `pulumi up` need `AWS_PROFILE`.
- **Bucket names** come from `BSKY_BUCKET` / `BSKY_WAREHOUSE_BUCKET` in `.env`, so they aren't committed. Iceberg tables live in the warehouse bucket.
- **`BucketNotification` owns the bucket's entire notification configuration.**
- **State and passphrase:** state is in a dedicated S3 state bucket, not Pulumi Cloud; the CLI's default login is an ephemeral agent account. Before any `pulumi` command, set `AWS_PROFILE`, `PULUMI_BACKEND_URL=s3://<state-bucket>?region=us-west-2` and `PULUMI_CONFIG_PASSPHRASE_FILE=~/.config/bsky-data-lake/pulumi-passphrase`. The `pulumi` binary is at `~/.pulumi/bin/pulumi` and isn't on PATH.
- **IAM is off in `dev`:** `manageIam` is `false` there because the SSO `PowerUserAccess` role can't create IAM policies. The policy JSON is exported as `loader_policy_document` for an admin to create.

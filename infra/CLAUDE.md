# infra

A Pulumi Python stack (uv toolchain) for the SQS queue + DLQ, the S3 bucket notification, the Glue database `bsky_raw` and the loader's IAM policy. The raw and warehouse buckets predate the stack: it imports them (`protect` and `retain_on_delete`, so Pulumi never deletes them) for their tags only.
- **Athena:** query in the `bsky` workgroup. It enforces its results location, the stack's own `bsky-athena-results-<account>-<region>-an` bucket (account-regional namespace), where results expire after `athenaResultsRetentionDays` (7).
- **Three Pulumi projects**, each its own uv environment with mock tests (`uv run pytest` in its directory), linked by `StackReference` (`organization/<project>/<stack>`):
  - `infra/` (`bsky-data-lake`, the data stack): queues, notification, Glue, and the workload IAM roles and policies.
  - `infra/platform/` (`bsky-platform`): VPC, EKS Auto Mode cluster, ECR, and Pod Identity associations (service account → data-stack role). It imports `../bsky_iam.py`.
  - `infra/apps/` (`bsky-apps`): Kubernetes objects. Image tags come from stack config (`loaderImageTag`).
  - Apply order is data → platform → apps.
- **Commands:** run them from this directory. `uv run pytest` checks resources with Pulumi mocks; `pulumi preview` / `pulumi up` need `AWS_PROFILE`.
- **Bucket names** come from `BSKY_BUCKET` / `BSKY_WAREHOUSE_BUCKET` in `.env`, so they aren't committed. Iceberg tables live in the warehouse bucket.
- **`BucketNotification` and `BucketLifecycleConfiguration` each own the raw bucket's entire notification or lifecycle configuration.** The `Bucket` resources hold only the name, namespace and tags; a v7 `Bucket` has no inline lifecycle or notification, so they don't conflict. Raw files (and `jetstream-test/`) expire after `rawRetentionDays` (7).
- **Container logs:** Fluent Bit (apps stack) ships the namespaces in `logNamespaces` (default `bsky`) to `/bsky/containers`, which the data stack owns. A Pod Identity association in the platform stack gives it the data stack's `bsky-fluent-bit` role. Its config is a ConfigMap whose hash is a pod-template annotation, so a config change rolls the DaemonSet.
- **EKS API allowlist:** `BSKY_EKS_PUBLIC_CIDRS` in `.env`. From a new network, kubectl and the apps stack time out until that address is added and the platform stack applied.
- **Alerts:** alarms and the budget email `BSKY_ALERT_EMAIL` from `.env` (or `alertEmail` config). It's personal, so it isn't committed.
- **State and passphrase:** state is in a dedicated S3 state bucket, not Pulumi Cloud; the CLI's default login is an ephemeral agent account. Before any `pulumi` command, set `AWS_PROFILE`, `PULUMI_BACKEND_URL=s3://<state-bucket>?region=us-west-2` and `PULUMI_CONFIG_PASSPHRASE_FILE=~/.config/bsky-data-lake/pulumi-passphrase`. The `pulumi` binary is at `~/.pulumi/bin/pulumi` and isn't on PATH.
- **IAM delegation:** run Pulumi with the `BskyDataLakeDeveloper` SSO profile (`PowerUserAccess` can't touch IAM). That role may only create IAM roles and policies under `/bsky/`, and roles only with the `bsky-workload-boundary` permissions boundary. The admin-applied setup is in `iam-bootstrap/`.
  - Create every `Role`/`Policy` through `bsky_iam.role` / `bsky_iam.policy`, never `aws.iam.*` directly.
  - `test_every_iam_resource_in_delegated_scope` fails on anything that bypasses them.

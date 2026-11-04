# Runbook: monitoring and operations

How to check that the pipeline is healthy and do routine operations on it. The repo is public, so names that identify the account (account ID, bucket names, the Pulumi state bucket) appear here as `<placeholders>`; the real values are in `.env`.

## Setup

Every command below assumes an AWS session for the `BskyDataLakeDeveloper` SSO profile:

```bash
aws sso login --profile <profile>          # when commands fail with "Token has expired"
export AWS_PROFILE=<profile> AWS_REGION=us-west-2
```

**Pulumi** (only for deploys):

```bash
export PULUMI_BACKEND_URL="s3://<state-bucket>?region=us-west-2"
export PULUMI_CONFIG_PASSPHRASE_FILE=~/.config/bsky-data-lake/pulumi-passphrase
alias pulumi=~/.pulumi/bin/pulumi
```

**kubectl:** the platform stack outputs a kubeconfig that gets tokens from `aws eks get-token`, so it holds no secrets.

```bash
(cd infra/platform && pulumi stack output kubeconfig --stack dev > ~/.kube/bsky.json)
export KUBECONFIG=~/.kube/bsky.json
```

The cluster API only accepts the addresses in `BSKY_EKS_PUBLIC_CIDRS` (`.env`). From any other network, such as a phone hotspot, kubectl times out (`dial tcp ...:443: i/o timeout`). Either add the address and apply the platform stack, or use the [checks that don't need the cluster](#without-cluster-access).

## What runs

| Component | Kind | Where | Healthy when |
|---|---|---|---|
| ingestor | Deployment `ingestor` | `bsky` namespace | a raw Parquet file lands in `s3://<raw-bucket>/jetstream/` every minute or so |
| loader | Deployment `iceberg-loader` | `bsky` namespace | the queue `bsky-raw-files` stays near empty; bronze `ingested_at` is minutes old |
| silver | CronJob `silver`, `:05` hourly | `bsky` namespace | `silver_runs.bronze_through` trails bronze by about an hour at most |
| log shipping | DaemonSet (Fluent Bit) | `logging` namespace | new lines appear in `/bsky/containers` |
| table maintenance | Glue table optimizers | Glue console, per table | runs show as succeeded in the table's optimization history |

Data flow: Jetstream → ingestor → raw Parquet in S3 → S3 notification → SQS `bsky-raw-files` → loader → bronze `bsky_raw.jetstream` (Iceberg) → silver → `bsky_silver.*`.

## Daily check

1. **Alarms.** All three should be `OK`.

   ```bash
   aws cloudwatch describe-alarms --alarm-name-prefix bsky- \
     --query 'MetricAlarms[].[AlarmName,StateValue,StateUpdatedTimestamp]' --output table
   ```

2. **Pods** (needs cluster access).

   ```bash
   kubectl -n bsky get deploy,cronjob,jobs,pods
   ```

   Both Deployments should be `1/1`. Recent silver Jobs should be `Complete`; see [silver](#silver) for when `Failed` is expected.

3. **Freshness**, in Athena (below):

   ```sql
   SELECT max(ingested_at) FROM bsky_raw.jetstream;            -- minutes old
   SELECT max(bronze_through) FROM bsky_silver.silver_runs;    -- within about an hour of bronze
   ```

4. **Spend.** The `bsky-monthly` budget emails at 50%, 80% and 100% of $250, and when the forecast passes 100%. Cost Explorer has the detail.

## Alarms

Alarms and budget alerts go to the SNS topic `bsky-alerts`, which emails `BSKY_ALERT_EMAIL`.

| Alarm | Fires when | Check |
|---|---|---|
| `bsky-ingest-stalled` | no raw-file notifications for 15 min | ingestor pod status and logs. It resumes from its newest file on restart, so a restart replays the gap. |
| `bsky-loader-behind` | the oldest queued notification is over 15 min old | loader pod status and logs: OOM kills, Glue commit errors, S3 permission errors. |
| `bsky-dead-letters` | anything in `bsky-raw-files-dlq` | a file or notification failed 5 times. Read the message (below) and the loader logs from that time. |

Read dead letters without removing them:

```bash
DLQ=$(aws sqs get-queue-url --queue-name bsky-raw-files-dlq --query QueueUrl --output text)
aws sqs receive-message --queue-url "$DLQ" --max-number-of-messages 10 --visibility-timeout 0
```

An `s3:TestEvent` message is S3 confirming a notification config change, not a failure; delete it. After fixing the cause of real failures, redrive them to the main queue with `aws sqs start-message-move-task --source-arn <dlq-arn>`. The loader skips files that are already in bronze, so a redrive doesn't duplicate rows.

Nothing alarms on failed silver Jobs yet. Check them in the daily check.

## Logs

Fluent Bit ships everything in the `bsky` namespace to the log group `/bsky/containers`, kept for 14 days, with one stream per `<pod>.<container>` (for example `silver-29895245-ml77h.silver`).

```bash
aws logs tail /bsky/containers --log-stream-name-prefix silver- --follow    # or ingestor-, iceberg-loader-
aws logs tail /bsky/containers --since 1h --filter-pattern ERROR
```

Logs Insights queries for the CloudWatch console:

```
# Errors across the pipeline
fields @timestamp, @logStream, @message
| filter @message like /(?i)error|traceback|exception/
| sort @timestamp desc

# Silver windows: one line per committed window
fields @timestamp, @message
| filter @logStream like /^silver-/ and @message like /bronze \(/
| sort @timestamp desc
```

Glue table optimizers log to `/aws-glue/iceberg-compaction/logs`, `/aws-glue/iceberg-retention/logs` and `/aws-glue/iceberg-orphan-file-deletion/logs`.

## Athena

Choose the **`bsky`** workgroup in the query editor. It writes results to the stack's results bucket, where they expire after 7 days. Both databases, `bsky_raw` and `bsky_silver`, come from the Glue catalog. Queries cost $5 per TB scanned; `silver_runs` and queries answered from Iceberg metadata cost almost nothing.

```sql
-- Silver progress: one row per committed window
SELECT finished_at, bronze_after, bronze_through, bronze_rows,
       json_extract_scalar(counts, '$.appended.posts') AS posts
FROM bsky_silver.silver_runs
ORDER BY finished_at DESC
LIMIT 20;

-- Silver totals
SELECT count(*) AS windows, sum(bronze_rows) AS bronze_rows, max(bronze_through) AS through
FROM bsky_silver.silver_runs;
```

`counts` is JSON with three keys: `appended` (rows written per table, including `post_edges`), `staged` (the same before deduplication) and `ignored` (rows per collection that has no silver table).

## Silver

The CronJob runs at `:05` each hour with `concurrencyPolicy: Forbid`, so a run that's still going makes the next one skip. Each run processes windows of up to 500k bronze rows until it has caught up to five minutes ago, then exits. A window always takes whole loader commits, so a single loader commit can make a window bigger than 500k rows.

**During a backlog**, a run keeps going until `activeDeadlineSeconds` (55 min) kills it, so the Job ends `Failed` with `DeadlineExceeded`. That's expected and safe: the interrupted window isn't recorded in `silver_runs`, and the next run redoes it; deduplication drops any rows it had already appended.

**Edge backfill:** once caught up, a run builds `post_edges` for windows processed before edges existed, until the deadline, and the next run continues. Until it's done, runs end `DeadlineExceeded` as during a backlog. Progress: `SELECT count(*), max(bronze_through) FROM bsky_silver.post_edges_backfill`, against the `silver_runs` windows without a `post_edges` count.

**Memory:** peak is about 1 GB + 2 GB per million rows in one append, against a 3 Gi limit (`silverMemoryLimit`). Appends over 500k rows are split into parts, so a large loader commit shouldn't push it higher. An OOM kill shows as `OOMKilled` in `kubectl describe pod`, and in CloudWatch as a new log stream from the retry pod (the Job retries twice). If it happens, raise `silverMemoryLimit` in the apps stack.

**Run it now**, without waiting for `:05`:

```bash
kubectl -n bsky create job --from=cronjob/silver silver-manual-$(date +%s)
```

**Pause it:** `kubectl -n bsky patch cronjob silver -p '{"spec":{"suspend":true}}'`. The next apps-stack `pulumi up` undoes this.

## Deploy a new image

Images are tagged with the short commit hash. ECR repos: `bsky/ingest` (built from the repo root), `bsky/iceberg-loader` (from `iceberg-loader/`) and `bsky/silver` (from `silver/`).

```bash
REG=<account-id>.dkr.ecr.us-west-2.amazonaws.com
TAG=$(git rev-parse --short HEAD)
aws ecr get-login-password | docker login --username AWS --password-stdin $REG
docker buildx build --platform linux/amd64,linux/arm64 -t $REG/bsky/silver:$TAG --push silver

cd infra/apps
pulumi config set --stack dev silverImageTag $TAG      # or loaderImageTag, ingestImageTag
pulumi preview --stack dev                             # check that only the expected image changes
pulumi up --stack dev
```

Commit the changed `Pulumi.dev.yaml` so the repo records what's deployed.

**Roll back** by setting the previous tag (`git log infra/apps/Pulumi.dev.yaml` shows the history) and running `pulumi up` again.

Infrastructure changes go through the stacks in order: data (`infra/`), then platform (`infra/platform/`), then apps (`infra/apps/`). Preview each before applying.

## Without cluster access

When the cluster API isn't reachable:

- **Pod health:** the [logs](#logs). A pod that keeps restarting shows up as new streams for the same Deployment; a killed or stuck one, as a stream with no new lines.
- **Pipeline health:** the [alarms](#alarms) and the freshness queries in [Athena](#athena).
- **Changes:** deploys of the data and platform stacks work from anywhere. The apps stack needs the cluster API.

# Deployment plan: EKS, managed by Pulumi

## Goal

Run the ingestor, the Iceberg loader and the pipeline jobs from [data-pipeline.md](data-pipeline.md) unattended on EKS. All AWS and Kubernetes resources are defined in Pulumi under `infra/`.

## Workloads

| Workload | Kind | Replicas / schedule | Needs |
|---|---|---|---|
| `bsky-ingest` | Deployment, strategy `Recreate` | exactly 1 | S3 write to the raw prefix |
| `iceberg-loader` | Deployment | 1 (KEDA on SQS depth, 0–N, optional) | SQS, raw S3 read, warehouse S3 read/write, Glue |
| `bsky-silver` | CronJob | hourly | warehouse S3, Glue |
| `bsky-classify` | CronJob | hourly, after silver | warehouse S3, Glue, TypeSafe API key |
| `bsky-embed` | CronJob | hourly, after silver | warehouse S3, Glue; GPU node if E1 picks a model that needs one |
| search API (E3) | Deployment + Service | 1–2 | warehouse S3 (index) read |
| table maintenance | Glue table optimizers (managed) | continuous | Glue, warehouse S3 |

**Why `Recreate` and exactly one replica for the ingestor:** Jetstream is a single stream, so a second replica writes duplicate data, and a rolling update would briefly run two. The restart gap is covered by cursor resume (P1). The pipeline's dedup handles the overlap.

**Job ordering:** the jobs use watermarks (see data-pipeline.md), so they don't strictly need ordering. Running classify or embed before silver just finds less new data. Plain CronJobs with offset schedules are enough; no workflow engine is needed yet. Set `concurrencyPolicy: Forbid` on every CronJob.

## Pulumi structure

Split into three Pulumi projects so each can change and be previewed on its own. Outputs pass between them with `StackReference`.

| Project | Directory | Contents |
|---|---|---|
| data (exists) | `infra/` | SQS + DLQ, bucket notification, Glue databases, IAM policy documents |
| platform | `infra/platform/` | VPC, EKS cluster, ECR repos, add-ons, pod roles, GitHub OIDC |
| apps | `infra/apps/` | Kubernetes namespace, service accounts, Deployments, CronJobs, Secrets, KEDA objects |

**Decide (D0):** whether to move the existing stack to `infra/data/` for symmetry. Moving it needs a state migration (`pulumi stack rename` or export/import), so the default is to leave it at `infra/`.

All projects use the same S3 state backend and passphrase file as the existing stack (see `infra/CLAUDE.md`), with a `dev` stack each. Keep the existing pattern of reading bucket names from `.env` or stack config, and testing with Pulumi mocks (`test_infra.py`).

## WP D0: IAM access (blocker)

**Status: done (2026-10-30).**
- The boundary and permission set are applied, and all four verification checks behaved as expected.
- `infra/bsky_iam.py` and its scope test are in place.
- `manageIam` is on in dev, and `pulumi up` created `policy/bsky/bsky-iceberg-loader`.

The dev SSO role (`PowerUserAccess`) can't create IAM roles or policies, so `manageIam` is off in `dev`. Every workload below needs an IAM role (Pod Identity), and so do the EKS cluster, its nodes and GitHub Actions. This has to be settled first.

**Decision: delegated, boundary-scoped IAM.**
- An admin creates the permissions boundary `bsky-workload-boundary` and a `BskyDataLakeDeveloper` permission set: `PowerUserAccess`, plus an inline policy allowing IAM roles and policies under `/bsky/`, only with that boundary.
- Pulumi then manages all project roles.
- Rejected:
  - a separate admin-run IAM project (every new workload would need an admin);
  - roles created by hand (no infrastructure-as-code record).
- Policies, admin steps and verification are in [`infra/iam-bootstrap/`](../../infra/iam-bootstrap/README.md).

- **Tasks:**
  1. **Admin:** apply `infra/iam-bootstrap/` (boundary policy, permission set, assignment).
  2. **Developer:** add an SSO profile for the new permission set and run the verification commands in that README.
  3. **In `infra/`:**
     - add one helper through which every `aws.iam.Role` and `aws.iam.Policy` is created, with `path="/bsky/"`, and with the boundary ARN built from the caller's account ID on every role;
     - move the loader policy onto it;
     - set `manageIam: "true"` in `Pulumi.dev.yaml`;
     - add a mock test that fails on any role or policy missing the path, or any role missing the boundary.

     Later WPs (D2–D5) create their roles through this helper.
- **Acceptance:**
  - all the verification commands behave as documented;
  - `pulumi up` with the new profile creates `policy/bsky/bsky-iceberg-loader`;
  - the mock test passes.

## WP D1: containerize the components

**Status: loader image done (2026-10-31).** `iceberg-loader/Dockerfile`. The ingestor, silver and enrich images are still open.

- **Depends on:** nothing for the loader image. P1 (cursor resume) before the ingestor image is used unattended (D4b).
- **Order:** the `iceberg-loader` image comes first, for D4a. The others follow their WPs.
- **Images:**
  - **`bsky-ingest`:** multi-stage Dockerfile at the repo root. Build with `cargo build --release` on a Rust image, run on a minimal base (distroless/cc or debian-slim).
  - **`iceberg-loader`, `silver`, `enrich`:** one Dockerfile per uv project, on python-slim with `uv sync --frozen --no-dev`.
    - The loader and silver images include the Sail runtime; check that `pysail`'s wheels support the target architecture.
    - `enrich` may need a CUDA variant for `bsky-embed`, depending on E1.
- **Rules:**
  - Configuration only through env vars, which all components already support.
  - No `.env` baked into images. Add a `.dockerignore` that excludes `.env`, `target/` and `.venv/`.
  - Each image runs as a non-root user and logs to stdout.
- **Credentials:** confirm each component picks up Pod Identity credentials (`AWS_CONTAINER_CREDENTIALS_FULL_URI` + `AWS_CONTAINER_AUTHORIZATION_TOKEN_FILE`).
  - boto3 and the AWS SDKs do.
  - **Sail (pysail 0.7.2):** confirmed in D4a. It reads Glue and S3 using Pod Identity credentials with nothing injected by hand.
  - **boto3 ignores `AWS_REGION`** (it reads only `AWS_DEFAULT_REGION` and `~/.aws/config`). The loader resolves the region through `aws_region()` and passes it explicitly.
  - **Ingestor:** check `object_store`'s `AmazonS3Builder::from_env` explicitly.
  - If a component only supports IRSA (web identity), use IRSA for that workload instead.
- **Architecture: multi-arch (amd64 + arm64)**, built with `docker buildx --platform linux/amd64,linux/arm64`. The built-in Auto Mode `general-purpose` NodePool is **amd64-only** (on-demand c/m/r; the first node was a c6a.large), while the dev laptop is arm64. To run on Graviton, add a custom NodePool that allows `arm64` (`pysail` publishes a `manylinux_2_24_aarch64` wheel). Check the embedding model runtime when E1 picks one.
- **Acceptance:** each image builds locally, and `docker run` with exported credentials and `.env` values passed as env vars reproduces a manual run: the ingestor against `jetstream-test/`, and the loader with `--once`.

## WP D2: ECR and CI builds

**Status: ECR done (2026-10-31)**, in the platform project. A multi-arch push is one tagged index plus untagged per-arch children, which ECR won't expire while the index exists, so the count rule selects tagged images only. CI is still open; until then images are pushed by hand.

- **Depends on:** D0, D1.
- **Platform project:**
  - one ECR repository per image, with scan-on-push and a lifecycle rule that keeps the last ~20 tagged images and expires untagged ones;
  - a GitHub Actions OIDC provider and a role trusted only for this repository's `main` branch (and tags), allowed to push to these repositories.
- **Workflow (`.github/workflows/images.yml`):**
  - on push to `main`, build the changed images, tag them with the git SHA, and push them;
  - on pull requests, run `cargo clippy --all-targets`, `cargo test`, each uv project's `uv run pytest`, and the Pulumi mock tests.
- **Public-repo care:** the account ID and role ARN go in GitHub repository variables or secrets, never in the workflow file.
- **Acceptance:** a merge to `main` produces SHA-tagged images in ECR, and a PR runs all test suites.

## WP D3: VPC and EKS cluster

**Status: done (2026-10-31).** `infra/platform/`, cluster `bsky` on Kubernetes 1.37 with Auto Mode. The permissions boundary was enough for Auto Mode to launch nodes. Use a kubectl within one minor version of the server (an older client prints a skew warning).

- **Depends on:** D0.
- **VPC:**
  - 2 AZs, public and private subnets, and **one** NAT gateway (cost over HA, for dev). Built from plain `pulumi_aws` resources rather than `awsx`, so the mock tests can check them and no extra plugin is needed.
  - Add gateway VPC endpoints for S3, which are free and keep lake traffic off the NAT.
  - Consider interface endpoints for SQS, ECR and STS only if NAT data charges become noticeable.
- **Cluster:**
  - Cluster and node roles are created through `bsky_iam`. The `pulumi-eks` component isn't used, because it creates its own roles outside the delegated scope.
  - Use **EKS Auto Mode**, which handles node provisioning, the Pod Identity agent and core add-ons, so there are no node groups or Karpenter to run. Auto Mode adds a per-instance management fee.
  - The alternative is a small managed node group plus Karpenter. Record the choice here.
  - Private API endpoint plus a public endpoint restricted to the admin IP ranges, set in stack config.
  - Access entries for the SSO admin role. Don't use the `aws-auth` ConfigMap.
  - Enable control-plane logging (api, audit) with short retention.
  - **GPU:** an Auto Mode NodePool for GPU instances, used only if E1 needs a GPU. It scales to zero between embed runs.
- **Outputs:** cluster name, kubeconfig (secret output), OIDC issuer, and VPC/subnet IDs.
- **Acceptance:**
  - `pulumi up` creates the cluster, `kubectl get nodes` works with the SSO admin role, and a test pod schedules.
  - The Pulumi mock tests check the NAT count, the S3 endpoint and the endpoint access settings.

## WP D4: core workloads

D4 is split so the loader, which recovers easily, is the first workload deployed. SQS redelivers anything it doesn't finish, and the `source_file` check skips files it has already loaded, so mistakes while setting up EKS can't lose or duplicate data. The ingestor follows once P1 makes restarts safe.

**Shared setup, built in D4a and reused by D4b:**
- **Apps project** (`pulumi_kubernetes` provider built from the platform stack's kubeconfig output): namespace `bsky`, one service account per workload.
- **Platform project:** Pod Identity associations linking each service account to its role.
- **Image tags** come from stack config (the SHA from D2). A deploy is `pulumi config set` + `pulumi up`, and a rollback is the same with the previous SHA.

### WP D4a: loader

**Status: done (2026-10-31).** `infra/apps/`. Acceptance results:
- Sail started and found the Glue table using Pod Identity credentials.
- Three ingestor runs into `jetstream/` wrote 11 files with 76,580 records. The four Iceberg snapshots they produced add exactly 76,580 records, and SQS drained.
- Deleting the pod while it held received messages: SIGTERM made it finish the batch and commit (21s after the delete, well inside the 120s grace period). The replacement pod found nothing left to do. A hard kill (SIGKILL or node loss) relies on the visibility timeout plus the `source_file` check and hasn't been exercised.

- **Depends on:** D1 (loader image), D2 (ECR; a manual `docker push` is enough here, CI can follow), D3.
- **Data project:** a role for the loader via `bsky_iam.role`, trusted by `pods.eks.amazonaws.com` (`sts:AssumeRole` + `sts:TagSession`), with the existing `bsky-iceberg-loader` policy attached. Export its ARN.
- **Loader Deployment:**
  - `replicas: 1`;
  - `terminationGracePeriodSeconds` above the batch window plus load time (the loader finishes its current batch on SIGTERM);
  - memory sized for embedded Sail;
  - env: queue URL from the data stack, warehouse bucket and region from config.
- **Acceptance:**
  - Logs show the embedded Sail server starting and the Glue table being found, using Pod Identity credentials with nothing injected by hand.
  - Files uploaded under the raw prefix are committed to `glue.bsky_raw.jetstream`, and SQS drains.
  - Deleting the pod mid-batch loses nothing: the messages reappear after the visibility timeout and are loaded once.

### WP D4b: ingestor

**Status: done (2026-11-02).** `Dockerfile` at the repo root (distroless/cc, with an allowlist `.dockerignore`), role `bsky-ingestor`, and Deployment `ingestor`. Results:
- `object_store` picks up Pod Identity credentials.
- At the cut-over from the laptop ingestor, the first EKS file overlapped the last local one by 4.95 s.
- Deleting the pod: SIGTERM flushes the partial batch, and the new pod resumes from it.
- **Liveness:** no probe. The read idle timeout reconnects a silent connection, and write failures exit the process (fail-fast), which Kubernetes restarts. A probe couldn't detect a stalled stream any better.
- **Testing a hard kill in the cluster isn't practical:** `kubectl delete --force` still sends SIGTERM, and PID 1 ignores SIGKILL sent from inside its own PID namespace (e.g. from an ephemeral debug container). The hard kill was tested locally instead (P1).
- **Batch size:** 100k messages and a 300 s flush interval, giving ~16 MB files about every 3.5 min instead of 1.5 MB every 20 s. Both are set in apps stack config (`ingestBatchSize`, `ingestFlushInterval`).

- **Depends on:** D4a, P1, and the ingestor image from D1.
- **Data project:** an ingestor role and policy: `s3:PutObject` on `<raw bucket>/<raw prefix>*`, plus `s3:ListBucket` and `s3:GetObject` on the prefix (needed for cursor resume).
- **Ingestor Deployment:**
  - `replicas: 1`, `strategy: Recreate`;
  - `terminationGracePeriodSeconds` of 60 or more, above one batch flush plus an S3 PUT;
  - CPU/memory requests from observed usage;
  - a liveness check: the ingestor needs an HTTP health endpoint or a heartbeat file. Add one in this WP if P1 didn't.
- **Cut-over:** stop any manually run ingestor first. Two ingestors would duplicate data; dedup handles it, but it's wasteful.
- **Acceptance:**
  - New files appear under the raw prefix, and the deployed loader commits them.
  - Deleting the ingestor pod produces overlap, not a gap: the cursor is resumed and visible in the logs.

## WP D5: pipeline jobs and maintenance

- **Depends on:** D4. Each job is added once its pipeline WP (S2, C2, E2, E3) is done.
- **Data project:**
  - Glue database `bsky_silver` (location `iceberg/bsky_silver` in the warehouse bucket).
  - Roles and policies per job, least privilege:
    - silver reads bronze and writes silver;
    - the support jobs read silver and write only their own tables;
    - the search API reads the index only.
  - An Athena workgroup (results in a separate prefix of the warehouse bucket) only if ad hoc SQL is wanted. Maintenance doesn't need it.
- **Secrets:** the TypeSafe API key.
  - Store it in AWS Secrets Manager, set once by hand or through `pulumi config set --secret`.
  - Deliver it to the classify job through the Secrets Store CSI driver or External Secrets Operator. A plain Kubernetes Secret created by Pulumi from a secret config value is the simpler option.
  - **Decide:** pick one and record it.
- **Apps project:**
  - CronJobs for `bsky-silver` (`:05` each hour), `bsky-classify` (`:20`) and `bsky-embed` (`:20`), all with `concurrencyPolicy: Forbid`, `backoffLimit: 2` and `activeDeadlineSeconds` set.
  - Classify gets its budget flags (`--max-input-tokens`, `--sample-rate`) from stack config, so spend is controlled in one place.
  - Embed gets a GPU node selector and toleration if needed.
  - The search API (E3) as a Deployment with a ClusterIP Service. External exposure is out of scope.
- **File sizes (measured 2026-11-02):** the loader writes one Iceberg data file per raw file. By then the bronze table had 2,101 files of ~1.5 MB for 20.8M records, plus a snapshot per commit. Fixes, in order:
  1. Larger raw files: done in D4b (~16 MB).
  2. **Loader:** done (2026-11-02). Each commit is repartitioned by `day`/`hour`, giving one file per hour partition, and commits are capped at 128 MB of input (`--max-load-mb`) to bound memory.
  3. **Compaction:** by the catalog, below.
- **Table maintenance: Glue's managed table optimizers** (done 2026-11-02, data stack). Compaction (binpack, `min_input_files` 10, since an hour partition gets ~17 files), snapshot retention (3 days, at least 10, deleting expired files), and orphan file deletion (3 days). All run under one role, `bsky-table-optimizer`, and log to `/aws-glue/iceberg-*/logs` with 14-day retention. Tables are listed in `optimizedTables`; add silver and support tables there once their jobs have created them.
  - **Why the catalog and not the pipeline:** maintenance stays out of the workflow code. Moving off AWS means replacing the catalog anyway, and the managed alternatives include equivalent maintenance (R2 Data Catalog has automatic compaction and snapshot expiration). So maintenance moves with the catalog, in infra.
  - **Rejected:** Athena `OPTIMIZE`/`VACUUM`, because it adds an AWS-only engine to the workflow. Iceberg's native procedures (`rewrite_data_files`, `expire_snapshots`) are portable, but Sail 0.7.2 can't run them (`CALL` is a parse error), so they would need a Spark or Trino job just for maintenance. They're the fallback for a catalog without managed maintenance.
  - **Raw bucket:** don't compact it. After loading, raw files only serve replays, so add a lifecycle rule that expires `jetstream/` objects after 7–14 days (data stack).
- **KEDA (optional):** install it as a Helm release from the apps project, with a `ScaledObject` scaling the loader on SQS `ApproximateNumberOfMessages`. Useful for backfills; skip it until a backfill needs it.
- **Acceptance:** each CronJob runs on schedule and processes its watermark range. The optimizers reduce the file count per hour partition and the snapshot count, and expired files are deleted.

## WP D6: observability, alarms and cost controls

**Status: alarms and budget done (2026-11-02)**, in the data stack: SNS topic `bsky-alerts` with an email subscription, alarms `bsky-ingest-stalled`, `bsky-loader-behind` and `bsky-dead-letters`, and the account-wide budget `bsky-monthly` ($250). Container logs in CloudWatch and job-failure alarms are still open; the latter come with D5's jobs. Raw files expire after 7 days (`rawRetentionDays`). That also bounds how far back a replay can reach.
- **Tested 2026-11-02:** scaling the loader to 0 fired `bsky-loader-behind` after 26 min, and it cleared 8 min after scaling back. Scaling the ingestor to 0 fired `bsky-ingest-stalled` after 19 min, and it cleared 4 min after scaling back; the ingestor resumed from its last file and replayed the gap. A stale `s3:TestEvent` in the DLQ fired `bsky-dead-letters`. Every email arrived.

- **Depends on:** D4 (expand as D5 adds jobs).
- **Logs:** container logs to CloudWatch (the EKS CloudWatch Observability add-on, or Fluent Bit), with 14–30 day retention.
- **Alarms (CloudWatch, in the data or platform project), sent to an SNS topic with an email subscription (address in stack config):**

  | Alarm | Signal | Meaning |
  |---|---|---|
  | Ingest stalled | raw queue `NumberOfMessagesSent` < 1 over 15 min | ingestor stopped writing (needs no ingestor metrics) |
  | Loader behind | raw queue `ApproximateAgeOfOldestMessage` > 15 min | loader stuck or down |
  | Poison messages | DLQ `ApproximateNumberOfMessagesVisible` > 0 | a file or notification failing repeatedly |
  | Job failures | Kubernetes Job failures (Container Insights metric or kube-state-metrics) | a silver, classify or embed run failed |

- **Cost:**
  - An AWS Budget with monthly alerts at 50/80/100%.
  - Classify's own `--max-input-tokens` is the control for Jev spend. Also log the daily token totals, and add a CloudWatch metric from them if spend needs watching.
  - Tag every resource with `project=bsky-data-lake` and `component=<name>` for cost allocation (Pulumi default tags on the AWS provider).
- **Acceptance:** each alarm fires in a deliberate test (stop the ingestor, stop the loader, put a malformed message in the queue, force a job failure), and the budget exists.

## Runbooks

Write these alongside D4–D6 in `docs/runbooks/`:

- deploy and roll back (image tag config + `pulumi up`);
- replay the DLQ: inspect, fix, and redrive to the main queue;
- backfill bronze from a past cursor (run a second ingestor with `--cursor` to a separate prefix, then load it);
- rebuild silver, support tables or the search index for a new `spec_version`;
- rotate the TypeSafe API key;
- tear down dev cleanly. Order matters: delete apps, then platform; the data stack holds state you want to keep.

## Costs to expect (dev, rough)

Fixed costs: the EKS control plane (~$73/month), one NAT gateway (~$32/month plus data), and small always-on nodes for the ingestor and loader. Variable costs: Jev input tokens (the C2 estimate), GPU time for embeddings if used, Glue optimizer DPU-hours, and S3 storage. Revisit after a month of running. If the semantic workloads stay small, ECS Fargate for the two always-on services is the cheaper fallback.

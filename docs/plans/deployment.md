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
| table maintenance | CronJob | nightly | Athena, Glue, warehouse S3 |

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

- **Depends on:** P1 (cursor resume) for the ingestor image to be safe to run unattended.
- **Images:**
  - **`bsky-ingest`:** multi-stage Dockerfile at the repo root. Build with `cargo build --release` on a Rust image, run on a minimal base (distroless/cc or debian-slim).
  - **`iceberg-loader`, `silver`, `enrich`:** one Dockerfile per uv project, on python-slim with `uv sync --frozen --no-dev`.
    - The loader and silver images include the Sail runtime; check that `pysail`'s wheels support the target architecture.
    - `enrich` may need a CUDA variant for `bsky-embed`, depending on E1.
- **Rules:**
  - Configuration only through env vars, which all components already support.
  - No `.env` baked into images. Add a `.dockerignore` that excludes `.env`, `target/` and `.venv/`.
  - Each image runs as a non-root user and logs to stdout.
- **Credentials:** confirm each component picks up Pod Identity credentials (`AWS_CONTAINER_CREDENTIALS_FULL_URI`).
  - boto3 and the AWS SDKs do.
  - Check `object_store`'s `AmazonS3Builder::from_env` and Sail's S3/Glue client explicitly.
  - If either only supports IRSA (web identity), use IRSA for that workload instead.
- **Architecture:** **Decide:** arm64 (Graviton, cheaper) or amd64. Pick arm64 unless a dependency lacks arm64 wheels or builds, and check `pysail` and the embedding model runtime first.
- **Acceptance:** each image builds locally, and `docker run` with exported credentials and `.env` values passed as env vars reproduces a manual run: the ingestor against `jetstream-test/`, and the loader with `--once`.

## WP D2: ECR and CI builds

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

- **Depends on:** D0.
- **VPC:**
  - `pulumi_awsx.ec2.Vpc` with 2 AZs, public and private subnets, and **one** NAT gateway (cost over HA, for dev).
  - Add gateway VPC endpoints for S3, which are free and keep lake traffic off the NAT.
  - Consider interface endpoints for SQS, ECR and STS only if NAT data charges become noticeable.
- **Cluster:**
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

## WP D4: core workloads (ingestor and loader)

- **Depends on:** D1, D2, D3, and P1.
- **Data project:**
  - Turn the loader policy document into a role (via D0's mechanism).
  - Add an ingestor policy: `s3:PutObject` on `<raw bucket>/<raw prefix>*`, plus `s3:ListBucket` and `s3:GetObject` on the prefix, needed for cursor resume.
- **Platform project:** Pod Identity associations linking each Kubernetes service account to its role.
- **Apps project** (`pulumi_kubernetes` provider built from the platform stack's kubeconfig output):
  - namespace `bsky`, plus one service account per workload;
  - **ingestor** Deployment:
    - `replicas: 1`, `strategy: Recreate`;
    - `terminationGracePeriodSeconds` of 60 or more, above one batch flush plus an S3 PUT;
    - CPU/memory requests from observed usage;
    - a liveness check: the ingestor needs an HTTP health endpoint or a heartbeat file. Add one in this WP if P1 didn't;
  - **loader** Deployment:
    - `terminationGracePeriodSeconds` above the batch window plus load time (the loader finishes its current batch on SIGTERM);
    - memory sized for embedded Sail.
  - Image tags passed in as stack config (the SHA from D2), so a deploy is `pulumi config set` + `pulumi up`, and a rollback is the same with the previous SHA.
- **Cut-over:** stop any manually run ingestor first. Two ingestors would duplicate data; dedup handles it, but it's wasteful.
- **Acceptance:**
  - New files appear under the raw prefix, the loader commits them, and SQS drains.
  - Deleting the ingestor pod produces overlap, not a gap: the cursor is resumed and visible in the logs.
  - Draining the loader's node finishes or releases its batch without losing messages.

## WP D5: pipeline jobs and maintenance

- **Depends on:** D4. Each job is added once its pipeline WP (S2, C2, E2, E3) is done.
- **Data project:**
  - Glue database `bsky_silver` (location `iceberg/bsky_silver` in the warehouse bucket).
  - Roles and policies per job, least privilege:
    - silver reads bronze and writes silver;
    - the support jobs read silver and write only their own tables;
    - the search API reads the index only.
  - An Athena workgroup with a results location in the warehouse bucket (a separate prefix, outside the Iceberg tables), plus a maintenance role.
- **Secrets:** the TypeSafe API key.
  - Store it in AWS Secrets Manager, set once by hand or through `pulumi config set --secret`.
  - Deliver it to the classify job through the Secrets Store CSI driver or External Secrets Operator. A plain Kubernetes Secret created by Pulumi from a secret config value is the simpler option.
  - **Decide:** pick one and record it.
- **Apps project:**
  - CronJobs for `bsky-silver` (`:05` each hour), `bsky-classify` (`:20`) and `bsky-embed` (`:20`), all with `concurrencyPolicy: Forbid`, `backoffLimit: 2` and `activeDeadlineSeconds` set.
  - Classify gets its budget flags (`--max-input-tokens`, `--sample-rate`) from stack config, so spend is controlled in one place.
  - Embed gets a GPU node selector and toleration if needed.
  - The search API (E3) as a Deployment with a ClusterIP Service. External exposure is out of scope.
- **Table maintenance CronJob (nightly):**
  - for each Iceberg table, run Athena `OPTIMIZE <table> REWRITE DATA USING BIN_PACK` (scoped to recent partitions) and `VACUUM <table>`, which expires old snapshots and removes orphan files;
  - set the snapshot retention properties on each table at creation.
  - The ingestor writes ~3,000 small files a day, so this matters within weeks.
  - **Decide:** if Sail gains Iceberg's `rewrite_data_files` and `expire_snapshots` procedures, compare it with Athena; otherwise Athena it is. Confirm Athena can OPTIMIZE tables written by Sail (format version and properties).
- **KEDA (optional):** install it as a Helm release from the apps project, with a `ScaledObject` scaling the loader on SQS `ApproximateNumberOfMessages`. Useful for backfills; skip it until a backfill needs it.
- **Acceptance:** each CronJob runs on schedule and processes its watermark range. Maintenance reduces the file count in a day partition, and VACUUM removes expired snapshot files.

## WP D6: observability, alarms and cost controls

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

Fixed costs: the EKS control plane (~$73/month), one NAT gateway (~$32/month plus data), and small always-on nodes for the ingestor and loader. Variable costs: Jev input tokens (the C2 estimate), GPU time for embeddings if used, Athena maintenance scans, and S3 storage. Revisit after a month of running. If the semantic workloads stay small, ECS Fargate for the two always-on services is the cheaper fallback.

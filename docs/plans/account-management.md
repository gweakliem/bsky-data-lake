# Account management plan: costs and account structure

## Goal

Keep the AWS bill understood and as low as the workloads allow, and bring the account up to current practice. The account is old and predates several of AWS's current recommendations. Unlike the other plans, most of this is account setup and housekeeping rather than code. Infrastructure changes still go through Pulumi under `infra/`, with a preview before each apply.

## Where the money goes

Cost Explorer, month to date on 2026-11-11 (Nov 1–10, $108.40):

| Usage type | Nov 1–10 | Monthly pace | Notes |
|---|---|---|---|
| EKS cluster hours | $23.78 | ~$72 | fixed per cluster |
| Glue optimizer DPU-hours | $21.66 | ~$65 | compaction and other table maintenance (M2) |
| EC2 nodes (m5a.large, c6a.large, m5a.xlarge) | $21.16 | ~$64 | EKS Auto Mode adds a management fee on top ($1.77) |
| NAT gateway hours | $10.75 | ~$33 | fixed (M1) |
| NAT gateway bytes (190 GB) | $8.96 | ~$27 | about 19 GB a day, most likely the ingestor's Jetstream stream (unverified) (M1) |
| CloudWatch vended logs (16.4 GB) | $5.92 | ~$18 | Fluent Bit container logs, 14-day retention |
| Regional data transfer (375 GB) | $4.06 | ~$12 | traffic between availability zones |

S3 traffic from the cluster already skips NAT through the VPC's S3 gateway endpoint, which is free.

The account budget `bsky-monthly` is $250 (deployment plan, D6).

**Coming costs:** E2's embeddings are about $40 a month at OpenRouter. If downloaded through the managed NAT gateway, their responses would add about $25 a month (base64 at 4,096 dims, about 63 GB a day), or about $155 a month as JSON floats. The data-pipeline plan's E2 section has the measurements.

## WP M1: replace the managed NAT gateway with a NAT instance

- **Status: proposed (2026-11-11).**
- **Why:** the managed NAT gateway costs about $33 a month in hours plus $0.045 for every GB it processes. A NAT instance has no per-GB processing charge, and AWS doesn't charge for data coming in from the internet. That removes both NAT lines (about $60 a month now) and the embed job's download cost, for a few dollars a month.
- **How:** in the platform stack.
  - An [fck-nat](https://fck-nat.dev/) AMI (an Amazon Linux image set up as a NAT) on a small Graviton instance, in an Auto Scaling group of one so a failed instance is replaced.
  - A fixed network interface and Elastic IP that the replacement instance takes over, so the private route tables (`0.0.0.0/0`) keep pointing at the same interface. The current Elastic IP can carry over, so the egress address doesn't change.
  - Source/destination check off on the interface.
- **Decide:**
  - **Instance size.** t4g.nano has a baseline of about 32 Mbit/s with bursts above it. Jetstream averages about 2 Mbit/s and the embed job's downloads add bursts. Pick t4g.nano or t4g.micro from measured peak throughput, not the average.
  - **Availability:** one instance in one zone, as the managed gateway is today (cost over HA for dev). A replacement takes a few minutes. The ingestor reconnects and resumes from its cursor, so that is a gap in the ingestor, not lost data.
- **Cut-over:** add the instance, then switch the route tables, then delete the managed gateway in the same apply or the next. Watch `bsky-ingest-stalled` and the loader alarms.
- **Acceptance:**
  - The cluster's outbound traffic flows through the instance: the ingestor writes files, ECR pulls work, and the OpenRouter and TypeSafe APIs are reachable.
  - Deleting the instance brings a replacement up and traffic resumes without editing routes.
  - The `NatGateway-Hours` and `NatGateway-Bytes` lines drop to zero in Cost Explorer.

## WP M2: Glue optimizer cost

- **Status: not started.**
- **Why:** at about $65 a month, table optimizers are the second-largest line, more than the nodes. They are configured as `optimizedSilverTables` in the data stack (see the silver job's notes on commit conflicts).
- **Look at:**
  - DPU-hours by table and by optimizer type (compaction, snapshot retention, orphan file deletion).
  - Whether compaction runs more often than the tables change, since silver commits hourly.
  - Whether some tables need only snapshot retention and not compaction, for example small tables, or `likes` once S4 settles its layout.
  - What the alternative would cost: a scheduled compaction job in the cluster, using node time that is already paid for.
- **Acceptance:** a recorded decision per table, and the monthly pace of the line after the change.

## WP M3: billing access and cost visibility

- **Status: partly done.** The cost allocation tags were activated on 2026-11-10. The developer permission set can already read Cost Explorer through the API (`aws ce`), since `PowerUserAccess` allows `ce:*`. The billing console pages are used from root.
- **Steps:**
  1. As root, once: *Account → IAM user and role access to Billing information*, turned on.
  2. In IAM Identity Center, a permission set `BillingReadOnly` (AWS managed policy `AWSBillingReadOnlyAccess`) assigned to the developer. Cost work is then done with that permission set instead of root or the developer role.
  3. Root is kept for the tasks only root can do.
- **Ongoing:**
  - Review Cost Explorer grouped by the `stack` and `component` tags monthly.
  - API spend outside AWS (OpenRouter, TypeSafe) is logged per run by the jobs. Each provider's key carries its own credit limit.
- **Acceptance:** billing pages open with `BillingReadOnly`, and nothing billing-related needs root.

## WP M4: account hygiene

- **Status: not started.**
- **Why:** an old account often has leftovers from before current defaults: root access keys, IAM users with long-lived keys, resources in other regions, logging that was never turned on.
- **Check:**
  - Root has MFA and no access keys (IAM credential report).
  - IAM users and their access keys: last used, and whether IAM Identity Center can replace them.
  - Resources in every region, not just us-west-2. Old instances, volumes, snapshots, Elastic IPs or buckets cost money and widen exposure.
  - CloudTrail: an organization or account trail exists and logs to a bucket outside the workloads' reach.
  - IAM Access Analyzer for resources shared outside the account, especially S3 bucket policies.
  - S3 Block Public Access at the account level.
- **Acceptance:** the findings are recorded here, each fixed or with a reason it stays, and nothing new costs money without a decision (GuardDuty and Security Hub are optional and billed).

## WP M5: move the workloads out of the management account

- **Status: in progress (2026-11-16). Option 2 chosen:** a member account for the data lake. Phases A–C are done. Phase D (the new account ingesting alongside the old) has run since T0, 2026-11-16 17:34Z. Phase E (freeze, copy, switch) is planned for the evening of 2026-11-17, Mountain time. Nothing has been copied yet, and the old pipeline still runs unchanged.
- **Why:** the data lake runs in the AWS Organization's management account, the account that owns the organization and pays the bill. AWS recommends keeping the management account free of workloads:
  - Service control policies (SCPs, the organization-wide guardrails) don't apply to the management account, so they can't limit what its roles do.
  - A credential leaked from a workload in the management account sits next to control of the whole organization: accounts, billing and IAM Identity Center.
  - Costs and permissions are cleaner when each account holds one thing.
  - It gets harder to move as the lake grows, which is why it goes ahead of other work.
- **Options considered:**
  1. Leave it, with compensating controls (permissions boundary, M4, least-privilege roles).
  2. **New member account for the data lake (chosen).**
  3. Cross-account data in place: compute in a member account, buckets left in the management account. Rejected because the workloads would still depend on the management account.

### Target layout

- **Management account:** Organizations, IAM Identity Center, consolidated billing, the organization-wide budget, cost allocation tags and (M4) CloudTrail. No workloads.
- **OU `Workloads`:** one account per project, so the bill splits by linked account without tags. `bsky-data-lake` is the first. A later production environment gets its own account (`Workloads/Prod`).
- **SCP on `Workloads`:** deny `organizations:LeaveOrganization`, and deny every action by the root user (`aws:PrincipalArn` like `arn:aws:iam::*:root`). `FullAWSAccess` stays attached. A region restriction is deferred, because it needs exceptions for global services.
- **Root access management** is centralized in the management account, so member accounts have no root credentials. Root access is not delegated to a member account: there is no security account to delegate to.
- **Account IDs stay out of the repo,** as before. They live in `~/.aws/config` and `.env`.

### Phase A: account and access (done 2026-11-16)

- Member account `bsky-data-lake` created in OU `Workloads`, tagged `project=bsky-data-lake`.
- `infra/iam-bootstrap/` applied in the new account:
  - the boundary `bsky-workload-boundary`;
  - a separate permission set `BskyDataLakeDeveloper-Workloads` (`PowerUserAccess` plus the inline policy rendered for the new account), since the existing permission set's inline policy names the management account's ID.
- Verified with the new developer profile: creating a role under `/bsky/` with the boundary succeeds. Creating one without the boundary, creating one outside `/bsky/`, removing a role's boundary and editing the boundary are all denied.
- A temporary `AdministratorAccess` assignment on the new account was used for the bootstrap. **Removed 2026-11-16 (verified).** A local SSO profile that asks for `AdministratorAccess` now signs in but gets `ForbiddenException: No access`. Re-assign the permission set in Identity Center for a one-off admin task, then remove it again. Day-to-day work uses `BskyDataLakeDeveloper-Workloads`.
- **Still to confirm in the console:** SCP attached; root access management on; billing access for IAM activated (M3 step 1); cost allocation tags `project`, `stack` and `component` active.

### What can't simply be copied

- **Bucket names change.** The buckets are in S3's account-regional namespace (`<prefix>-<account>-<region>-an`), so the new account's names follow from its ID. The data stack derives them (`createBuckets`), and nothing secret goes in config. They still differ from the old names, which is what matters below. The Pulumi state bucket follows the same pattern.
- **Iceberg metadata holds absolute paths.** Every metadata file, manifest list and manifest names its files as `s3://<warehouse>/iceberg/...`. Copying the objects into a bucket with a new name leaves tables pointing at the old bucket.
  - Iceberg's `rewrite_table_path` procedure fixes this, but it needs Spark with Iceberg 1.8 or later, and Sail can't run `CALL`.
  - **So tables are re-written, not copied.** A copy job reads each old table and appends its rows to a new table of the same schema and partitioning in the new account, day by day. Snapshot history isn't carried over, which nothing here needs. Every row keeps its values, including `event_id`, `bronze_ingested_at` and the run-state tables' watermarks.
- **Lance stores relative paths** inside a dataset (verified in phase C), so the search index can be copied as plain objects.
- **EKS, VPC, NAT, IAM roles, queues, alarms and log groups** are created fresh from the stacks. Nothing is moved.

### Sizes (2026-11-16)

| What | Size | Plan |
|---|---|---|
| Raw bucket (`jetstream/`, 7-day expiry) | 41 GB | not copied: the new ingestor fills its own |
| Warehouse `iceberg/` (bronze `jetstream` and 15 silver tables) | ~250 GB | re-written table by table |
| Warehouse `lance/` (search index) | 415 GB, 154 GB after the version cleanup | copied |
| Warehouse `reports/` | small | copied |
| Warehouse `lance-test/` (scratch) | — | not copied |

Copies within one region have no transfer charge, only request charges. Both accounts store the data during the overlap, about $15 a month for the duplicate 665 GB, prorated. The two clusters overlap too, at about $5 a day.

With `lance/` at 154 GB, the duplicate is about 400 GB, roughly $9 a month while both accounts hold it.

~~**415 GB for 37M rows looks large.**~~ Resolved: most of it was unreferenced index files from old versions (264 of 274 GB of index files). With 2 hours' version retention (`1e818aa`), `lance/` went to 154 GB.

### Phase B: make the stacks account-neutral (code, no applies; done 2026-11-16)

- **New stack name `main`** in all three projects (`Pulumi.main.yaml`). A second `dev` in the new backend would share `Pulumi.dev.yaml` with the old stacks, including image tags and the encryption salt. `StackReference`s default to the current stack's name, so they follow. The old `dev` stacks stay in the old backend until phase F destroys them.
- **Data stack, `createBuckets`:**
  - creates the raw and warehouse buckets instead of importing them, named from the account ID, with `protect` and `retain_on_delete` either way;
  - adds a public access block on each new bucket;
  - makes the notification and lifecycle resources refer to the bucket resources, so they wait for a created bucket;
  - S3's defaults for a new bucket match the old buckets (unversioned, SSE-S3, ACLs disabled).
  - Checked with mocks in a separate process, since the existing tests load the stack once with the old account's settings: the names come from the account ID, `.env`'s old names are ignored, and the notification targets the new raw bucket.
- **Apps stack, stand-up switches:** `ingestReplicas`, `loaderReplicas` (0 or 1), `silverSuspend` and `reportsSuspend`, next to the existing `embedSuspend`, `labelsSuspend`, `indexSuspend` and `searchReplicas`. `Pulumi.main.yaml` starts everything stopped, with the `dev` stack's image tags.
- **Alarms in the new account:** the embed and labels alarm actions start off. The ingest, loader and silver alarms will be in alarm while everything is stopped. The new SNS email subscription stays unconfirmed until phase D, so nothing is delivered before then.
- **Budget:** stays in the data stack. In the new account it watches that account's spend. The old account's `bsky-monthly` covers the whole organization, since a management account's budget includes linked accounts. When phase F destroys the old data stack, recreate an organization-wide budget in the management account by hand.
- **Nothing else is tied to the account.** In the cluster, the queue URL, account ID and locations come from stack outputs. On the laptop they come from `.env` (`BSKY_LOADER_QUEUE_URL`, `BSKY_AWS_ACCOUNT_ID`, bucket names), which switches to the new values at phase E.
- **Not done in phase B, waiting on decisions:**
  - ~~the NAT instance (M1) in the new platform stack;~~ added in phase C;
  - the GitHub OIDC provider and CI role (D2). The developer inline policy already allows the provider by its exact ARN, so either can be added before or after phase C.
### Phase C: stand up the new account, empty

- **State backend first:** create `pulumi-state-<new account>-us-west-2-an` in the new account (by hand, like the original, since Pulumi can't create its own backend). Then `pulumi stack init main` in each project, with `PULUMI_BACKEND_URL` pointing at it and the same passphrase file.
- Apply data → platform → apps in the new account with the `bsky-data-lake` profile, each after a preview and a yes.
- **Apps stack:**
  - **Nothing scheduled runs yet:** ingestor at 0 replicas, every CronJob suspended.
  - **Images:** copy the current tags from the old ECR to the new one (for example `crane copy`), rather than rebuilding, so the same code runs.
- **Cross-account read:** a bucket policy on the old warehouse bucket (old data stack, a Pulumi change in the management account) lets the new account's copy role read `iceberg/`, `lance/` and `reports/`. It is read-only, and removed in phase F.
- **New EKS endpoint and kube context:** `BSKY_EKS_PUBLIC_CIDRS` and a new kubeconfig entry. The reports NLB gets a new URL, so tell the demo users.
- **SNS:** confirm the new alert email subscription.
- **Lance check:** copy `lance-test/` to the new warehouse and open it there with the search CLI. If it opens and answers queries, the relative-path assumption holds.

- **Progress (2026-11-16):**
  - **State bucket:** created, with versioning (old versions expire after 30 days), unlike the original. The `main` stacks are initialised in it.
  - **Data stack applied:** 73 resources.
  - **Platform stack applied:** 59 resources, with the fck-nat NAT instance (M1) instead of the managed gateway. Both EKS add-ons are active and `kubectl top` works. The kube context is `bsky-main`.
  - **Images:** the 7 current tags are copied to the new ECR with `docker buildx imagetools create`.
  - **Apps stack applied:** 37 resources, with nothing writing. Fluent Bit, event-exporter and reports-web (a new NLB URL) run; the ingestor, loader and search are at 0, and every CronJob is suspended.
  - **Copy job (`migrate/`, see its CLAUDE.md):** re-writes each table from its metadata file one partition at a time, resumably, and copies `lance/` and `reports/inputs/` server side. It reads the old warehouse through a read-only bucket policy (old data stack, `migrationReadPolicy`), so the old Glue catalog isn't needed. The role `bsky-migration` (new data stack, `migrationRole`) writes the new warehouse and catalog.
  - **Lance check passed:** `lance-test/` (89 objects, 552 MB) copied in 4 s and opened in the new bucket with the same rows (105,221), version, 6 indexes and query results. So `lance/` is copied, not rebuilt.
  - **Old tables (2026-11-16):** no delete files; one partition spec and one schema each. Bronze is partitioned by `day` and `hour`, silver by `day`; `silver_runs` and `post_edges_backfill` are unpartitioned.
  - **Copy job deployed:** the platform's `bsky/migrate` repository and Pod Identity association are applied, and the image is pushed (`a706bb7`, amd64 and arm64).
- **What the stand-up turned up:**
  - **New-account EC2 validation:** a new account can't launch instances until AWS validates it (minutes, up to 4 hours, with an email). The first platform apply failed on the NAT instance and EKS add-ons until it cleared.
  - **fck-nat's interfaces:** traffic comes in on the fixed interface (`eni_id`) and leaves masqueraded through the instance's own interface. fck-nat moves the Elastic IP there (`eip_id`) and turns off that interface's source/destination check, so its role needs `ec2:ModifyNetworkInterfaceAttribute` on network interfaces and `ec2:AssociateAddress` on the Elastic IP. With the Elastic IP on the fixed interface instead, return traffic was dropped and nothing in the private subnets reached the internet.
  - **Instance profiles:** the developer policy needed `instance-profile/bsky/*` rights (`iam-bootstrap/developer-inline-policy.json`) before the NAT instance could have one.
  - **Glue table optimizers** fail on tables that don't exist yet, so `main` starts with none (`optimizedTables: []`).
  - **Changing a role's description** needs `iam:UpdateRoleDescription`, which the developer policy doesn't grant. Set descriptions right the first time.
  - **EC2 interface descriptions** reject apostrophes.

### Phase D: start ingest in the new account (T0)

- Scale the new ingestor to 1 and start the loader. The new bronze fills from T0.
- **The old pipeline keeps running unchanged.** Both ingestors read Jetstream independently, so there is no gap and no shared state.
- Watch the new account's `bsky-ingest-stalled` and loader alarms for a day.

- **Progress:**
  - **Started 2026-11-16: T0 = 17:34:44 UTC,** when the new ingestor connected (live, no file to resume from). The first raw file and bronze commit arrived by 17:38Z. The loader created `bsky_raw.jetstream` in the new catalog.
  - **SNS confirmed 2026-11-16.** CloudWatch notifies only on a state change. Alarms that were already in ALARM before the subscription (silver stalled; embed, index and labels with actions off) send nothing until they recover and fire again.
  - **Check at 2026-11-17 01:18Z (T0 + 7¾ h):** in `bsky-main`, the ingestor and loader had run 7h43m with no restarts. The loader was appending one 16–17 MB file to `bsky_raw.jetstream` about every 4 minutes, with none already loaded.

### Phase E: freeze, copy overnight, switch (after about a day of phase D)

Run across a night, Mountain time: steps 1–3 in the evening, the copy while nobody's watching, steps 4–7 the next morning. The copy runs as Jobs in the new cluster; the laptop is only needed to start them.

1. **Freeze the old downstream jobs** (`--context bsky-dev`): suspend the old silver, embed, index, labels and reports CronJobs once each has finished its current run. The old ingestor and loader keep running. Note the old silver's last `bronze_through` (W).
2. **Write the plan** with the old account's credentials, right after the freeze: `bsky-migrate plan --dest-warehouse <new warehouse> --exclude bsky_silver.post_edges_backfill` (a temporary table, dropped). It pins each table's current metadata file. Then apply the new apps stack with `BSKY_MIGRATION_PLAN_FILE` set, which adds the migrate CronJobs.
3. **Copy** (`migrate/CLAUDE.md`): start `migrate-tables` and `migrate-objects`. They write different prefixes, so they can run at the same time.
   - **Tables:** every silver table, including `silver_runs` and the other jobs' state, and **all of bronze**, as of the plan (decided 2026-11-16).
     - **Why no cut-off at T0:** silver's position is a bronze `ingested_at`. The two loaders stamp the same event minutes apart, so cutting bronze at T0 would lose the events stamped after W by the old loader and before W by the new one.
     - **With all of bronze:** the new silver resumes at W and reads both copies of every later event, and its anti-join on `event_id` keeps one, as after any ingestor restart. Embed and index follow, since the new silver rows all come after W.
     - **The cost:** bronze holds the T0-to-freeze day twice. Accepted while in development.
   - **Objects:** `lance/` (smaller after the 2-hour version retention, `1e818aa`) and `reports/inputs/`.
4. **Verify:** `migrate-verify` compares row counts per partition, and each job's state table shows the same position as the old one.
5. **Switch the new data stack to running:**
   - restore the optimizer lists (drop `optimizedTables: []` and `optimizedSilverTables: []` from `Pulumi.main.yaml`);
   - turn on `embedAlarmActions`, `labelsAlarmActions` and `indexAlarmActions`.
6. **Unsuspend the new CronJobs** in pipeline order: silver, embed, index, labels, reports; then search to 1 replica. Each continues from its copied position, with no adjustment.
7. **Stop the old ingestor and loader.**

**Rollback until step 7:** unsuspend the old CronJobs. The old account has missed nothing, because its ingest never stopped.

### Phase F: decommission the old account's workloads (about two weeks after E)

- `pulumi destroy` the old apps and platform stacks, from the old backend.
- Keep the old buckets read-only for the two weeks, then remove the cross-account policy and delete them. The data stack's `protect`/`retain_on_delete` means the old buckets are deleted by hand, on purpose.
- Remove the old Glue databases, queues, alarms, log groups, ECR repository and the old state bucket.
- Remove the old `BskyDataLakeDeveloper` assignment on the management account. The developer then has no workload access there.
- The management account is left with only Organizations, IAM Identity Center, billing, the budget and CloudTrail (M4).

### Decide

- ~~**`lance/`:** copy it or rebuild it.~~ Copy: phase C's check passed. Its 415 GB was mostly a day of old versions' index files; with 2 hours' retention (`1e818aa`) the first run took it to 154 GB in 1,903 objects (from 9,517), 2026-11-16 18:40Z.
- ~~**NAT.**~~ The new platform stack runs the fck-nat instance (M1).
- ~~**`post_edges_backfill`:** copy it or drop it.~~ Dropped: a temporary table.
- ~~**Overlap length.**~~ About a day, then phase E across a night, Mountain time.

### Acceptance

- Every stack is deployed in the member account, and every job runs there on schedule, with its alarms.
- Per-table row counts match across the freeze, and no day has a gap in silver.
- Search, reports and the clustering inputs work from the new account.
- The old workloads and buckets are deleted, and the management account holds only Organizations, IAM Identity Center, billing and CloudTrail.

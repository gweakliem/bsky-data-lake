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

- **Status: proposed; decision needed.**
- **Why:** the data lake runs in the AWS Organization's management account, the account that owns the organization and pays the bill. AWS recommends keeping the management account free of workloads:
  - Service control policies (SCPs, the organization-wide guardrails) don't apply to the management account, so they can't limit what its roles do.
  - A credential leaked from a workload in the management account sits next to control of the whole organization: accounts, billing and IAM Identity Center.
  - Costs and permissions are cleaner when each account holds one thing.
- **Options:**
  1. **Leave it, with compensating controls.** Keep the developer role's permissions boundary (`infra/iam-bootstrap`), finish M4, and keep workload roles least-privilege. This costs nothing, and is reasonable while one person runs the project.
  2. **New member account for the data lake.** Create an account in the organization, bootstrap IAM there (`infra/iam-bootstrap`), and point the Pulumi stacks at it. Then move the data:
     - S3: copy the raw and warehouse buckets. Copies within a region pay request charges, not transfer.
     - Glue: re-register the Iceberg tables in the new account's catalog, pointing at the copied locations.
     - EKS, VPC and IAM: create fresh from the stacks rather than move.
     - Cut over the ingestor last, so the gap is covered by cursor resume.
  3. **Cross-account data in place.** Leave the buckets where they are and run compute in a member account with cross-account bucket and Glue access. This moves less data, but the workloads still depend on the management account, which defeats most of the purpose.
- **Decide:** option 1 or 2, and when. Option 2 is easiest before the lake grows further. The warehouse was 136 GB of silver on 2026-11-10, plus bronze.
- **Acceptance for option 2:** every stack deployed in the member account, the jobs running there, the old resources deleted from the management account, and the management account left with only Organizations, IAM Identity Center, billing and CloudTrail.

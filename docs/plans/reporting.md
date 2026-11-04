# Reporting plan: dashboards over silver

## Goal

Fixed reports over the silver tables (pipeline health, activity per day, languages), kept in the repo as code, built on a schedule in the cluster and viewed privately through `kubectl port-forward`. The query layer must not tie reporting to AWS: moving the lake to another cloud should change a connection setting, not the report SQL.

Out of scope for now: ad hoc exploration (Athena's `bsky` workgroup covers that, see the [runbook](../runbook.md#athena)), public sharing, per-user access control, and live (query-at-view-time) dashboards.

## Layout

```
glue.bsky_silver.*  (Iceberg, S3)
        │  Iceberg REST catalog (Glue today; R2 Data Catalog or BigLake later)
        ▼
DuckDB in the Evidence build ── SQL in reports/sources/ and pages/*.md
        │  hourly CronJob `reports`, :30
        ▼
static site (HTML + JS + extracted Parquet) ──► nginx pod ◄── kubectl port-forward
                                                                 │
                                                  browser: DuckDB-WASM for filters
```

New code:

| Directory | Contents | Engine |
|---|---|---|
| `reports/` | Evidence project (Node): DuckDB source, Markdown pages, Dockerfile | Evidence + DuckDB 1.4 |

## Decisions

### Tool: Evidence (evidence.dev)

Evidence is static-site BI as code: Markdown pages with SQL blocks and chart components, built into a static site.
- **Dashboards live in the repo**, reviewed and versioned like everything else.
- **Nothing is queried at view time.** The build runs each source query and ships the results with the site. Interactivity (filters, dropdowns) runs in the browser on DuckDB-WASM over that snapshot. Freshness is the build schedule, which suits reports over hourly silver.
- **No always-on application:** no app database, nothing running beyond a static file server.

**Rejected:**
- **Metabase:** needs an application database (Neon and Supabase were considered) and an always-on JVM pod, and click-built dashboards live outside the repo.
- **Superset:** heavier than needed for a handful of fixed reports.
- **QuickSight:** strongly disliked, and AWS-only.
- **Grafana:** time-series oriented; tables and top-N lists are awkward.
- **Streamlit:** live queries and an always-on pod for what are mostly fixed reports.

**Risk: project activity.** Evidence's last release (`@evidence-dev/evidence` 40.1.8) was on 2026-02-06, about nine months ago. Watch it. The fallback is **Observable Framework** (data loaders in any language, Observable Plot), which keeps the same architecture: a static build, DuckDB for the queries, pages versioned in the repo. Keeping the source SQL in plain `.sql` files (R2) means such a move mostly rewrites page markup, not queries.

### Query engine: DuckDB over the Iceberg REST catalog

DuckDB reads the Iceberg tables directly through the catalog's Iceberg REST endpoint:

```sql
CREATE SECRET (TYPE s3, PROVIDER credential_chain, REGION '<region>');
ATTACH '<account-id>' AS glue (TYPE iceberg, ENDPOINT_TYPE glue);
SELECT ... FROM glue.bsky_silver.silver_runs;
```

The account ID and region come from the environment at build time, never from a committed file.

- **Portability:** Glue, Cloudflare R2 Data Catalog and Google BigLake all speak Iceberg REST. Moving clouds changes the `ATTACH` endpoint and the credential `SECRET`, not the report SQL.
- **Version:** Evidence's DuckDB connector (`@evidence-dev/duckdb` 2.0.1) depends on `@duckdb/node-api ^1.4.2`, i.e. DuckDB 1.4. The tests so far used Python DuckDB 1.4.5.
- **Rejected: Athena as the engine.** It works well (about 2 s for both test queries below, and 0 bytes scanned for a count answered from metadata), but it entangles reporting with AWS, and Evidence has no Athena connector anyway. Athena stays for ad hoc queries.

### Data source: silver only

Reports read `bsky_silver.*`, never bronze's raw JSON payloads. Pages aggregate in SQL: the extracted results ship to the browser, so each query's result must stay small (hundreds to low thousands of rows, never raw records).

### Build and hosting

- **Build:** a CronJob `reports` in the `bsky` namespace (apps stack) runs the Evidence build hourly after silver, at `:30` (silver runs at `:05` and takes about 5 minutes once caught up). On demand: `kubectl -n bsky create job --from=cronjob/reports reports-manual-$(date +%s)`.
- **Serving:** private. Access is by `kubectl port-forward` (decided). No public endpoint, no Ingress, no load balancer. The build writes the site somewhere a small nginx pod serves it from; where is decided in R3.

### Identity

A dedicated read-only role, rather than borrowing a pipeline role:

| Stack | Resource |
|---|---|
| data (`infra/`) | role `bsky-reports` via `bsky_iam`, trusted by `pods.eks.amazonaws.com`: S3 `GetObject` and `ListBucket` on `iceberg/bsky_silver/*` in the warehouse bucket; Glue `GetCatalog`, `GetDatabase(s)` and `GetTable(s)` on the catalog and `bsky_silver` |
| platform (`infra/platform/`) | Pod Identity association for service account `reports` |
| apps (`infra/apps/`) | service account `reports`, the CronJob, and the serving Deployment + ClusterIP Service |

Changes go through Pulumi, in the order data → platform → apps.

## Measurements so far

- **From a laptop outside AWS**, DuckDB 1.4.5 through the Glue Iceberg REST catalog: `silver_runs` totals took 34.6 s and posts per day 47.9 s. The same queries in Athena: 1.8 s and 2.3 s.
- **Cause:** silver commits once per window to every table, so the tables are many small files and snapshots (for example `silver_runs`: 818 S3 objects for 203 rows). DuckDB reads Iceberg metadata sequentially, and the laptop was far from the region.
- **Fix applied (2026-11-04):** Glue table optimizers (compaction, snapshot retention of 3 days, orphan-file deletion) now cover all 11 silver tables (`optimizedSilverTables` in the data stack).
  - **Open:** whether Glue's optimizers also rewrite manifests. If they don't, the manifest count keeps growing with commits, and silver may need fewer, larger commits. R1 measures this.
- **In the cluster** (throwaway pod): DuckDB's `credential_chain` picked up EKS Pod Identity credentials directly, with no boto3 shim. The `ATTACH` then got HTTP 403 from Glue's Iceberg REST `/iceberg/v1/config`, because the test borrowed the silver job's role, which lacks `glue:GetCatalog`. The `bsky-reports` role fixes that. In-cluster timings are still to be measured.

## WP R1: access and in-cluster spike

- **Depends on:** nothing open (silver exists, optimizers applied).
- **Goal:** confirm DuckDB in the cluster is fast enough to build reports, using the role reports will actually run with.
- **Tasks:**
  - **Data stack:** role `bsky-reports` and its policy as in [Identity](#identity), through `bsky_iam`. Add a mock test that it is read-only (no S3 writes, no Glue writes).
  - **Platform stack:** Pod Identity association for `bsky/reports`.
  - **Apps stack:** service account `reports` (the CronJob comes in R3).
  - **Spike:** a throwaway pod as `reports` with DuckDB 1.4 (Python or the CLI, matching the version Evidence uses). Run the two test queries (`silver_runs` totals, posts per day) plus one `likes` aggregate, cold and warm, and record:
    - wall time per query, and how much of it is `ATTACH` and metadata;
    - S3 objects per table before and after the optimizers' first runs;
    - whether the optimizers rewrite manifests (manifests per snapshot over a day).
- **Proposed thresholds** (adjust in this WP):
  - each test query under 10 s in the cluster;
  - a full build of the R2 pages under 5 minutes, so an hourly build fits comfortably in its slot.
- **Decide:** if the thresholds are missed after compaction, which lever comes first:
  - fewer, larger silver commits (a larger `--max-rows`, or one commit per run instead of per window);
  - a small pre-aggregated table written by silver (e.g. daily counts), which reports read instead of scanning `posts`;
  - DuckDB settings (metadata caching, `threads`), if they help.
- **Deliverable:** results in `docs/plans/notes/reports-spike.md`.
- **Acceptance:**
  - `ATTACH` succeeds from a pod running as `reports`, with nothing injected by hand.
  - The same pod is denied a write (e.g. `CREATE TABLE` in `bsky_silver`, or an S3 `PutObject`).
  - The timings are recorded and meet the thresholds, or the chosen lever is recorded.
  - The Pulumi mock tests pass.

## WP R2: reports project

- **Depends on:** R1 for the in-cluster role, though development can start in parallel against the lake from a laptop. S3 for the content pages (below), not for the health page.
- **Goal:** a `reports/` Evidence project that builds locally, against a fixture and against the lake.
- **Tasks:**
  - Scaffold an Evidence project in `reports/` (Node; pin Evidence and `@evidence-dev/duckdb`, commit the lockfile). Add a `reports/CLAUDE.md` with commands and gotchas, as the other subprojects have.
  - **Source:** a DuckDB source that runs the `CREATE SECRET` and `ATTACH` above, with the account ID and region from env vars (add them to `.env.example`). Source queries are `.sql` files, one per dataset, each aggregating.
    - **Decide:** how the source runs the `ATTACH`: through the DuckDB connector's initialization, or a prelude in each source query. Pick whichever the connector supports cleanly, and record it.
  - **Pages:**
    - **Pipeline health** (`silver_runs`): windows and bronze rows per hour, appended rows per table (from `counts`), and the lag of `bronze_through` behind each run's `finished_at`; plus rejects per day. It reads only silver's own run and reject tables, so it doesn't wait for S3.
    - **Activity:** posts, likes and reposts per day.
    - **Languages:** top languages per day, from `langs` (unnested), with a date-range filter.
  - The content pages (activity, languages) read **`posts_current`** from WP S3 (in progress), not raw `posts`, so edits and deletes are handled once, in one place. Until S3 lands they can be prototyped on `posts`, but don't merge them that way.
  - **Local build:** document in `reports/CLAUDE.md` how to build against the lake from a laptop (export SSO credentials as in the repo `CLAUDE.md`, then `npm run sources && npm run build`), and that it is slow from outside the region (see [Measurements](#measurements-so-far)).
  - **Tests:** a build against a small local fixture (Parquet or a DuckDB file with the silver schema, no AWS access), so CI can check that pages compile and queries run.
    - **Decide:** whether one source switches between the lake and the fixture by env var, or the fixture is a second source.
- **Acceptance:**
  - `npm run build` produces the site from the fixture with no AWS access.
  - A local build against the lake produces all three pages, with numbers matching the same queries in Athena.
  - No page ships more than a few thousand rows to the browser (check the sizes of the built data files).

## WP R3: deploy

- **Depends on:** R1, R2.
- **Image:** `reports/Dockerfile` on a Node base, running `npm run sources && npm run build` as a non-root user. Multi-arch, pushed to a new ECR repo `bsky/reports` (platform stack), tagged with the short commit hash like the other images.
- **CronJob `reports`** (apps stack): `:30` hourly, service account `reports`, `concurrencyPolicy: Forbid`, `backoffLimit: 2`, `activeDeadlineSeconds` under an hour. Region and account ID from config, image tag from `reportsImageTag`.
- **Serving:** a Deployment `reports-web` (nginx, one replica) with a ClusterIP Service.
  - **Decide:** where the built site lives:
    - **S3 prefix + sync (default):** the build uploads the site to a `reports/` prefix in the warehouse bucket. That needs `PutObject` on that prefix only, as a separate statement of the `bsky-reports` policy, and R1's read-only test then covers `iceberg/` only. nginx serves a local copy that a sidecar refreshes from S3. It survives pod restarts and keeps the last good build when a build fails.
    - **PVC:** the build writes to a volume nginx mounts. The simplest data path, but it needs a `ReadWriteMany` volume, or the build and nginx pinned to one node with `ReadWriteOnce`.
    - **The build image serves its own output:** no shared storage, but it becomes a long-running pod rebuilt on a schedule, and a failed build leaves nothing to serve.
  - Whichever is chosen, a failed build must leave the previous site in place.
- **Access:**

  ```bash
  kubectl -n bsky port-forward svc/reports-web 8080:80
  # then open http://localhost:8080
  ```

- **Runbook:** on deploy, add a "Reports" section to `docs/runbook.md`: what runs (CronJob `reports`, Deployment `reports-web`), opening the site, running a build now, build logs (`/bsky/containers`, stream prefix `reports-`), and `bsky/reports` in "Deploy a new image".
- **Acceptance:**
  - The CronJob builds on schedule, and an on-demand Job builds too.
  - Port-forwarding shows the pages, with data from the latest silver run.
  - A deliberately broken build (a failing query, say) leaves the previous site served.
  - Nothing is exposed outside the cluster: no Ingress, no LoadBalancer Service.

## Follow-on: pages over classification and embeddings

Once C2/C3 exist, C3's report (daily topic shares, spam/adult rates, agreement with Bluesky's labels) can be Evidence pages in `reports/`, reading C3's threshold views, instead of a separate script or notebook. Pages built on embeddings (E2) could follow. Neither is planned in detail yet.

## Costs

- **No always-on server for the build:** the CronJob runs a few minutes an hour on existing nodes.
- **Reads:** DuckDB costs S3 GET requests and in-region transfer only (through the free S3 gateway endpoint), plus Glue catalog requests. There is no per-TB scan charge as with Athena.
- **Serving:** one nginx pod, tens of MB of memory.

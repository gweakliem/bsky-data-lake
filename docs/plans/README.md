# Plans

Work plans for the next phases of the data lake. Each plan is split into work packages (WPs) that can be picked up independently, by a person or a subagent, once their dependencies are done.

| Plan | Scope |
|---|---|
| [data-pipeline.md](data-pipeline.md) | Silver layer, Jev classification, embeddings and search |
| [deployment.md](deployment.md) | Running everything on EKS, managed by Pulumi |

## Dependency overview

```
                      ┌──────────────► C1 ──► C2 ──► C3          (classification)
P1 ──► S1 ──► S2 ──► S3
                      └──────────────► E1 ──► E2 ──► E3          (embeddings/search)

D1 (loader image) ──┐
D0 ──► D2 ──────────┼──► D4a (loader) ──► D4b (ingestor) ──► D5 ──► D6     (deployment)
D0 ──► D3 ──────────┘                          ▲
                                     P1 + ingestor image

Done: D0 (IAM access), D3 (cluster), P1 (cursor resume), S1 (bronze survey), D4a (loader on EKS), D4b (ingestor on EKS) and D6 apart from job alarms. D1 and D2 are done apart from CI.
D5 adds each pipeline job (S2, C2, E2, E3) as it lands.
```

Pipeline WPs can be developed and run locally (or with `--once`-style runs against AWS) before deployment exists. D5 adds each job's deployment as it lands.

## Conventions for anyone picking up a WP

- Read the repo `CLAUDE.md` and the CLAUDE.md of any subproject you touch first.
- **The repo is public.** No bucket names, account IDs, queue URLs or API keys in committed files. Configuration comes from env vars and the git-ignored `.env`; add new variables to `.env.example` with placeholder values.
- New Python components are uv projects with their own `CLAUDE.md`, following `iceberg-loader/` (tests runnable with `uv run pytest` and no AWS access).
- Infrastructure goes in Pulumi under `infra/`, never in ad-hoc scripts or console clicks.
- Each WP lists acceptance criteria. A WP is done when they are met and tests and lint pass. Note in the WP's PR anything deferred or found to be wrong in the plan, and update the plan.
- Open questions are marked **Decide:** in the WPs. Resolve them in the WP (record the outcome in the plan) rather than guessing silently.

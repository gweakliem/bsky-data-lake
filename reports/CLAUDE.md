# reports

Static report site over the lake: [Observable Framework](https://observablehq.com/framework/) pages, with Python data loaders that query the lake through DuckDB. Node (npm) for the site, a uv project for the loaders; run commands from this directory.

```bash
npm install && uv sync
npm test                      # build from the fixture (no AWS), then uv run pytest
npm run dev:fixture           # live preview on the fixture, http://127.0.0.1:3000
npm run build                 # build against the lake into dist/ (credentials below)
npm run dev                   # live preview against the lake
```

Against the lake from a laptop, export credentials first (DuckDB's `credential_chain` doesn't read SSO profiles), as in the root CLAUDE.md:

```bash
eval "$(aws configure export-credentials --profile <profile> --format env)"
export AWS_REGION=us-west-2
npm run build                 # about 4 min from outside the region: posts_current twice, and bronze
```

`BSKY_AWS_ACCOUNT_ID` comes from the repo's `.env` (`lib/lake.py` reads it without overriding the environment).

- **Layout:** `src/*.md` are pages; `src/data/*.py` are data loaders, each a few lines that run one query from `sql/` through `lib/lake.py` and print CSV or JSON. Framework runs `.py` loaders with `uv run python` (`observablehq.config.js`), from this directory.
- **SQL:** one file per dataset in `sql/`, naming tables `{silver}.<table>` and `{bronze}.<table>`. `lake.connect` fills those in: `glue.bsky_silver`/`glue.bsky_raw` after `ATTACH`ing the Glue catalog through its Iceberg REST endpoint, or the fixture's schemas. Moving clouds changes `_attach_lake`, not the SQL. Times leave the SQL as ISO strings ending in `Z`, so `FileAttachment(...).csv({typed: true})` makes them `Date`s.
- **Fixture:** `BSKY_REPORTS_FIXTURE=1` makes `lake.connect` build in-memory tables (`lib/fixture.py`) with the lake's table and column names, relative to now. Add the columns a new query reads there, and its file to `COLUMNS` in `tests/test_queries.py`.
- **Pages:** Pipeline health (`index.md`: freshness tiles, silver windows, appended rows, rejects), Activity (posts, likes, reposts per day) and Languages (posts per language, with a day-range input and a per-language share chart).
- **Current-state views:** `lake.connect` creates `posts_current` from `silver/sql/posts_current.sql`, rendered by `silver/src/bsky_silver/views.py` (loaded from its file, since the `bsky_silver` package imports Spark), over `recent.posts`/`recent.deletes`: the silver tables filtered to the last `RECENT_DAYS` (14) days. The view can't be pruned by day (latest version per uri), so the window is what bounds it: about 60-90 s per query from a laptop on 2026-11-06, with about 3.5M posts a day. Keep content pages on `posts_current`, not raw `posts`. Its window (over `day`, `uri`, `rev`, `time_us` only: DuckDB prunes the other columns) is also what sizes the build: at 4 threads, activity needs about 4 GB and languages (which also carries `langs`) more than 4 GB but under 6 GB (2026-11-06), hence the CronJob's 6 GB DuckDB limit in an 8Gi pod (`BSKY_DUCKDB_MEMORY_LIMIT`, `BSKY_DUCKDB_THREADS`). If builds get slow or outgrow that, the next lever is daily aggregates written by silver (see the reporting plan).
- **Small data only:** every data file ships to the browser. Queries aggregate; `tests/test_site.py` fails a data file over 5,000 rows or 1 MB.
- **Builds are snapshots:** loaders run at build time and the cache is cleared first (`npm run clean`), so the site shows the lake as of the build. A failing loader fails the build (exit 1), so a broken build never replaces a good site.
- **Bronze:** only the freshness reading touches it, `max(ingested_at)` over the last two days' partitions (about 20 s from a laptop). Never read bronze payloads here.
- **Charts:** colors are the dataviz reference palette's categorical slots (light and dark steps, picked by Framework's `dark`), and status tiles use its status colors with an icon and a label. A stacked chart keeps at most four fixed groups so a table keeps its color between builds; the full breakdown goes in a table under it.
- **Framework imports:** `npm:` imports (d3, Plot, Inputs) are downloaded and self-hosted at build time, so a build needs network access to the npm CDN.
- **Why not Evidence:** the plan chose Evidence, but by 2026-11 its template had moved to Evidence Studio (hosted, `evidence login`, ClickHouse SQL) and deprecated the open-source build. Framework was the plan's fallback.

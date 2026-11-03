"""Copy bronze days into a local DuckDB file for the S1 survey.

Every event keeps its kind, collection and operation; the payload is kept for
everything but likes, reposts and follows, plus a 0.2% sample of those. That is
about 6 GB of disk per day, so check free space first.

    export BSKY_BRONZE_METADATA=$(aws glue get-table --database-name bsky_raw --name jetstream \
        --query 'Table.Parameters.metadata_location' --output text)
    eval "$(aws configure export-credentials --format env)"
    uv run --with duckdb python bronze_extract.py bronze.duckdb 2026-11-01 [more days...]
"""

import os
import sys
import time

import duckdb

db, days = sys.argv[1], sys.argv[2:]
con = duckdb.connect(db)
for ext in ["httpfs", "iceberg"]:
    con.install_extension(ext)
    con.load_extension(ext)
con.execute("CREATE SECRET (TYPE s3, PROVIDER credential_chain, REGION 'us-west-2')")
con.execute(f"CREATE VIEW bronze AS SELECT * FROM iceberg_scan('{os.environ['BSKY_BRONZE_METADATA']}')")
con.execute("""CREATE TABLE IF NOT EXISTS events (day DATE, hour INT, received_at TIMESTAMPTZ, time_us BIGINT,
  kind VARCHAR, collection VARCHAR, operation VARCHAR, payload VARCHAR)""")
for day in days:
    # An hour at a time (~15 s each over the internet), so progress shows.
    for h in range(24):
        t = time.time()
        con.execute(f"""INSERT INTO events
          SELECT day, hour, received_at, time_us, payload->>'$.kind', payload->>'$.commit.collection',
                 payload->>'$.commit.operation',
                 CASE WHEN coalesce(payload->>'$.commit.collection', '')
                           NOT IN ('app.bsky.feed.like', 'app.bsky.feed.repost', 'app.bsky.graph.follow')
                        OR hash(payload) % 500 = 0 THEN payload END
          FROM bronze WHERE day = DATE '{day}' AND hour = {h}""")
        print(day, h, round(time.time() - t, 1), flush=True)
print(con.execute("SELECT count(*), count(payload) FROM events").fetchall())

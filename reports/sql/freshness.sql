-- One row: how current bronze and silver were when the site was built.
-- Bronze is filtered to the last two days' partitions so the scan stays small.
SELECT
    strftime((SELECT max(ingested_at) FROM {bronze}.jetstream WHERE day >= current_date - 1), '%Y-%m-%dT%H:%M:%SZ') AS bronze_ingested_at,
    strftime(max(bronze_through), '%Y-%m-%dT%H:%M:%SZ') AS silver_bronze_through,
    strftime(max(finished_at), '%Y-%m-%dT%H:%M:%SZ') AS silver_finished_at,
    strftime(now(), '%Y-%m-%dT%H:%M:%SZ') AS built_at
FROM {silver}.silver_runs

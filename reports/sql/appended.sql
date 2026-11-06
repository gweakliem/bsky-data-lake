-- Rows appended per silver table per hour, last 7 days, from each run's counts.appended.
WITH runs AS (
    SELECT finished_at, counts, unnest(json_keys(counts, '$.appended')) AS tbl
    FROM {silver}.silver_runs
    WHERE finished_at >= now() - INTERVAL 7 DAY
)
SELECT
    strftime(date_trunc('hour', finished_at), '%Y-%m-%dT%H:%M:%SZ') AS hour,
    tbl AS "table",
    sum(CAST(json_extract(counts, '$.appended.' || tbl) AS BIGINT)) AS rows
FROM runs
GROUP BY 1, 2
ORDER BY 1, 2

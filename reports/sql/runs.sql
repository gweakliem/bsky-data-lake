-- One row per silver window in the last 7 days.
SELECT
    strftime(finished_at, '%Y-%m-%dT%H:%M:%SZ') AS finished_at,
    bronze_rows,
    round(date_diff('second', bronze_through, finished_at) / 60.0, 1) AS lag_minutes,
    date_diff('second', started_at, finished_at) AS seconds
FROM {silver}.silver_runs
WHERE finished_at >= now() - INTERVAL 7 DAY
ORDER BY finished_at

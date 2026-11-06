-- Rejected bronze rows per day and reason, last 30 days. Reasons are exception
-- text, so they're cut to their first 80 characters before grouping.
SELECT
    strftime(day, '%Y-%m-%d') AS day,
    left(reason, 80) AS reason,
    count(*) AS rows
FROM {silver}.rejects
WHERE day >= current_date - 30
GROUP BY 1, 2
ORDER BY 1, 2

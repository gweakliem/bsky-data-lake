-- Posts, likes and reposts per day over the recent window (lake.RECENT_DAYS), by
-- day received. Posts are current ones (posts_current: latest version, deleted
-- posts left out); likes and reposts count every create.
SELECT strftime(day, '%Y-%m-%d') AS day, 'posts' AS kind, count(*) AS rows
FROM posts_current
GROUP BY day
UNION ALL
SELECT strftime(day, '%Y-%m-%d'), 'likes', count(*)
FROM {silver}.likes
WHERE day >= (SELECT min(day) FROM recent.posts)
GROUP BY day
UNION ALL
SELECT strftime(day, '%Y-%m-%d'), 'reposts', count(*)
FROM {silver}.reposts
WHERE day >= (SELECT min(day) FROM recent.posts)
GROUP BY day
ORDER BY day, kind

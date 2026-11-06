-- Current posts per day and language over the recent window. A post counts once
-- for each language it declares, by primary subtag (en-US is en); posts with
-- none count as '(none)'. Languages
-- outside the window's top 12 fold into '(other)' so the result stays small.
WITH counts AS (
    -- Primary subtags, deduplicated within each post (en-US and en count once),
    -- counted per day first: posts_current is read once, and what follows is small.
    SELECT day, coalesce(lang, '(none)') AS lang, count(*) AS posts
    FROM (
        SELECT day, unnest(coalesce(nullif(
            list_distinct(list_filter(list_transform(langs, l -> lower(split_part(trim(l), '-', 1))), l -> l <> '')),
            []), [NULL])) AS lang
        FROM posts_current
    )
    GROUP BY 1, 2
),
top AS (
    SELECT lang FROM counts WHERE lang <> '(none)' GROUP BY lang ORDER BY sum(posts) DESC LIMIT 12
)
SELECT
    strftime(day, '%Y-%m-%d') AS day,
    CASE WHEN lang = '(none)' OR lang IN (SELECT lang FROM top) THEN lang ELSE '(other)' END AS lang,
    sum(posts) AS posts
FROM counts
GROUP BY 1, 2
ORDER BY 1, 3 DESC

-- post_embeddings_current: one embedding per post in posts_current, for the
-- active spec_version.
--
-- Same rules as post_classifications_current: the current cid's embedding
-- when there is one, otherwise the newest of an older cid with stale = true.
-- Deleted posts drop out with posts_current.
SELECT
    uri, cid, current_cid, cid <> current_cid AS stale, day, model, spec_version, embedded_at,
    bronze_ingested_at, vector
FROM (
    SELECT
        e.*,
        p.cid AS current_cid,
        row_number() OVER (
            PARTITION BY e.uri ORDER BY CASE WHEN e.cid = p.cid THEN 0 ELSE 1 END, e.embedded_at DESC
        ) AS pick
    FROM {silver}.post_embeddings e
    JOIN posts_current p ON p.uri = e.uri
    WHERE e.spec_version = '{embedding_spec}'
) e
WHERE pick = 1

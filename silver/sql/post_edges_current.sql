-- post_edges_current: edges from the latest version of each source record,
-- without sources that have a later delete (an unliked like, a deleted reply).
--
-- An edited post repeats its edges under each version; dense_rank keeps every
-- edge of the latest one. Edges to a deleted post are kept: the reply or the
-- like still happened.
SELECT src_uri, dst_uri, type, src_did, rev, created_at, time_us, day
FROM (
    SELECT *, dense_rank() OVER (PARTITION BY src_uri ORDER BY rev DESC, time_us DESC) AS version_rank
    FROM {silver}.post_edges
) e
LEFT JOIN (
    SELECT uri AS deleted_uri, max(rev) AS deleted_rev
    FROM {silver}.deletes
    GROUP BY uri
) d ON d.deleted_uri = e.src_uri AND d.deleted_rev > e.rev
WHERE e.version_rank = 1 AND d.deleted_uri IS NULL

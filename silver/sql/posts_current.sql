-- posts_current: the latest version of each post, without deleted posts.
--
-- The latest version is the highest rev (a TID, so it sorts as a string),
-- then the highest time_us. A post is deleted when `deletes` has a later rev
-- for its uri. Same columns as `posts`.
SELECT
    event_id, did, time_us, received_at, bronze_ingested_at, day, rkey, uri, rev, cid, operation, created_at, text,
    langs, reply_root_uri, reply_parent_uri, embed_type, quote_uri, image_alts, external_uri, external_title,
    mention_dids, link_uris, tags, self_labels, record
FROM (
    SELECT *, row_number() OVER (PARTITION BY uri ORDER BY rev DESC, time_us DESC) AS version_rank
    FROM {silver}.posts
) p
LEFT JOIN (
    SELECT uri AS deleted_uri, max(rev) AS deleted_rev
    FROM {silver}.deletes
    WHERE collection = 'app.bsky.feed.post'
    GROUP BY uri
) d ON d.deleted_uri = p.uri AND d.deleted_rev > p.rev
WHERE p.version_rank = 1 AND d.deleted_uri IS NULL

-- post_classifications_current: one classification per post in posts_current,
-- for the active spec_version.
--
-- It is the classification of the post's current cid when there is one.
-- Otherwise (the post was edited since) it is the newest classification of an
-- older cid, with stale = true. Filter on NOT stale for classifications that
-- match the current text; a stale or missing row means the post needs
-- classifying again. Deleted posts drop out with posts_current. Thresholds
-- belong in C3's views, not here.
SELECT
    uri, cid, current_cid, cid <> current_cid AS stale, day, model, spec_version, classified_at,
    bronze_ingested_at, p_spam, p_adult, p_ai_generated, topic, topic_confidence, topic_probs
FROM (
    SELECT
        c.*,
        p.cid AS current_cid,
        row_number() OVER (
            PARTITION BY c.uri ORDER BY CASE WHEN c.cid = p.cid THEN 0 ELSE 1 END, c.classified_at DESC
        ) AS pick
    FROM {silver}.post_classifications c
    JOIN posts_current p ON p.uri = c.uri
    WHERE c.spec_version = '{classification_spec}'
) c
WHERE pick = 1

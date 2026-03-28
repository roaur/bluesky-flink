-- 02-trending-topics.sql
-- Deduplicate posts, extract hashtags, 5-min tumbling window,
-- write to Kafka topic for ClickHouse

CREATE TEMPORARY VIEW deduped_posts AS
SELECT did, cid, text, hashtags, created_at, obtained_at, event_time
FROM (
    SELECT *,
        ROW_NUMBER() OVER (PARTITION BY cid ORDER BY obtained_at ASC) AS rn
    FROM raw_events
)
WHERE rn = 1;

CREATE TEMPORARY VIEW hashtag_counts AS
SELECT
    tag AS hashtag,
    TUMBLE_START(event_time, INTERVAL '5' MINUTE) AS window_start,
    TO_TIMESTAMP_LTZ(MIN(obtained_at), 3) AS obtained_at,
    CURRENT_TIMESTAMP AS created_at,
    COUNT(*) AS cnt
FROM deduped_posts
CROSS JOIN UNNEST(hashtags) AS t(tag)
WHERE tag IS NOT NULL AND LENGTH(tag) > 0
GROUP BY tag, TUMBLE(event_time, INTERVAL '5' MINUTE);

-- Sink to Kafka (trending-topics topic)
INSERT INTO analytics.trending_topics_sink (hashtag, `count`, window_start, obtained_at, created_at)
SELECT hashtag, cnt, window_start, obtained_at, created_at
FROM hashtag_counts;

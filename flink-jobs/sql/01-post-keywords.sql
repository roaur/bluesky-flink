-- 01-post-keywords.sql
-- Deduplicate posts, tokenize text into words, 5-min tumbling window,
-- write to Kafka topic for ClickHouse

CREATE TEMPORARY VIEW deduped_posts AS
SELECT did, cid, text, created_at, obtained_at, event_time
FROM (
    SELECT *,
        ROW_NUMBER() OVER (PARTITION BY cid ORDER BY obtained_at ASC) AS rn
    FROM raw_events
)
WHERE rn = 1;

CREATE TEMPORARY VIEW word_counts AS
SELECT
    word,
    TUMBLE_START(event_time, INTERVAL '5' MINUTE) AS window_start,
    TO_TIMESTAMP_LTZ(MIN(obtained_at), 3) AS obtained_at,
    CURRENT_TIMESTAMP AS created_at,
    COUNT(*) AS cnt
FROM deduped_posts
CROSS JOIN LATERAL TABLE(STRING_SPLIT(LOWER(text), ' ')) AS t(word)
WHERE word IS NOT NULL AND LENGTH(word) > 1
GROUP BY word, TUMBLE(event_time, INTERVAL '5' MINUTE);

-- Sink to Kafka (keyword-counts topic)
INSERT INTO analytics.keyword_counts_sink (word, `count`, window_start, obtained_at, created_at)
SELECT word, cnt, window_start, obtained_at, created_at
FROM word_counts;

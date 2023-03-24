WITH base AS (
    SELECT
        author_name,
        author_url,
        extracted_at,
        extracted_at_epoch,
        extraction_id,
        extraction_url,
        CAST(published_at AS TIMESTAMP) AS published_at,
        CAST(published_at AS DATE) AS published_date,
        CAST(IF(reading_time_minutes = -1, 0, reading_time_minutes) AS INTEGER) AS reading_time_minutes,
        story_url,
        subtitle,
        tag,
        title,
        ROW_NUMBER() OVER(PARTITION BY story_url ORDER BY extracted_at DESC) AS rnum
    FROM read_json_auto("{{ env_var('DATA_DIR') }}/landing_zone/*/*/*.json")
)

SELECT
    * EXCLUDE(rnum)
FROM base
WHERE
    rnum = 1

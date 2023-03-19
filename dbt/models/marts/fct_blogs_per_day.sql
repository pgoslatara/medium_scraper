{{
    config(
        location='{{ env_var("DATA_DIR") }}/marts/{{ this.name }}.parquet'
    )
}}

SELECT
    published_date,
    tag,
    COUNT(DISTINCT story_url) AS num_blogs
FROM {{ ref('stg_medium__articles') }}
GROUP BY 1, 2

SELECT 
    published_date,
    tag,
    COUNT(DISTINCT story_url) AS num_blogs
FROM {{ ref('stg_medium__articles') }}
GROUP BY 1, 2

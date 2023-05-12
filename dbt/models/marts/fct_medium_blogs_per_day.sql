{{
    config(
        location='{{ env_var("DATA_DIR") }}/marts/{{ this.name }}.parquet'
    )
}}

select published_date, tag, count(distinct story_url) as num_blogs
from {{ ref('stg_medium_blogs__articles') }}
group by 1, 2

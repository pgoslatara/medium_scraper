{{
    config(
        location='{{ env_var("DATA_DIR") }}/marts/{{ this.name }}.parquet'
    )
}}

select art.*, auth.is_author_based_in_netherlands
from {{ ref('stg_medium_blogs__articles') }} art
left join
    {{ ref('int_medium__dim_medium_authors') }} auth on auth.author_url = art.author_url

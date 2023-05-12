{{
    config(
        location='{{ env_var("DATA_DIR") }}/marts/{{ this.name }}.parquet'
    )
}}

select
    a.author_name,
    a.author_url,
    b.author_name,
    a.num_followers,
    a.short_bio,
    a.extracted_at
from {{ ref('stg_medium_authors__authors') }} a
left join {{ ref('stg_medium_blogs__articles') }} b on b.author_url = a.author_url
group by 1, 2, 3, 4, 5, 6

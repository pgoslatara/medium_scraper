{{
    config(
        location='{{ env_var("DATA_DIR") }}/marts/{{ this.name }}.parquet'
    )
}}

select
    author,
    created_at,
    discussion_number,
    discussion_title,
    discussion_url,
    first_extracted_at,
    repo_name,
    repo_owner
from {{ ref('stg_github_discussions__discussions') }}

{{
    config(
        location='{{ env_var("DATA_DIR") }}/marts/{{ this.name }}.parquet'
    )
}}

select author_name, author_url, author_name, num_followers, short_bio, extracted_at
from {{ ref('stg_medium_authors__authors') }}

{{
    config(
        location='{{ env_var("DATA_DIR") }}/marts/{{ this.name }}.parquet'
    )
}}

select *
from {{ ref('stg_github_repo_interactors__repos') }}

{{
    config(
        location='{{ env_var("DATA_DIR") }}/marts/{{ this.name }}.parquet'
    )
}}

select *
from {{ ref('stg_medium_blogs__articles') }}

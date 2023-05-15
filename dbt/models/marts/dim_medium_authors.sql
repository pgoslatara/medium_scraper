{{
    config(
        location='{{ env_var("DATA_DIR") }}/marts/{{ this.name }}.parquet'
    )
}}

select *
from {{ ref('int_medium__dim_medium_authors') }}

with
    base as (
        select
            author_name,
            author_url,
            extracted_at,
            extracted_at_epoch,
            extraction_id,
            extraction_url,
            cast(published_at as timestamp) as published_at,
            cast(published_at as date) as published_date,
            cast(
                if(reading_time_minutes = -1, 0, reading_time_minutes) as integer
            ) as reading_time_minutes,
            story_url,
            subtitle,
            tag,
            title,
            row_number() over (
                partition by story_url order by extracted_at desc
            ) as rnum
        from read_json_auto("{{ env_var('DATA_DIR') }}/landing_zone/*/*/*.json")
        where author_name != 'Thirahealth'  -- Tags Dialectical Behavior Therapy blogs with dbt, want to exclude these
    )

select * exclude(rnum)
from base
where rnum = 1

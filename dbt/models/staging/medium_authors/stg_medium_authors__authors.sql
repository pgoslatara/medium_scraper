with
    base as (
        select
            author_name,
            author_url,
            extracted_at,
            extracted_at_epoch,
            extraction_id,
            num_followers,
            short_bio,
            row_number() over (
                partition by author_url order by extracted_at desc
            ) as rnum
        from
            read_json_auto(
                "{{ env_var('DATA_DIR') }}/landing_zone/domain=medium_authors/schema_version=2/*/*.json"
            )
    )

select * exclude(rnum)
from base
where rnum = 1

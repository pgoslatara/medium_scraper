with
    base as (
        select *
        from
            read_json_auto(
                "{{ env_var('DATA_DIR') }}/landing_zone/domain=github_pull_requests/schema_version=1/*/*.json",
                columns = {
                    extracted_at:'STRING',
                    extracted_at_epoch:'NUMERIC',
                    extraction_id:'UUID',
                    created_at:'STRING',
                    html_url:'STRING',
                    id:'STRING',
                    number:'NUMERIC',
                    repository_url:'STRING',
                    state:'STRING',
                    title:'STRING',
                    "user":'STRUCT(login STRING)'
                },
                maximum_object_size = 268435456
            )
    ),
    t1 as (select max(extracted_at_epoch) as max_extracted_at_epoch from base)

select
    cast(created_at as timestamp) as created_at,
    html_url,
    id as pull_request_id,
    "user".login as pull_request_creator,
    number,
    repository_url,
    state,
    title
from base
join t1 on base.extracted_at_epoch = t1.max_extracted_at_epoch

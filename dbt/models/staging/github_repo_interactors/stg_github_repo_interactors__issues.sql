with
    base as (
        select *
        from
            read_json_auto(
                "{{ env_var('DATA_DIR') }}/landing_zone/domain=github_issues/schema_version=1/*/*.json",
                columns = {
                    extracted_at:'STRING',
                    extracted_at_epoch:'NUMERIC',
                    extraction_id:'UUID',
                    created_at:'STRING',
                    html_url:'STRING',
                    id:'NUMERIC',
                    number:'NUMERIC',
                    repository_url:'STRING',
                    state:'STRING',
                    title:'STRING',
                    "user":'STRUCT(login STRING)'
                },
                maximum_object_size = 134217728
            )
    ),
    t1 as (select max(extracted_at_epoch) as max_extracted_at_epoch from base)

select
    cast(created_at as timestamp) as created_at,
    html_url,
    id as issue_id,
    "user".login as issue_creator,
    number as issue_number,
    repository_url,
    state,
    title
from base
join t1 on base.extracted_at_epoch = t1.max_extracted_at_epoch
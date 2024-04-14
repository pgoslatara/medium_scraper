with
    base as (
        select *
        from
            read_json_auto(
                "{{ env_var('DATA_DIR') }}/landing_zone/domain=github_pull_requests/schema_version=2/*/*.json",
                columns = {
                    author:'STRUCT(login STRING)',
                    "createdAt":'STRING',
                    "databaseId":'NUMERIC',
                    extracted_at:'STRING',
                    extracted_at_epoch:'NUMERIC',
                    extraction_id:'UUID',
                    number:'NUMERIC',
                    repository:'STRUCT(url STRING)',
                    state:'STRING',
                    title:'STRING',
                    url:'STRING'
                },
                maximum_object_size = 268435456
            )
    ),
    t1 as (select max(extracted_at_epoch) as max_extracted_at_epoch from base)

select
    cast("createdAt" as timestamp) as created_at,
    url as html_url,
    databaseid as pull_request_id,
    author.login as pull_request_creator,
    number,
    repository.url as repository_url,
    state,
    title
from base
join t1 on base.extracted_at_epoch = t1.max_extracted_at_epoch

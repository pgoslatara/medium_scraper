with
    t1 as (
        select max(extracted_at_epoch) as max_extracted_at_epoch

        from
            read_json_auto(
                "{{ env_var('DATA_DIR') }}/landing_zone/domain=github_repos/schema_version=2/*/*/*.json"
            )

    ),
    first_extracted as (
        select name, min(to_timestamp(extracted_at)) as first_extracted_at
        from
            read_json_auto(
                "{{ env_var('DATA_DIR') }}/landing_zone/domain=github_repos/schema_version=2/*/*/*.json"
            )
        group by 1
    )

select distinct
    l.namewithowner as full_name,
    l.url as html_url,
    l."databaseId" as repo_id,
    l.name as repo_name,
    l.url,
    first_extracted.first_extracted_at
from
    read_json_auto(
        "{{ env_var('DATA_DIR') }}/landing_zone/domain=github_repos/schema_version=2/*/*/*.json"
    ) l
join t1 on l.extracted_at_epoch = t1.max_extracted_at_epoch
join first_extracted on l.name = first_extracted.name

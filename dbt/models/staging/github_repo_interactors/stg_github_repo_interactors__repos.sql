with
    t1 as (
        select max(extracted_at_epoch) as max_extracted_at_epoch

        from
            read_json_auto(
                "{{ env_var('DATA_DIR') }}/landing_zone/domain=github_repos/schema_version=2/*/*/*.json"
            )

    )

select distinct
    namewithowner as full_name,
    url as html_url,
    "databaseId" as repo_id,
    name as repo_name,
    url,
    cast("createdAt" as timestamp) as created_at
from
    read_json_auto(
        "{{ env_var('DATA_DIR') }}/landing_zone/domain=github_repos/schema_version=2/*/*/*.json"
    ) base
join t1 on base.extracted_at_epoch = t1.max_extracted_at_epoch

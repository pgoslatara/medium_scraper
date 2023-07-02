with
    t1 as (
        select max(extracted_at_epoch) as max_extracted_at_epoch

        from
            read_json_auto(
                "{{ env_var('DATA_DIR') }}/landing_zone/domain=github_repos/schema_version=1/*/*/*.json"
            )

    )

select distinct full_name, html_url, id as repo_id, name as repo_name, url
from
    read_json_auto(
        "{{ env_var('DATA_DIR') }}/landing_zone/domain=github_repos/schema_version=1/*/*/*.json"
    ) l
join t1 on l.extracted_at_epoch = t1.max_extracted_at_epoch

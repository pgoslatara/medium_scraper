with
    base as (
        select
            cast(extracted_at as timestamp) as extracted_at,
            extracted_at_epoch,
            extraction_id,
            html_url,
            repo_name,
            run_duration_ms,
            cast(run_started_at as timestamp) as run_started_at,
            workflow_id,
            workflow_name,
            workflow_run_id,
            row_number() over (
                partition by workflow_run_id order by extracted_at desc
            ) as rnum
        from
            read_json_auto(
                "{{ env_var('DATA_DIR') }}/landing_zone/domain=github_actions_workflow_runs/schema_version=1/*/*.json"
            )
    )

select * exclude(rnum)
from base
where rnum = 1

with
    base as (
        select
            cast(completed_at as timestamp) as completed_at,
            extracted_at,
            extracted_at_epoch,
            extraction_id,
            html_url,
            job_id,
            job_name,
            cast(started_at as timestamp) as started_at,
            workflow_run_id,
            row_number() over (partition by job_id order by extracted_at desc) as rnum
        from
            read_json_auto(
                "{{ env_var('DATA_DIR') }}/landing_zone/domain=github_actions_jobs/schema_version=1/*/*.json"
            )
    )

select * exclude(rnum)
from base
where rnum = 1

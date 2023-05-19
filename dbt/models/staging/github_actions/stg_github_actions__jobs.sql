with
    base as (
        select
            cast(completed_at as timestamp) as completed_at,
            extracted_at,
            extracted_at_epoch,
            extraction_id,
            html_url,
            id as job_id,
            name as job_name,
            run_id as workflow_run_id,
            cast(started_at as timestamp) as started_at,
            row_number() over (partition by job_id order by extracted_at desc) as rnum
        from
            read_json_auto(
                "{{ env_var('DATA_DIR') }}/landing_zone/domain=github_actions_jobs/schema_version=1/*/*.json",
                columns = {
                    completed_at:'STRING',
                    extracted_at:'STRING',
                    extracted_at_epoch:'NUMERIC',
                    extraction_id:'UUID',
                    html_url:'STRING',
                    id:'NUMERIC',
                    name:'STRING',
                    run_id:'NUMERIC',
                    started_at:'STRING'
                },
                maximum_object_size = 33554432
            )
    )

select * exclude(rnum)
from base
where rnum = 1

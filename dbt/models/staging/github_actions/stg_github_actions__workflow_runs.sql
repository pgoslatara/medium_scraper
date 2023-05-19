with
    base as (
        select
            extracted_at,
            extracted_at_epoch,
            extraction_id,
            html_url,
            id as workflow_run_id,
            name as workflow_name,
            repository.name as repo_name,
            strptime(run_started_at, '%Y-%m-%dT%H:%M:%SZ') as run_started_at,
            workflow_id,
            row_number() over (
                partition by workflow_run_id order by extracted_at desc
            ) as rnum
        from
            read_json_auto(
                "{{ env_var('DATA_DIR') }}/landing_zone/domain=github_actions_workflow_runs/schema_version=1/*/*.json",
                columns = {
                    extracted_at:'STRING',
                    extracted_at_epoch:'NUMERIC',
                    extraction_id:'UUID',
                    html_url:'STRING',
                    id:'NUMERIC',
                    name:'STRING',
                    repository:'STRUCT(name STRING)',
                    run_started_at:'STRING',
                    workflow_id:'NUMERIC'
                },
                maximum_object_size = 33554432
            )
    )

select * exclude(rnum)
from base
where rnum = 1

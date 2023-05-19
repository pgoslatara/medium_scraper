with
    usage as (
        select
            workflow_run_id,
            sum(date_diff('seconds', started_at, completed_at)) as usage_sec
        from {{ ref('stg_github_actions__jobs') }}
        group by 1
        order by 1
    )
select w.*, coalesce(usage.usage_sec, 0) as usage_sec
from {{ ref('stg_github_actions__workflow_runs') }} w
left join usage on usage.workflow_run_id = w.workflow_run_id

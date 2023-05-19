with
    usage as (
        select
            workflow_run_id,
            job_id,
            date_diff('seconds', started_at, completed_at) as usage_sec,
            ceiling(cast(coalesce(usage_sec, 0) as numeric) / 60) as billable_minutes  -- billable minutes are calculated per job per minute
        from {{ ref('stg_github_actions__jobs') }}
    ),
    agg_usage as (
        select
            workflow_run_id,
            sum(usage_sec) as usage_sec,
            sum(billable_minutes) as billable_minutes
        from usage
        group by 1
    )

select w.*, agg_usage.usage_sec, agg_usage.billable_minutes
from {{ ref('stg_github_actions__workflow_runs') }} w
left join agg_usage on agg_usage.workflow_run_id = w.workflow_run_id

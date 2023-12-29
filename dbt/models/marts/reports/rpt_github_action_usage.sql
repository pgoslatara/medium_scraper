{{
    config(
        location='{{ env_var("DATA_DIR") }}/marts/reports/{{ this.name }}.parquet'
    )
}}
select
    run_started_date,
    sum(billable_minutes) as billable_minutes_daily,
    sum(sum(billable_minutes)) over (
        partition by datetrunc('month', run_started_date) order by run_started_date
    ) as billable_minutes_cum_sum
from {{ ref('dim_github_action_usage') }}
group by 1
qualify run_started_date >= (current_date() - interval 60 day)
order by 1

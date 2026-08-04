{{
    config(
        location='{{ env_var("DATA_DIR") }}/marts/{{ this.name }}.parquet'
    )
}}


with
    issue_stats as (
        select
            issue_creator as username,
            count(distinct issue_id) as num_issues_created,
            min(created_at) as first_issue_created_at,
            max(created_at) as last_issue_created_at,
        from {{ ref('stg_github_repo_interactors__issues') }}
        group by 1
    ),
    pr_stats as (
        select
            pull_request_creator as username,
            count(distinct pull_request_id) as num_prs_created,
            min(created_at) as first_pr_created_at,
            max(created_at) as last_pr_created_at,
        from {{ ref('stg_github_repo_interactors__pull_requests') }}
        group by 1
    ),
    interactions as (
        select
            issue_creator as username,
            'issue' as interaction_type,
            created_at,
            title,
            html_url,
            repository_url
        from {{ ref('stg_github_repo_interactors__issues') }}

        union all

        select
            pull_request_creator as username,
            'pull_request' as interaction_type,
            created_at,
            title,
            html_url,
            repository_url
        from {{ ref('stg_github_repo_interactors__pull_requests') }}
    ),
    first_interaction as (
        -- The staging models hold every issue and PR ever opened on a scraped repo, so
        -- the earliest row per user is their first ever interaction, not merely the
        -- first one the pipeline happened to see.
        select
            username,
            created_at as first_interaction_at,
            concat(
                string_split(repository_url, '/')[4],
                '/',
                string_split(repository_url, '/')[5]
            ) as first_interaction_repo,
            title as first_interaction_title,
            interaction_type as first_interaction_type,
            html_url as first_interaction_url,
        from interactions
        -- html_url breaks ties between an issue and a PR opened at the same second
        qualify
            row_number() over (partition by username order by created_at, html_url) = 1
    )

select
    u.username,
    u.html_url as user_url,
    u.location,
    i.num_issues_created,
    i.first_issue_created_at,
    i.last_issue_created_at,
    pr.num_prs_created,
    pr.first_pr_created_at,
    pr.last_pr_created_at,
    {{ is_location_in_netherlands('u.location') }} as is_user_based_in_netherlands,
    fi.first_interaction_at,
    fi.first_interaction_repo,
    fi.first_interaction_title,
    fi.first_interaction_type,
    fi.first_interaction_url,
    u.user_info_extracted_at
from {{ ref ('stg_github_repo_interactors__users') }} u
left join issue_stats i on u.username = i.username
left join pr_stats pr on u.username = pr.username
left join first_interaction fi on u.username = fi.username

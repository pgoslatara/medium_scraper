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
    if(
        u.location in (
            'Almere',
            'Amsterdam',
            'Amsterdam, NL',
            'Amsterdam, The Netherlands',
            'Amsterdam / Gliwice',
            'Breda',
            'Delft',
            'Delft, The Netherlands',
            'Eindhoven',
            'Eindhoven, The Netherlands',
            'Groningen',
            'Netherlands',
            'Nijmegen',
            'Oegstgeest',
            'Rotterdam',
            'Rotterdam, the Netherlands',
            'The Hague',
            'The Netherlands',
            'Tilburg',
            'Utrecht'
        )
        or lower(u.location) like '%amsterdam%'
        or lower(u.location) like '%netherlands%'
        or u.location like '%, NL%'
        or u.location = 'NL',
        true,
        false
    ) as is_user_based_in_netherlands
from {{ ref ('stg_github_repo_interactors__users') }} u
left join issue_stats i on u.username = i.username
left join pr_stats pr on u.username = pr.username

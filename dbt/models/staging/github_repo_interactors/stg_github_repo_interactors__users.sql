with
    base as (
        select *
        from
            read_json_auto(
                "{{ env_var('DATA_DIR') }}/landing_zone/domain=github_users/schema_version=2/*/*.json",
                columns = {
                    bio:'STRING',
                    company:'STRING',
                    "databaseId":'NUMERIC',
                    email:'STRING',
                    extracted_at:'STRING',
                    extracted_at_epoch:'NUMERIC',
                    extraction_id:'UUID',
                    location:'STRING',
                    login:'STRING',
                    name:'STRING',
                    "twitterUsername":'STRING',
                    url:'STRING',
                    "websiteUrl":'STRING',
                },
                maximum_object_size = 268435456
            )
    ),
    t1 as (
        select "databaseId" as id, max(extracted_at_epoch) as max_extracted_at_epoch

        from base
        group by 1

    )

select
    bio,
    "websiteUrl" as blog,
    company,
    email,
    to_timestamp(extracted_at) as user_info_extracted_at,
    "databaseId" as id,
    url as html_url,
    location,
    login as username,
    name,
    "twitterUsername" as twitter_username
from base
join
    t1
    on base."databaseId" = t1.id
    and base.extracted_at_epoch = t1.max_extracted_at_epoch

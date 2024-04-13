with
    t1 as (
        select max(extracted_at_epoch) as max_extracted_at_epoch

        from
            read_json_auto(
                "{{ env_var('DATA_DIR') }}/landing_zone/domain=github_users/schema_version=2/*/*.json",
                maximum_object_size = 134217728
            )

    )

select
    bio,
    "websiteUrl" as blog,
    company,
    email,
    "databaseId" as id,
    url as html_url,
    location,
    login as username,
    name,
    "twitterUsername" as twitter_username
from
    read_json_auto(
        "{{ env_var('DATA_DIR') }}/landing_zone/domain=github_users/schema_version=2/*/*.json",
        maximum_object_size = 134217728
    ) l
join t1 on l.extracted_at_epoch = t1.max_extracted_at_epoch

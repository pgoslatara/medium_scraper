with
    t1 as (
        select max(extracted_at_epoch) as max_extracted_at_epoch

        from
            read_json_auto(
                "{{ env_var('DATA_DIR') }}/landing_zone/domain=github_discussions/schema_version=1/*/*.json"
            )

    ),
    first_extracted as (
        select id, min(to_timestamp(extracted_at)) as first_extracted_at
        from
            read_json_auto(
                "{{ env_var('DATA_DIR') }}/landing_zone/domain=github_discussions/schema_version=1/*/*.json"
            )
        group by 1
    ),
    base as (
        select
            extracted_at,
            extracted_at_epoch,
            extraction_id,
            author.login as author,
            category.name as category_name,
            strptime("createdAt", '%Y-%m-%dT%H:%M:%SZ') as created_at,
            id as discussion_id,
            number as discussion_number,
            string_split(url, '/')[4] as repo_owner,
            string_split(url, '/')[5] as repo_name,
            title as discussion_title,
            url as discussion_url,
            row_number() over (partition by number order by extracted_at desc) as rnum
        from
            read_json_auto(
                "{{ env_var('DATA_DIR') }}/landing_zone/domain=github_discussions/schema_version=1/*/*.json",
                columns = {
                    author:'STRUCT(login STRING)',
                    category:'STRUCT(name STRING)',
                    "createdAt":'STRING',
                    extracted_at:'STRING',
                    extracted_at_epoch:'NUMERIC',
                    extraction_id:'UUID',
                    id:'STRING',
                    number:'NUMERIC',
                    title:'STRING',
                    url:'STRING'
                },
                maximum_object_size = 33554432
            )
    )

select base.* exclude(rnum), first_extracted.first_extracted_at
from base
join t1 on base.extracted_at_epoch = t1.max_extracted_at_epoch
join first_extracted on base.discussion_id = first_extracted.id
where base.rnum = 1

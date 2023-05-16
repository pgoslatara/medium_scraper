select
    a.*,
    auth_enr.linkedin_username,
    auth_enr.spacy_location,
    auth_enr.twitter_handle,
    if(
        lower(auth_enr.spacy_location) like '%netherlands%'
        or auth_enr.spacy_location in (
            'Almere',
            'Amsterdam',
            'Breda',
            'Eindhoven',
            'Groningen',
            'Nijmegen',
            'Rotterdam',
            'The Hague',
            'Tilburg',
            'Utrecht'
        ),
        true,
        false
    ) as is_author_based_in_netherlands
from {{ ref('stg_medium_authors__authors') }} a
left join
    {{ ref('stg_medium_authors__authors_enriched') }} auth_enr
    on auth_enr.author_url = a.author_url

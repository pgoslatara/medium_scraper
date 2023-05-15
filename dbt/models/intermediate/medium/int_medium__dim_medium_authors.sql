select
    a.*,
    nlp.spacy_location,
    if(
        lower(nlp.spacy_location) like '%netherlands%'
        or nlp.spacy_location in (
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
    {{ ref('stg_medium_authors__nlp_location') }} nlp on nlp.author_url = a.author_url

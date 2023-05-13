select distinct author_url, short_bio, spacy_location
from "{{ env_var('DATA_DIR') }}/enriched/nlp_author_location.parquet"

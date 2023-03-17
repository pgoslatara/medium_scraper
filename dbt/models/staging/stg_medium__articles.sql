SELECT
    *
FROM read_json_auto("{{ env_var('DATA_DIR') }}/*/*/*.json")

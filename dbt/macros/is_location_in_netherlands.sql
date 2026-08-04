{% macro is_location_in_netherlands(location_column) %}
    -- GitHub locations are free text, so match on an explicit list of places seen in
    -- the data plus a few catch-all patterns. Nulls resolve to false, not null.
    coalesce(
        {{ location_column }} in (
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
            'Nederland',
            'Nederlands',
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
        or lower({{ location_column }}) like '%amsterdam%'
        or lower({{ location_column }}) like '%netherlands%'
        or {{ location_column }} like '%, NL%'
        or {{ location_column }} = 'NL',
        false
    )
{% endmacro %}

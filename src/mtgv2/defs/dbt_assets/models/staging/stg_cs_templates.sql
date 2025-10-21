-- models/staging/stg_cs_templates.sql
-- Cleans and standardizes Commander Spellbook templates data

WITH source AS (
    SELECT * FROM {{ source('dagster', 'pull_cs_templates_table') }}
),

cleaned AS (
    SELECT 
        -- Timestamps
        _loaded_at,
        _partition_date,
        
        -- Core identifiers
        id AS template_id,
        TRIM(name) AS template_name,
        
        -- Template properties
        TRIM(scryfallquery) AS scryfall_query,
        TRIM(scryfallapi) AS scryfall_api_url
        
    FROM source
    WHERE id IS NOT NULL
)

SELECT * FROM cleaned

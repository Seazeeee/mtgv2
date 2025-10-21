-- models/staging/stg_cs_features.sql
-- Cleans and standardizes Commander Spellbook features data

WITH source AS (
    SELECT * FROM {{ source('dagster', 'pull_cs_features_table') }}
),

cleaned AS (
    SELECT 
        -- Timestamps
        _loaded_at,
        _partition_date,
        
        -- Core identifiers
        id AS feature_id,
        TRIM(name) AS feature_name,
        
        -- Feature properties
        uncountable::BOOLEAN AS is_uncountable,
        TRIM(status) AS status
        
    FROM source
    WHERE id IS NOT NULL
)

SELECT * FROM cleaned

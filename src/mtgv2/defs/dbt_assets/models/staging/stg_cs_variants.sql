-- models/staging/stg_cs_variants.sql
-- Cleans and standardizes Commander Spellbook variants data (combo/strategy data)

WITH source AS (
    SELECT * FROM {{ source('dagster', 'pull_cs_variants_table') }}
),

cleaned AS (
    SELECT 
        -- Timestamps
        _loaded_at,
        _partition_date,
        
        -- Core identifiers
        id AS variant_id,
        
        -- Variant properties
        TRIM(status) AS status,
        spoiler::BOOLEAN AS is_spoiler,
        TRIM(identity) AS color_identity,
        
        -- Combo/strategy data (keep as JSONB for now)
        of AS combo_cards,
        uses AS combo_uses,
        includes AS combo_includes,
        produces AS combo_produces,
        requires AS combo_requires,
        
        -- Metadata
        TRIM(notes) AS notes,
        TRIM(description) AS description,
        TRIM(mananeeded) AS mana_needed,
        variantcount::INTEGER AS variant_count,
        manavalueneeded::INTEGER AS mana_value_needed,
        TRIM(easyprerequisites) AS easy_prerequisites,
        TRIM(notableprerequisites) AS notable_prerequisites,
        popularity::INTEGER AS popularity,
        TRIM(brackettag) AS bracket_tag,
        
        -- Prices - standardize to numeric with 2 decimal places
        ROUND(CAST(NULLIF(TRIM("prices.tcgplayer"::TEXT), '') AS NUMERIC), 2) AS price_tcgplayer,
        ROUND(CAST(NULLIF(TRIM("prices.cardmarket"::TEXT), '') AS NUMERIC), 2) AS price_cardmarket,
        ROUND(CAST(NULLIF(TRIM("prices.cardkingdom"::TEXT), '') AS NUMERIC), 2) AS price_cardkingdom,
        
        -- Legalities
        UPPER(NULLIF(TRIM("legalities.brawl"::TEXT), '')) AS legal_brawl,
        UPPER(NULLIF(TRIM("legalities.predh"::TEXT), '')) AS legal_predh,
        UPPER(NULLIF(TRIM("legalities.legacy"::TEXT), '')) AS legal_legacy,
        UPPER(NULLIF(TRIM("legalities.modern"::TEXT), '')) AS legal_modern,
        UPPER(NULLIF(TRIM("legalities.pauper"::TEXT), '')) AS legal_pauper,
        UPPER(NULLIF(TRIM("legalities.pioneer"::TEXT), '')) AS legal_pioneer,
        UPPER(NULLIF(TRIM("legalities.vintage"::TEXT), '')) AS legal_vintage,
        UPPER(NULLIF(TRIM("legalities.standard"::TEXT), '')) AS legal_standard,
        UPPER(NULLIF(TRIM("legalities.commander"::TEXT), '')) AS legal_commander,
        UPPER(NULLIF(TRIM("legalities.premodern"::TEXT), '')) AS legal_premodern,
        UPPER(NULLIF(TRIM("legalities.oathbreaker"::TEXT), '')) AS legal_oathbreaker,
        UPPER(NULLIF(TRIM("legalities.paupercommander"::TEXT), '')) AS legal_paupercommander,
        UPPER(NULLIF(TRIM("legalities.paupercommandermain"::TEXT), '')) AS legal_paupercommandermain
        
    FROM source
    WHERE id IS NOT NULL
)

SELECT * FROM cleaned

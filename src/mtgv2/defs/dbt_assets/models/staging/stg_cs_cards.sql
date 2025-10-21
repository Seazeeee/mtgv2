-- models/staging/stg_cs_cards.sql
-- Cleans and standardizes Commander Spellbook cards data

WITH source AS (
    SELECT * FROM {{ source('dagster', 'pull_cs_cards_table') }}
),

cleaned AS (
    SELECT 
        -- Timestamps
        _loaded_at,
        _partition_date,
        
        -- Core identifiers
        id AS cs_card_id,
        TRIM(name) AS card_name,
        oracleid AS oracle_id,
        
        -- Card properties
        UPPER(TRIM(identity)) AS color_identity,
        manavalue AS cmc,
        spoiler::BOOLEAN AS is_spoiler,
        TRIM(typeline) AS type_line,
        TRIM(oracletext) AS oracle_text,
        
        -- Keywords - parse array string properly
        CASE 
            WHEN keywords IS NULL OR keywords = '' THEN ARRAY[]::TEXT[]
            WHEN keywords LIKE '[%' THEN 
                -- Remove brackets and quotes, split by comma
                string_to_array(
                    regexp_replace(
                        regexp_replace(keywords, '[\[\]'']', '', 'g'),
                        '\s*,\s*', ',', 'g'
                    ),
                    ','
                )
            ELSE ARRAY[keywords]
        END AS keywords_array,
        
        -- Set info
        TRIM(latestprintingset) AS latest_printing_set,
        variantcount::INTEGER AS variant_count,
        
        -- Boolean flags
        reserved::BOOLEAN AS is_reserved,
        reprinted::BOOLEAN AS is_reprinted,
        gamechanger::BOOLEAN AS is_gamechanger,
        tutor::BOOLEAN AS is_tutor,
        extraturn::BOOLEAN AS is_extraturn,
        masslanddenial::BOOLEAN AS is_masslanddenial,
        
        -- Image URIs - clean up URLs
        NULLIF(TRIM(imageurifrontpng::TEXT), '') AS image_uri_front_png,
        NULLIF(TRIM(imageurifrontlarge::TEXT), '') AS image_uri_front_large,
        NULLIF(TRIM(imageurifrontnormal::TEXT), '') AS image_uri_front_normal,
        NULLIF(TRIM(imageurifrontsmall::TEXT), '') AS image_uri_front_small,
        NULLIF(TRIM(imageurifrontartcrop::TEXT), '') AS image_uri_front_art_crop,
        NULLIF(TRIM(imageuribackpng::TEXT), '') AS image_uri_back_png,
        NULLIF(TRIM(imageuribacklarge::TEXT), '') AS image_uri_back_large,
        NULLIF(TRIM(imageuribacknormal::TEXT), '') AS image_uri_back_normal,
        NULLIF(TRIM(imageuribacksmall::TEXT), '') AS image_uri_back_small,
        NULLIF(TRIM(imageuribackartcrop::TEXT), '') AS image_uri_back_art_crop,
        
        -- Features - keep as JSONB for now, will normalize later
        features,
        
        -- Legalities
        UPPER(NULLIF(TRIM("legalities.commander"::TEXT), '')) AS legal_commander,
        UPPER(NULLIF(TRIM("legalities.paupercommandermain"::TEXT), '')) AS legal_paupercommandermain,
        UPPER(NULLIF(TRIM("legalities.paupercommander"::TEXT), '')) AS legal_paupercommander,
        UPPER(NULLIF(TRIM("legalities.oathbreaker"::TEXT), '')) AS legal_oathbreaker,
        UPPER(NULLIF(TRIM("legalities.predh"::TEXT), '')) AS legal_predh,
        UPPER(NULLIF(TRIM("legalities.brawl"::TEXT), '')) AS legal_brawl,
        UPPER(NULLIF(TRIM("legalities.vintage"::TEXT), '')) AS legal_vintage,
        UPPER(NULLIF(TRIM("legalities.legacy"::TEXT), '')) AS legal_legacy,
        UPPER(NULLIF(TRIM("legalities.premodern"::TEXT), '')) AS legal_premodern,
        UPPER(NULLIF(TRIM("legalities.modern"::TEXT), '')) AS legal_modern,
        UPPER(NULLIF(TRIM("legalities.pioneer"::TEXT), '')) AS legal_pioneer,
        UPPER(NULLIF(TRIM("legalities.standard"::TEXT), '')) AS legal_standard,
        UPPER(NULLIF(TRIM("legalities.pauper"::TEXT), '')) AS legal_pauper,
        
        -- Prices - clean up decimals
        ROUND(CAST(NULLIF(TRIM("prices.tcgplayer"::TEXT), '') AS NUMERIC), 2) AS price_tcgplayer,
        ROUND(CAST(NULLIF(TRIM("prices.cardkingdom"::TEXT), '') AS NUMERIC), 2) AS price_cardkingdom,
        ROUND(CAST(NULLIF(TRIM("prices.cardmarket"::TEXT), '') AS NUMERIC), 2) AS price_cardmarket
        
    FROM cs_cards_raw
    WHERE oracleid IS NOT NULL
)

SELECT * FROM cleaned
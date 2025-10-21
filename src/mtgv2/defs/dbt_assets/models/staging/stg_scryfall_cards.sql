-- models/staging/stg_scryfall_cards.sql
-- Cleans and standardizes Scryfall cards data

WITH source AS (
    SELECT * FROM {{ source('dagster', 'pull_scryfall_table') }}
),

cleaned AS (
    SELECT DISTINCT ON (oracle_id)
        -- Core identifiers
        oracle_id,
        id AS scryfall_id,
        
        -- Card info
        name AS card_name,
        type_line,
        oracle_text,
        mana_cost,
        cmc,
        power,
        toughness,
        loyalty,
        defense,
        keywords,
        
        -- Card properties
        reserved AS is_reserved,
        content_warning AS has_content_warning,
        layout,
        hand_modifier,
        life_modifier,
        
        -- Colors
        colors,
        color_identity,
        color_indicator,
        
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
        
        -- Set info
        set,
        set_name,
        set_type,
        rarity,
        released_at,
        
        -- Image URIs (extract from JSON)
        UPPER(NULLIF(TRIM("image_uris.small"::TEXT), '')) AS image_small,
        UPPER(NULLIF(TRIM("image_uris.normal"::TEXT), '')) AS image_normal,
        UPPER(NULLIF(TRIM("image_uris.large"::TEXT), '')) AS image_large,
        UPPER(NULLIF(TRIM("image_uris.png"::TEXT), '')) AS image_png,
        UPPER(NULLIF(TRIM("image_uris.art_crop"::TEXT), '')) AS image_art_crop,
        UPPER(NULLIF(TRIM("image_uris.border_crop"::TEXT), '')) AS image_border_crop,

        -- Prices (extract from JSON)
        UPPER(NULLIF(TRIM("prices.usd"::TEXT), '')) AS price_usd,
        UPPER(NULLIF(TRIM("prices.usd_foil"::TEXT), '')) AS price_usd_foil,
        UPPER(NULLIF(TRIM("prices.usd_etched"::TEXT), '')) AS price_usd_etched,
        UPPER(NULLIF(TRIM("prices.eur"::TEXT), '')) AS price_eur,
        UPPER(NULLIF(TRIM("prices.eur_foil"::TEXT), '')) AS price_eur_foil,
        UPPER(NULLIF(TRIM("prices.tix"::TEXT), '')) AS price_tix,

        -- Meta
        digital AS is_digital,
        promo AS is_promo,
        reprint AS is_reprint,
        variation AS is_variation,
        games,
        
        -- Timestamps
        released_at AS released_date
        
    FROM source
    WHERE oracle_id IS NOT NULL
    ORDER BY oracle_id, released_at DESC
)

SELECT * FROM cleaned
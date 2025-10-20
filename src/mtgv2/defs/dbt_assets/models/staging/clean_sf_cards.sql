-- models/staging/stg_scryfall_cards.sql
-- Flattens image_uris JSON and cleans Scryfall data

WITH source AS (
    SELECT * FROM {{ source('dagster', 'pull_scryfall_table') }}
),

cleaned AS (
    SELECT DISTINCT ON (oracle_id)
        -- Core identifiers
        oracle_id,
        id AS scryfall_id,
        
        -- Card info
        name,
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
        reserved,
        content_warning,
        layout,
        hand_modifier,
        life_modifier,
        
        -- Colors
        colors,
        color_identity,
        color_indicator,
        
        -- Legalities (as JSON for now, we'll extract later)
        "legalities.standard",
        "legalities.future",
        "legalities.historic",
        "legalities.timeless",
        "legalities.gladiator",
        "legalities.pioneer",
        "legalities.modern",
        "legalities.legacy",
        "legalities.pauper",
        "legalities.vintage",
        "legalities.penny",
        "legalities.commander",
        "legalities.oathbreaker",
        "legalities.standardbrawl",
        "legalities.brawl",
        "legalities.alchemy",
        "legalities.paupercommander",
        "legalities.duel",
        "legalities.oldschool",
        "legalities.premodern",
        "legalities.predh",
        
        -- Set info
        set,
        set_name,
        set_type,
        rarity,
        released_at,
        
        -- Image URIs (extract from JSON)
        "image_uris.small",
        "image_uris.normal",
        "image_uris.large",
        "image_uris.png",
        "image_uris.art_crop",
        "image_uris.border_crop",
        -- Prices (extract from JSON)
        "prices.usd",
        "prices.usd_foil",
        "prices.usd_etched",
        "prices.eur",
        "prices.eur_foil",
        "prices.tix",
        -- Meta
        digital,
        promo,
        reprint,
        variation,
        games,
        
        -- Timestamps
        released_at AS released_date
        
    FROM source
    WHERE oracle_id IS NOT NULL
    ORDER BY oracle_id, released_at DESC
)

SELECT * FROM cleaned
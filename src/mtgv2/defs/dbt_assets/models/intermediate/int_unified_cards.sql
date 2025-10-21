-- models/intermediate/int_unified_cards.sql
-- Combines Scryfall and Commander Spellbook card data into unified card records

WITH scryfall_cards AS (
    SELECT 
        oracle_id,
        scryfall_id,
        card_name,
        type_line,
        oracle_text,
        mana_cost,
        cmc,
        power,
        toughness,
        loyalty,
        defense,
        keywords,
        is_reserved,
        has_content_warning,
        layout,
        hand_modifier,
        life_modifier,
        colors,
        color_identity,
        color_indicator,
        set,
        set_name,
        set_type,
        rarity,
        released_date,
        image_small,
        image_normal,
        image_large,
        image_png,
        image_art_crop,
        image_border_crop,
        price_usd,
        price_usd_foil,
        price_usd_etched,
        price_eur,
        price_eur_foil,
        price_tix,
        is_digital,
        is_promo,
        is_reprint,
        is_variation,
        games,
        legal_brawl,
        legal_predh,
        legal_legacy,
        legal_modern,
        legal_pauper,
        legal_pioneer,
        legal_vintage,
        legal_standard,
        legal_commander,
        legal_premodern,
        legal_oathbreaker,
        legal_paupercommander,
        'scryfall' AS data_source
    FROM {{ ref('stg_scryfall_cards') }}
),

cs_cards AS (
    SELECT 
        oracle_id,
        cs_card_id,
        card_name,
        type_line,
        oracle_text,
        cmc,
        color_identity,
        is_spoiler,
        keywords_array,
        latest_printing_set,
        variant_count,
        is_reserved,
        is_reprinted,
        is_gamechanger,
        is_tutor,
        is_extraturn,
        is_masslanddenial,
        image_uri_front_png,
        image_uri_front_large,
        image_uri_front_normal,
        image_uri_front_small,
        image_uri_front_art_crop,
        image_uri_back_png,
        image_uri_back_large,
        image_uri_back_normal,
        image_uri_back_small,
        image_uri_back_art_crop,
        features,
        legal_commander,
        legal_paupercommandermain,
        legal_paupercommander,
        legal_oathbreaker,
        legal_predh,
        legal_brawl,
        legal_vintage,
        legal_legacy,
        legal_premodern,
        legal_modern,
        legal_pioneer,
        legal_standard,
        legal_pauper,
        price_tcgplayer,
        price_cardkingdom,
        price_cardmarket,
        'commander_spellbook' AS data_source
    FROM {{ ref('stg_cs_cards') }}
),

-- Combine both sources, prioritizing Scryfall for core card data
unified_cards AS (
    SELECT 
        COALESCE(sf.oracle_id, cs.oracle_id) AS oracle_id,
        sf.scryfall_id,
        cs.cs_card_id,
        
        -- Card info (prefer Scryfall)
        COALESCE(sf.card_name, cs.card_name) AS card_name,
        COALESCE(sf.type_line, cs.type_line) AS type_line,
        COALESCE(sf.oracle_text, cs.oracle_text) AS oracle_text,
        sf.mana_cost,
        COALESCE(sf.cmc, cs.cmc) AS cmc,
        sf.power,
        sf.toughness,
        sf.loyalty,
        sf.defense,
        COALESCE(sf.keywords, cs.keywords_array::TEXT) AS keywords,
        
        -- Card properties
        COALESCE(sf.is_reserved, cs.is_reserved) AS is_reserved,
        sf.has_content_warning,
        sf.layout,
        sf.hand_modifier,
        sf.life_modifier,
        
        -- Colors (prefer Scryfall)
        sf.colors,
        COALESCE(sf.color_identity, cs.color_identity) AS color_identity,
        sf.color_indicator,
        
        -- Set info (prefer Scryfall)
        sf.set,
        sf.set_name,
        sf.set_type,
        sf.rarity,
        sf.released_date,
        cs.latest_printing_set,
        cs.variant_count,
        
        -- Images (combine both sources)
        COALESCE(sf.image_small, cs.image_uri_front_small) AS image_small,
        COALESCE(sf.image_normal, cs.image_uri_front_normal) AS image_normal,
        COALESCE(sf.image_large, cs.image_uri_front_large) AS image_large,
        COALESCE(sf.image_png, cs.image_uri_front_png) AS image_png,
        COALESCE(sf.image_art_crop, cs.image_uri_front_art_crop) AS image_art_crop,
        COALESCE(sf.image_border_crop, cs.image_uri_front_small) AS image_border_crop,
        cs.image_uri_back_png,
        cs.image_uri_back_large,
        cs.image_uri_back_normal,
        cs.image_uri_back_small,
        cs.image_uri_back_art_crop,
        
        -- Prices (combine both sources)
        sf.price_usd,
        sf.price_usd_foil,
        sf.price_usd_etched,
        sf.price_eur,
        sf.price_eur_foil,
        sf.price_tix,
        cs.price_tcgplayer,
        cs.price_cardkingdom,
        cs.price_cardmarket,
        
        -- Meta flags
        sf.is_digital,
        sf.is_promo,
        COALESCE(sf.is_reprint, cs.is_reprinted) AS is_reprint,
        sf.is_variation,
        sf.games,
        cs.is_spoiler,
        cs.is_gamechanger,
        cs.is_tutor,
        cs.is_extraturn,
        cs.is_masslanddenial,
        
        -- Legalities (prefer Scryfall, fallback to CS)
        COALESCE(sf.legal_brawl, cs.legal_brawl) AS legal_brawl,
        COALESCE(sf.legal_predh, cs.legal_predh) AS legal_predh,
        COALESCE(sf.legal_legacy, cs.legal_legacy) AS legal_legacy,
        COALESCE(sf.legal_modern, cs.legal_modern) AS legal_modern,
        COALESCE(sf.legal_pauper, cs.legal_pauper) AS legal_pauper,
        COALESCE(sf.legal_pioneer, cs.legal_pioneer) AS legal_pioneer,
        COALESCE(sf.legal_vintage, cs.legal_vintage) AS legal_vintage,
        COALESCE(sf.legal_standard, cs.legal_standard) AS legal_standard,
        COALESCE(sf.legal_commander, cs.legal_commander) AS legal_commander,
        COALESCE(sf.legal_premodern, cs.legal_premodern) AS legal_premodern,
        COALESCE(sf.legal_oathbreaker, cs.legal_oathbreaker) AS legal_oathbreaker,
        COALESCE(sf.legal_paupercommander, cs.legal_paupercommander) AS legal_paupercommander,
        cs.legal_paupercommandermain,
        
        -- Additional CS data
        cs.features,
        
        -- Data source tracking
        CASE 
            WHEN sf.oracle_id IS NOT NULL AND cs.oracle_id IS NOT NULL THEN 'both'
            WHEN sf.oracle_id IS NOT NULL THEN 'scryfall_only'
            WHEN cs.oracle_id IS NOT NULL THEN 'cs_only'
        END AS data_coverage
        
    FROM scryfall_cards sf
    FULL OUTER JOIN cs_cards cs ON sf.oracle_id = cs.oracle_id
)

SELECT * FROM unified_cards

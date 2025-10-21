-- models/marts/mart_card_catalog.sql
-- Complete card catalog with pricing, legalities, and metadata

WITH unified_cards AS (
    SELECT 
        oracle_id,
        scryfall_id,
        cs_card_id,
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
        latest_printing_set,
        variant_count,
        image_small,
        image_normal,
        image_large,
        image_png,
        image_art_crop,
        image_border_crop,
        image_uri_back_png,
        image_uri_back_large,
        image_uri_back_normal,
        image_uri_back_small,
        image_uri_back_art_crop,
        price_usd,
        price_usd_foil,
        price_usd_etched,
        price_eur,
        price_eur_foil,
        price_tix,
        price_tcgplayer,
        price_cardkingdom,
        price_cardmarket,
        is_digital,
        is_promo,
        is_reprint,
        is_variation,
        games,
        is_spoiler,
        is_gamechanger,
        is_tutor,
        is_extraturn,
        is_masslanddenial,
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
        legal_paupercommandermain,
        features,
        data_coverage
    FROM {{ ref('int_unified_cards') }}
),

price_analysis AS (
    SELECT 
        oracle_id,
        best_usd_price,
        price_tier,
        foil_premium_percent,
        price_source_count,
        has_usd_price,
        has_usd_foil_price,
        has_eur_price,
        has_tix_price,
        has_tcgplayer_price,
        has_cardkingdom_price,
        has_cardmarket_price,
        price_volatility,
        price_percentile_in_set_rarity,
        release_era
    FROM {{ ref('int_price_analysis') }}
),

legality_analysis AS (
    SELECT 
        oracle_id,
        legal_formats_count,
        banned_formats_count,
        is_commander_legal,
        is_modern_legal,
        is_legacy_legal,
        is_vintage_legal,
        is_pauper_legal,
        is_pioneer_legal,
        is_standard_legal
    FROM {{ ref('int_card_legalities') }}
)

SELECT 
    -- Core identifiers
    uc.oracle_id,
    uc.scryfall_id,
    uc.cs_card_id,
    
    -- Card information
    uc.card_name,
    uc.type_line,
    uc.oracle_text,
    uc.mana_cost,
    uc.cmc,
    uc.power,
    uc.toughness,
    uc.loyalty,
    uc.defense,
    uc.keywords,
    
    -- Card properties
    uc.is_reserved,
    uc.has_content_warning,
    uc.layout,
    uc.hand_modifier,
    uc.life_modifier,
    
    -- Colors and identity
    uc.colors,
    uc.color_identity,
    uc.color_indicator,
    
    -- Set information
    uc.set,
    uc.set_name,
    uc.set_type,
    uc.rarity,
    uc.released_date,
    uc.latest_printing_set,
    uc.variant_count,
    
    -- Images
    uc.image_small,
    uc.image_normal,
    uc.image_large,
    uc.image_png,
    uc.image_art_crop,
    uc.image_border_crop,
    uc.image_uri_back_png,
    uc.image_uri_back_large,
    uc.image_uri_back_normal,
    uc.image_uri_back_small,
    uc.image_uri_back_art_crop,
    
    -- Pricing information
    uc.price_usd,
    uc.price_usd_foil,
    uc.price_usd_etched,
    uc.price_eur,
    uc.price_eur_foil,
    uc.price_tix,
    uc.price_tcgplayer,
    uc.price_cardkingdom,
    uc.price_cardmarket,
    pa.best_usd_price,
    pa.price_tier,
    pa.foil_premium_percent,
    pa.price_source_count,
    pa.has_usd_price,
    pa.has_usd_foil_price,
    pa.has_eur_price,
    pa.has_tix_price,
    pa.has_tcgplayer_price,
    pa.has_cardkingdom_price,
    pa.has_cardmarket_price,
    pa.price_volatility,
    pa.price_percentile_in_set_rarity,
    pa.release_era,
    
    -- Meta flags
    uc.is_digital,
    uc.is_promo,
    uc.is_reprint,
    uc.is_variation,
    uc.games,
    uc.is_spoiler,
    uc.is_gamechanger,
    uc.is_tutor,
    uc.is_extraturn,
    uc.is_masslanddenial,
    
    -- Legality information
    uc.legal_brawl,
    uc.legal_predh,
    uc.legal_legacy,
    uc.legal_modern,
    uc.legal_pauper,
    uc.legal_pioneer,
    uc.legal_vintage,
    uc.legal_standard,
    uc.legal_commander,
    uc.legal_premodern,
    uc.legal_oathbreaker,
    uc.legal_paupercommander,
    uc.legal_paupercommandermain,
    la.legal_formats_count,
    la.banned_formats_count,
    la.is_commander_legal,
    la.is_modern_legal,
    la.is_legacy_legal,
    la.is_vintage_legal,
    la.is_pauper_legal,
    la.is_pioneer_legal,
    la.is_standard_legal,
    
    -- Additional data
    uc.features,
    uc.data_coverage,
    
    -- Calculated fields
    CASE 
        WHEN uc.type_line ILIKE '%creature%' THEN 'Creature'
        WHEN uc.type_line ILIKE '%planeswalker%' THEN 'Planeswalker'
        WHEN uc.type_line ILIKE '%instant%' THEN 'Instant'
        WHEN uc.type_line ILIKE '%sorcery%' THEN 'Sorcery'
        WHEN uc.type_line ILIKE '%artifact%' THEN 'Artifact'
        WHEN uc.type_line ILIKE '%enchantment%' THEN 'Enchantment'
        WHEN uc.type_line ILIKE '%land%' THEN 'Land'
        ELSE 'Other'
    END AS primary_type,
    
    CASE 
        WHEN uc.cmc = 0 THEN '0'
        WHEN uc.cmc <= 2 THEN '1-2'
        WHEN uc.cmc <= 4 THEN '3-4'
        WHEN uc.cmc <= 6 THEN '5-6'
        ELSE '7+'
    END AS cmc_tier,
    
    CASE 
        WHEN uc.color_identity IS NULL OR uc.color_identity = '' THEN 'Colorless'
        WHEN LENGTH(uc.color_identity) = 1 THEN 'Mono'
        WHEN LENGTH(uc.color_identity) = 2 THEN 'Two-color'
        WHEN LENGTH(uc.color_identity) = 3 THEN 'Three-color'
        WHEN LENGTH(uc.color_identity) = 4 THEN 'Four-color'
        WHEN LENGTH(uc.color_identity) = 5 THEN 'Five-color'
        ELSE 'Multi-color'
    END AS color_complexity

FROM unified_cards uc
LEFT JOIN price_analysis pa ON uc.oracle_id = pa.oracle_id
LEFT JOIN legality_analysis la ON uc.oracle_id = la.oracle_id

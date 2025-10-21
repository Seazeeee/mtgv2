-- models/marts/mart_format_legalities.sql
-- Cards organized by format legality for easy filtering

WITH card_legalities AS (
    SELECT 
        oracle_id,
        card_name,
        type_line,
        color_identity,
        cmc,
        legal_formats_count,
        banned_formats_count,
        is_commander_legal,
        is_modern_legal,
        is_legacy_legal,
        is_vintage_legal,
        is_pauper_legal,
        is_pioneer_legal,
        is_standard_legal,
        brawl_legality,
        predh_legality,
        legacy_legality,
        modern_legality,
        pauper_legality,
        pioneer_legality,
        vintage_legality,
        standard_legality,
        commander_legality,
        premodern_legality,
        oathbreaker_legality,
        paupercommander_legality,
        paupercommandermain_legality
    FROM {{ ref('int_card_legalities') }}
),

card_catalog AS (
    SELECT 
        oracle_id,
        card_name,
        type_line,
        color_identity,
        cmc,
        rarity,
        set_name,
        released_date,
        best_usd_price,
        price_tier,
        primary_type,
        cmc_tier,
        color_complexity
    FROM {{ ref('mart_card_catalog') }}
)

SELECT 
    -- Core identifiers
    cl.oracle_id,
    cl.card_name,
    cl.type_line,
    cl.color_identity,
    cl.cmc,
    cc.rarity,
    cc.set_name,
    cc.released_date,
    
    -- Pricing (from card catalog)
    cc.best_usd_price,
    cc.price_tier,
    
    -- Card classification
    cc.primary_type,
    cc.cmc_tier,
    cc.color_complexity,
    
    -- Format legality counts
    cl.legal_formats_count,
    cl.banned_formats_count,
    
    -- Individual format legalities
    cl.brawl_legality,
    cl.predh_legality,
    cl.legacy_legality,
    cl.modern_legality,
    cl.pauper_legality,
    cl.pioneer_legality,
    cl.vintage_legality,
    cl.standard_legality,
    cl.commander_legality,
    cl.premodern_legality,
    cl.oathbreaker_legality,
    cl.paupercommander_legality,
    cl.paupercommandermain_legality,
    
    -- Boolean format flags
    cl.is_commander_legal,
    cl.is_modern_legal,
    cl.is_legacy_legal,
    cl.is_vintage_legal,
    cl.is_pauper_legal,
    cl.is_pioneer_legal,
    cl.is_standard_legal,
    
    -- Calculated fields
    CASE 
        WHEN cl.legal_formats_count >= 8 THEN 'Universal'
        WHEN cl.legal_formats_count >= 5 THEN 'Multi-format'
        WHEN cl.legal_formats_count >= 3 THEN 'Limited'
        WHEN cl.legal_formats_count >= 1 THEN 'Single-format'
        ELSE 'Banned'
    END AS format_availability,
    
    CASE 
        WHEN cl.banned_formats_count >= 5 THEN 'Heavily Banned'
        WHEN cl.banned_formats_count >= 3 THEN 'Moderately Banned'
        WHEN cl.banned_formats_count >= 1 THEN 'Somewhat Banned'
        ELSE 'Not Banned'
    END AS ban_status,
    
    -- Format-specific flags
    CASE 
        WHEN cl.is_commander_legal AND cl.is_modern_legal AND cl.is_legacy_legal THEN 'Eternal'
        WHEN cl.is_commander_legal AND cl.is_modern_legal THEN 'Modern+'
        WHEN cl.is_commander_legal THEN 'Commander'
        WHEN cl.is_modern_legal THEN 'Modern'
        WHEN cl.is_legacy_legal THEN 'Legacy'
        WHEN cl.is_pioneer_legal THEN 'Pioneer'
        WHEN cl.is_standard_legal THEN 'Standard'
        WHEN cl.is_pauper_legal THEN 'Pauper'
        ELSE 'Other'
    END AS primary_format,
    
    -- Power level indicators
    CASE 
        WHEN cl.is_vintage_legal AND cl.is_legacy_legal AND cl.is_modern_legal THEN 'High Power'
        WHEN cl.is_legacy_legal AND cl.is_modern_legal THEN 'Medium-High Power'
        WHEN cl.is_modern_legal AND cl.is_pioneer_legal THEN 'Medium Power'
        WHEN cl.is_pioneer_legal AND cl.is_standard_legal THEN 'Low-Medium Power'
        WHEN cl.is_standard_legal THEN 'Low Power'
        WHEN cl.is_pauper_legal THEN 'Pauper Power'
        ELSE 'Variable Power'
    END AS power_level

FROM card_legalities cl
LEFT JOIN card_catalog cc ON cl.oracle_id = cc.oracle_id

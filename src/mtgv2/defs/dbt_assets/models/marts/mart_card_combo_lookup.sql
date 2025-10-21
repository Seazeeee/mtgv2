-- models/marts/mart_card_combo_lookup.sql
-- Flattened card-combo relationships for efficient lookups

WITH combo_data AS (
    SELECT 
        variant_id,
        description,
        notes,
        complexity_level,
        difficulty_level,
        price_tier,
        legal_formats_count,
        estimated_card_count,
        estimated_feature_count,
        mana_value_needed,
        popularity,
        bracket_tag,
        best_price,
        combo_cards,
        combo_uses,
        combo_includes,
        combo_produces,
        combo_requires,
        legal_commander,
        legal_modern,
        legal_legacy,
        legal_vintage,
        legal_pauper,
        legal_pioneer,
        legal_standard,
        legal_brawl,
        legal_oathbreaker,
        legal_paupercommander
    FROM {{ ref('mart_combo_analysis') }}
),

-- Extract card names from combo_cards field using LATERAL join
-- This approach properly handles set-returning functions
card_extractions AS (
    SELECT 
        cd.variant_id,
        cd.description,
        cd.notes,
        cd.complexity_level,
        cd.difficulty_level,
        cd.price_tier,
        cd.legal_formats_count,
        cd.estimated_card_count,
        cd.estimated_feature_count,
        cd.mana_value_needed,
        cd.popularity,
        cd.bracket_tag,
        cd.best_price,
        cd.combo_cards,
        cd.combo_uses,
        cd.combo_includes,
        cd.combo_produces,
        cd.combo_requires,
        cd.legal_commander,
        cd.legal_modern,
        cd.legal_legacy,
        cd.legal_vintage,
        cd.legal_pauper,
        cd.legal_pioneer,
        cd.legal_standard,
        cd.legal_brawl,
        cd.legal_oathbreaker,
        cd.legal_paupercommander,
        
        -- Extract individual card names using LATERAL join
        TRIM(card_name) AS card_name,
        
        -- Extract card positions (for ordering)
        ROW_NUMBER() OVER (
            PARTITION BY cd.variant_id 
            ORDER BY card_name
        ) AS card_position
        
    FROM combo_data cd
    CROSS JOIN LATERAL (
        SELECT unnest(string_to_array(
            REGEXP_REPLACE(cd.combo_cards, '[\[\]{}"]', '', 'g'), 
            ','
        )) AS card_name
    ) AS card_split
    WHERE cd.combo_cards IS NOT NULL 
      AND cd.combo_cards != '' 
      AND cd.combo_cards != 'null'
),

-- Clean up card names and filter out empty ones
cleaned_cards AS (
    SELECT 
        variant_id,
        description,
        notes,
        complexity_level,
        difficulty_level,
        price_tier,
        legal_formats_count,
        estimated_card_count,
        estimated_feature_count,
        mana_value_needed,
        popularity,
        bracket_tag,
        best_price,
        combo_cards,
        combo_uses,
        combo_includes,
        combo_produces,
        combo_requires,
        legal_commander,
        legal_modern,
        legal_legacy,
        legal_vintage,
        legal_pauper,
        legal_pioneer,
        legal_standard,
        legal_brawl,
        legal_oathbreaker,
        legal_paupercommander,
        TRIM(card_name) AS card_name,
        card_position
    FROM card_extractions
    WHERE TRIM(card_name) IS NOT NULL 
      AND TRIM(card_name) != ''
      AND LENGTH(TRIM(card_name)) > 2
),

-- Add card metadata from card catalog
card_combo_lookup AS (
    SELECT 
        cc.variant_id,
        cc.description,
        cc.notes,
        cc.complexity_level,
        cc.difficulty_level,
        cc.price_tier,
        cc.legal_formats_count,
        cc.estimated_card_count,
        cc.estimated_feature_count,
        cc.mana_value_needed,
        cc.popularity,
        cc.bracket_tag,
        cc.best_price,
        cc.combo_cards,
        cc.combo_uses,
        cc.combo_includes,
        cc.combo_produces,
        cc.combo_requires,
        cc.legal_commander,
        cc.legal_modern,
        cc.legal_legacy,
        cc.legal_vintage,
        cc.legal_pauper,
        cc.legal_pioneer,
        cc.legal_standard,
        cc.legal_brawl,
        cc.legal_oathbreaker,
        cc.legal_paupercommander,
        cc.card_name,
        cc.card_position,
        
        -- Card metadata from catalog
        cat.oracle_id,
        cat.type_line,
        cat.color_identity,
        cat.cmc,
        cat.rarity,
        cat.set_name,
        cat.best_usd_price AS card_price,
        cat.primary_type,
        cat.cmc_tier,
        cat.color_complexity,
        
        -- Calculated fields
        CASE 
            WHEN cc.card_position = 1 THEN 'Primary'
            WHEN cc.card_position = 2 THEN 'Secondary'
            WHEN cc.card_position <= 4 THEN 'Supporting'
            ELSE 'Additional'
        END AS card_role,
        
        CASE 
            WHEN cc.estimated_card_count <= 2 THEN 'Simple Combo'
            WHEN cc.estimated_card_count <= 4 THEN 'Moderate Combo'
            WHEN cc.estimated_card_count <= 6 THEN 'Complex Combo'
            ELSE 'Very Complex Combo'
        END AS combo_type,
        
        -- Search optimization fields
        LOWER(cc.card_name) AS card_name_lower,
        LOWER(cc.description) AS description_lower,
        LOWER(cc.notes) AS notes_lower
        
    FROM cleaned_cards cc
    LEFT JOIN {{ ref('mart_card_catalog') }} cat 
        ON LOWER(TRIM(cc.card_name)) = LOWER(TRIM(cat.card_name))
)

SELECT 
    -- Core identifiers
    variant_id,
    card_name,
    card_name_lower,
    card_position,
    card_role,
    
    -- Card metadata
    oracle_id,
    type_line,
    color_identity,
    cmc,
    rarity,
    set_name,
    card_price,
    primary_type,
    cmc_tier,
    color_complexity,
    
    -- Combo information
    description,
    description_lower,
    notes,
    notes_lower,
    complexity_level,
    difficulty_level,
    combo_type,
    price_tier,
    
    -- Combo metrics
    legal_formats_count,
    estimated_card_count,
    estimated_feature_count,
    mana_value_needed,
    popularity,
    bracket_tag,
    best_price,
    
    -- Format legality
    legal_commander,
    legal_modern,
    legal_legacy,
    legal_vintage,
    legal_pauper,
    legal_pioneer,
    legal_standard,
    legal_brawl,
    legal_oathbreaker,
    legal_paupercommander,
    
    -- Raw combo data
    combo_cards,
    combo_uses,
    combo_includes,
    combo_produces,
    combo_requires,
    
    -- Search optimization
    CASE 
        WHEN card_role = 'Primary' AND complexity_level = 'Simple' THEN 1
        WHEN card_role = 'Primary' AND complexity_level = 'Moderate' THEN 2
        WHEN card_role = 'Primary' AND complexity_level = 'Complex' THEN 3
        WHEN card_role = 'Secondary' AND complexity_level = 'Simple' THEN 4
        WHEN card_role = 'Secondary' AND complexity_level = 'Moderate' THEN 5
        ELSE 6
    END AS search_priority

FROM card_combo_lookup
ORDER BY 
    card_name_lower,
    search_priority,
    popularity DESC,
    estimated_card_count ASC

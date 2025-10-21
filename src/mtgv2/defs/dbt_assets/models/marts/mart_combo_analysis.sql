-- models/marts/mart_combo_analysis.sql
-- Combo strategies with enriched metadata and analysis

WITH combo_data AS (
    SELECT 
        variant_id,
        status,
        is_spoiler,
        color_identity,
        description,
        notes,
        mana_needed,
        variant_count,
        mana_value_needed,
        easy_prerequisites,
        notable_prerequisites,
        popularity,
        bracket_tag,
        price_tcgplayer,
        price_cardmarket,
        price_cardkingdom,
        estimated_card_count,
        estimated_feature_count,
        combo_cards,
        combo_uses,
        combo_includes,
        combo_produces,
        combo_requires,
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
        legal_paupercommandermain
    FROM {{ ref('int_combo_enrichment') }}
),

-- Calculate combo complexity and metrics
combo_metrics AS (
    SELECT 
        *,
        
        -- Complexity scoring
        CASE 
            WHEN estimated_card_count <= 2 THEN 'Simple'
            WHEN estimated_card_count <= 4 THEN 'Moderate'
            WHEN estimated_card_count <= 6 THEN 'Complex'
            ELSE 'Very Complex'
        END AS complexity_level,
        
        -- Mana efficiency
        CASE 
            WHEN mana_value_needed <= 3 THEN 'Low'
            WHEN mana_value_needed <= 6 THEN 'Medium'
            WHEN mana_value_needed <= 9 THEN 'High'
            ELSE 'Very High'
        END AS mana_efficiency,
        
        -- Price analysis
        COALESCE(price_tcgplayer::NUMERIC, price_cardmarket::NUMERIC, price_cardkingdom::NUMERIC) AS best_price,
        
        CASE 
            WHEN COALESCE(price_tcgplayer::NUMERIC, price_cardmarket::NUMERIC, price_cardkingdom::NUMERIC) < 10 THEN 'Budget'
            WHEN COALESCE(price_tcgplayer::NUMERIC, price_cardmarket::NUMERIC, price_cardkingdom::NUMERIC) < 50 THEN 'Moderate'
            WHEN COALESCE(price_tcgplayer::NUMERIC, price_cardmarket::NUMERIC, price_cardkingdom::NUMERIC) < 100 THEN 'Expensive'
            ELSE 'Premium'
        END AS price_tier,
        
        -- Format legality count
        (CASE WHEN legal_commander = 'LEGAL' THEN 1 ELSE 0 END +
         CASE WHEN legal_modern = 'LEGAL' THEN 1 ELSE 0 END +
         CASE WHEN legal_legacy = 'LEGAL' THEN 1 ELSE 0 END +
         CASE WHEN legal_vintage = 'LEGAL' THEN 1 ELSE 0 END +
         CASE WHEN legal_pauper = 'LEGAL' THEN 1 ELSE 0 END +
         CASE WHEN legal_pioneer = 'LEGAL' THEN 1 ELSE 0 END +
         CASE WHEN legal_standard = 'LEGAL' THEN 1 ELSE 0 END +
         CASE WHEN legal_brawl = 'LEGAL' THEN 1 ELSE 0 END +
         CASE WHEN legal_oathbreaker = 'LEGAL' THEN 1 ELSE 0 END +
         CASE WHEN legal_paupercommander = 'LEGAL' THEN 1 ELSE 0 END) AS legal_formats_count,
        
        -- Color identity analysis
        CASE 
            WHEN color_identity IS NULL OR color_identity = '' THEN 'Colorless'
            WHEN LENGTH(color_identity) = 1 THEN 'Mono'
            WHEN LENGTH(color_identity) = 2 THEN 'Two-color'
            WHEN LENGTH(color_identity) = 3 THEN 'Three-color'
            WHEN LENGTH(color_identity) = 4 THEN 'Four-color'
            WHEN LENGTH(color_identity) = 5 THEN 'Five-color'
            ELSE 'Multi-color'
        END AS color_complexity,
        
        -- Popularity tier
        CASE 
            WHEN popularity <= 2 THEN 'Low'
            WHEN popularity <= 5 THEN 'Medium'
            WHEN popularity <= 8 THEN 'High'
            ELSE 'Very High'
        END AS popularity_tier
        
    FROM combo_data
)

SELECT 
    -- Core identifiers
    variant_id,
    status,
    is_spoiler,
    color_identity,
    
    -- Combo information
    description,
    notes,
    mana_needed,
    variant_count,
    mana_value_needed,
    easy_prerequisites,
    notable_prerequisites,
    popularity,
    bracket_tag,
    
    -- Pricing
    price_tcgplayer,
    price_cardmarket,
    price_cardkingdom,
    best_price,
    price_tier,
    
    -- Complexity metrics
    estimated_card_count,
    estimated_feature_count,
    complexity_level,
    mana_efficiency,
    color_complexity,
    popularity_tier,
    
    -- Format legality
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
    legal_formats_count,
    
    -- Raw combo data (for future processing)
    combo_cards,
    combo_uses,
    combo_includes,
    combo_produces,
    combo_requires,
    
    -- Calculated fields
    CASE 
        WHEN complexity_level = 'Simple' AND mana_efficiency = 'Low' THEN 'Beginner'
        WHEN complexity_level IN ('Simple', 'Moderate') AND mana_efficiency IN ('Low', 'Medium') THEN 'Intermediate'
        WHEN complexity_level IN ('Moderate', 'Complex') AND mana_efficiency IN ('Medium', 'High') THEN 'Advanced'
        ELSE 'Expert'
    END AS difficulty_level,
    
    CASE 
        WHEN legal_commander = 'LEGAL' AND legal_modern = 'LEGAL' AND legal_legacy = 'LEGAL' THEN 'Universal'
        WHEN legal_commander = 'LEGAL' AND legal_modern = 'LEGAL' THEN 'Modern+'
        WHEN legal_commander = 'LEGAL' THEN 'Commander Only'
        WHEN legal_modern = 'LEGAL' THEN 'Modern Only'
        WHEN legal_legacy = 'LEGAL' THEN 'Legacy Only'
        ELSE 'Limited'
    END AS format_availability

FROM combo_metrics

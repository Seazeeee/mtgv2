-- models/intermediate/int_combo_enrichment.sql
-- Enriches combo data with basic metrics and summaries

WITH combos AS (
    SELECT 
        variant_id,
        status,
        is_spoiler,
        color_identity,
        combo_cards,
        combo_uses,
        combo_includes,
        combo_produces,
        combo_requires,
        notes,
        description,
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
    FROM {{ ref('stg_cs_variants') }}
),

-- Create basic combo summary without JSONB parsing
combo_summary AS (
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
        
        -- Basic metrics without JSONB parsing
        CASE 
            WHEN combo_cards IS NOT NULL AND combo_cards != '' AND combo_cards != 'null' 
            THEN LENGTH(combo_cards) - LENGTH(REPLACE(combo_cards, ',', '')) + 1
            ELSE 0
        END AS estimated_card_count,
        
        CASE 
            WHEN combo_produces IS NOT NULL AND combo_produces != '' AND combo_produces != 'null' 
            THEN LENGTH(combo_produces) - LENGTH(REPLACE(combo_produces, ',', '')) + 1
            ELSE 0
        END AS estimated_feature_count,
        
        -- Keep raw JSONB data for later processing
        combo_cards,
        combo_uses,
        combo_includes,
        combo_produces,
        combo_requires,
        
        -- Legalities
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
        
    FROM combos
)

SELECT * FROM combo_summary

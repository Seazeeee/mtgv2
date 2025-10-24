-- models/intermediate/int_card_legalities.sql
-- Normalizes and standardizes legality data across formats

WITH scryfall_cards AS (
    SELECT 
        oracle_id,
        card_name,
        type_line,
        color_identity,
        cmc,
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
        legal_paupercommander
    FROM {{ ref('stg_scryfall_cards') }}
),

cs_cards AS (
    SELECT DISTINCT ON (oracle_id)
        oracle_id,
        card_name,
        type_line,
        color_identity,
        cmc,
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
        legal_pauper
    FROM {{ ref('stg_cs_cards') }}
    ORDER BY oracle_id
),

-- Combine legalities from both sources
unified_legalities AS (
    SELECT 
        COALESCE(sf.oracle_id, cs.oracle_id) AS oracle_id,
        COALESCE(sf.card_name, cs.card_name) AS card_name,
        COALESCE(sf.type_line, cs.type_line) AS type_line,
        COALESCE(sf.color_identity, cs.color_identity) AS color_identity,
        COALESCE(sf.cmc, cs.cmc) AS cmc,
        
        -- Prefer Scryfall legalities, fallback to CS
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
        cs.legal_paupercommandermain
        
    FROM scryfall_cards sf
    FULL OUTER JOIN cs_cards cs ON sf.oracle_id = cs.oracle_id
),

-- Normalize legality values to standard format
normalized_legalities AS (
    SELECT 
        oracle_id,
        card_name,
        type_line,
        color_identity,
        cmc,
        
        -- Standardize legality values (LEGAL, BANNED, RESTRICTED, NULL)
        CASE 
            WHEN UPPER(legal_brawl) = 'LEGAL' THEN 'LEGAL'
            WHEN UPPER(legal_brawl) = 'BANNED' THEN 'BANNED'
            WHEN UPPER(legal_brawl) = 'RESTRICTED' THEN 'RESTRICTED'
            ELSE NULL
        END AS brawl_legality,
        
        CASE 
            WHEN UPPER(legal_predh) = 'LEGAL' THEN 'LEGAL'
            WHEN UPPER(legal_predh) = 'BANNED' THEN 'BANNED'
            WHEN UPPER(legal_predh) = 'RESTRICTED' THEN 'RESTRICTED'
            ELSE NULL
        END AS predh_legality,
        
        CASE 
            WHEN UPPER(legal_legacy) = 'LEGAL' THEN 'LEGAL'
            WHEN UPPER(legal_legacy) = 'BANNED' THEN 'BANNED'
            WHEN UPPER(legal_legacy) = 'RESTRICTED' THEN 'RESTRICTED'
            ELSE NULL
        END AS legacy_legality,
        
        CASE 
            WHEN UPPER(legal_modern) = 'LEGAL' THEN 'LEGAL'
            WHEN UPPER(legal_modern) = 'BANNED' THEN 'BANNED'
            WHEN UPPER(legal_modern) = 'RESTRICTED' THEN 'RESTRICTED'
            ELSE NULL
        END AS modern_legality,
        
        CASE 
            WHEN UPPER(legal_pauper) = 'LEGAL' THEN 'LEGAL'
            WHEN UPPER(legal_pauper) = 'BANNED' THEN 'BANNED'
            WHEN UPPER(legal_pauper) = 'RESTRICTED' THEN 'RESTRICTED'
            ELSE NULL
        END AS pauper_legality,
        
        CASE 
            WHEN UPPER(legal_pioneer) = 'LEGAL' THEN 'LEGAL'
            WHEN UPPER(legal_pioneer) = 'BANNED' THEN 'BANNED'
            WHEN UPPER(legal_pioneer) = 'RESTRICTED' THEN 'RESTRICTED'
            ELSE NULL
        END AS pioneer_legality,
        
        CASE 
            WHEN UPPER(legal_vintage) = 'LEGAL' THEN 'LEGAL'
            WHEN UPPER(legal_vintage) = 'BANNED' THEN 'BANNED'
            WHEN UPPER(legal_vintage) = 'RESTRICTED' THEN 'RESTRICTED'
            ELSE NULL
        END AS vintage_legality,
        
        CASE 
            WHEN UPPER(legal_standard) = 'LEGAL' THEN 'LEGAL'
            WHEN UPPER(legal_standard) = 'BANNED' THEN 'BANNED'
            WHEN UPPER(legal_standard) = 'RESTRICTED' THEN 'RESTRICTED'
            ELSE NULL
        END AS standard_legality,
        
        CASE 
            WHEN UPPER(legal_commander) = 'LEGAL' THEN 'LEGAL'
            WHEN UPPER(legal_commander) = 'BANNED' THEN 'BANNED'
            WHEN UPPER(legal_commander) = 'RESTRICTED' THEN 'RESTRICTED'
            ELSE NULL
        END AS commander_legality,
        
        CASE 
            WHEN UPPER(legal_premodern) = 'LEGAL' THEN 'LEGAL'
            WHEN UPPER(legal_premodern) = 'BANNED' THEN 'BANNED'
            WHEN UPPER(legal_premodern) = 'RESTRICTED' THEN 'RESTRICTED'
            ELSE NULL
        END AS premodern_legality,
        
        CASE 
            WHEN UPPER(legal_oathbreaker) = 'LEGAL' THEN 'LEGAL'
            WHEN UPPER(legal_oathbreaker) = 'BANNED' THEN 'BANNED'
            WHEN UPPER(legal_oathbreaker) = 'RESTRICTED' THEN 'RESTRICTED'
            ELSE NULL
        END AS oathbreaker_legality,
        
        CASE 
            WHEN UPPER(legal_paupercommander) = 'LEGAL' THEN 'LEGAL'
            WHEN UPPER(legal_paupercommander) = 'BANNED' THEN 'BANNED'
            WHEN UPPER(legal_paupercommander) = 'RESTRICTED' THEN 'RESTRICTED'
            ELSE NULL
        END AS paupercommander_legality,
        
        CASE 
            WHEN UPPER(legal_paupercommandermain) = 'LEGAL' THEN 'LEGAL'
            WHEN UPPER(legal_paupercommandermain) = 'BANNED' THEN 'BANNED'
            WHEN UPPER(legal_paupercommandermain) = 'RESTRICTED' THEN 'RESTRICTED'
            ELSE NULL
        END AS paupercommandermain_legality
        
    FROM unified_legalities
),

-- Calculate format availability metrics
format_metrics AS (
    SELECT 
        oracle_id,
        card_name,
        type_line,
        color_identity,
        cmc,
        
        -- Individual legalities
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
        paupercommandermain_legality,
        
        -- Aggregated metrics
        COUNT(CASE WHEN brawl_legality = 'LEGAL' THEN 1 END) +
        COUNT(CASE WHEN predh_legality = 'LEGAL' THEN 1 END) +
        COUNT(CASE WHEN legacy_legality = 'LEGAL' THEN 1 END) +
        COUNT(CASE WHEN modern_legality = 'LEGAL' THEN 1 END) +
        COUNT(CASE WHEN pauper_legality = 'LEGAL' THEN 1 END) +
        COUNT(CASE WHEN pioneer_legality = 'LEGAL' THEN 1 END) +
        COUNT(CASE WHEN vintage_legality = 'LEGAL' THEN 1 END) +
        COUNT(CASE WHEN standard_legality = 'LEGAL' THEN 1 END) +
        COUNT(CASE WHEN commander_legality = 'LEGAL' THEN 1 END) +
        COUNT(CASE WHEN premodern_legality = 'LEGAL' THEN 1 END) +
        COUNT(CASE WHEN oathbreaker_legality = 'LEGAL' THEN 1 END) +
        COUNT(CASE WHEN paupercommander_legality = 'LEGAL' THEN 1 END) +
        COUNT(CASE WHEN paupercommandermain_legality = 'LEGAL' THEN 1 END) AS legal_formats_count,
        
        COUNT(CASE WHEN brawl_legality = 'BANNED' THEN 1 END) +
        COUNT(CASE WHEN predh_legality = 'BANNED' THEN 1 END) +
        COUNT(CASE WHEN legacy_legality = 'BANNED' THEN 1 END) +
        COUNT(CASE WHEN modern_legality = 'BANNED' THEN 1 END) +
        COUNT(CASE WHEN pauper_legality = 'BANNED' THEN 1 END) +
        COUNT(CASE WHEN pioneer_legality = 'BANNED' THEN 1 END) +
        COUNT(CASE WHEN vintage_legality = 'BANNED' THEN 1 END) +
        COUNT(CASE WHEN standard_legality = 'BANNED' THEN 1 END) +
        COUNT(CASE WHEN commander_legality = 'BANNED' THEN 1 END) +
        COUNT(CASE WHEN premodern_legality = 'BANNED' THEN 1 END) +
        COUNT(CASE WHEN oathbreaker_legality = 'BANNED' THEN 1 END) +
        COUNT(CASE WHEN paupercommander_legality = 'BANNED' THEN 1 END) +
        COUNT(CASE WHEN paupercommandermain_legality = 'BANNED' THEN 1 END) AS banned_formats_count,
        
        -- Format availability flags
        CASE WHEN commander_legality = 'LEGAL' THEN TRUE ELSE FALSE END AS is_commander_legal,
        CASE WHEN modern_legality = 'LEGAL' THEN TRUE ELSE FALSE END AS is_modern_legal,
        CASE WHEN legacy_legality = 'LEGAL' THEN TRUE ELSE FALSE END AS is_legacy_legal,
        CASE WHEN vintage_legality = 'LEGAL' THEN TRUE ELSE FALSE END AS is_vintage_legal,
        CASE WHEN pauper_legality = 'LEGAL' THEN TRUE ELSE FALSE END AS is_pauper_legal,
        CASE WHEN pioneer_legality = 'LEGAL' THEN TRUE ELSE FALSE END AS is_pioneer_legal,
        CASE WHEN standard_legality = 'LEGAL' THEN TRUE ELSE FALSE END AS is_standard_legal
        
    FROM normalized_legalities
    GROUP BY 
        oracle_id, card_name, type_line, color_identity, cmc,
        brawl_legality, predh_legality, legacy_legality, modern_legality,
        pauper_legality, pioneer_legality, vintage_legality, standard_legality,
        commander_legality, premodern_legality, oathbreaker_legality,
        paupercommander_legality, paupercommandermain_legality
)

SELECT * FROM format_metrics

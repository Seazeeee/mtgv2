-- models/intermediate/int_price_analysis.sql
-- Aggregates and analyzes pricing data across sources

WITH scryfall_cards AS (
    SELECT 
        oracle_id,
        card_name,
        type_line,
        color_identity,
        rarity,
        set_name,
        released_date,
        is_reserved,
        is_reprint,
        is_promo,
        is_digital,
        price_usd,
        price_usd_foil,
        price_usd_etched,
        price_eur,
        price_eur_foil,
        price_tix
    FROM {{ ref('stg_scryfall_cards') }}
),

cs_cards AS (
    SELECT DISTINCT ON (oracle_id)
        oracle_id,
        card_name,
        type_line,
        color_identity,
        latest_printing_set,
        is_reserved,
        is_reprinted,
        price_tcgplayer,
        price_cardkingdom,
        price_cardmarket
    FROM {{ ref('stg_cs_cards') }}
    ORDER BY oracle_id
),

-- Combine pricing data from both sources
unified_prices AS (
    SELECT 
        COALESCE(sf.oracle_id, cs.oracle_id) AS oracle_id,
        COALESCE(sf.card_name, cs.card_name) AS card_name,
        COALESCE(sf.type_line, cs.type_line) AS type_line,
        COALESCE(sf.color_identity, cs.color_identity) AS color_identity,
        COALESCE(sf.rarity, NULL) AS rarity,
        COALESCE(sf.set_name, cs.latest_printing_set) AS set_name,
        sf.released_date,
        COALESCE(sf.is_reserved, cs.is_reserved) AS is_reserved,
        COALESCE(sf.is_reprint, cs.is_reprinted) AS is_reprint,
        sf.is_promo,
        sf.is_digital,
        
        -- Scryfall prices
        sf.price_usd,
        sf.price_usd_foil,
        sf.price_usd_etched,
        sf.price_eur,
        sf.price_eur_foil,
        sf.price_tix,
        
        -- CS prices
        cs.price_tcgplayer,
        cs.price_cardkingdom,
        cs.price_cardmarket
        
    FROM scryfall_cards sf
    FULL OUTER JOIN cs_cards cs ON sf.oracle_id = cs.oracle_id
),

-- Calculate price statistics and availability
price_analysis AS (
    SELECT 
        oracle_id,
        card_name,
        type_line,
        color_identity,
        rarity,
        set_name,
        released_date,
        is_reserved,
        is_reprint,
        is_promo,
        is_digital,
        
        -- USD Prices
        price_usd,
        price_usd_foil,
        price_usd_etched,
        
        -- EUR Prices  
        price_eur,
        price_eur_foil,
        
        -- MTGO Prices
        price_tix,
        
        -- Marketplace Prices
        price_tcgplayer,
        price_cardkingdom,
        price_cardmarket,
        
        -- Price availability flags
        CASE WHEN price_usd IS NOT NULL THEN TRUE ELSE FALSE END AS has_usd_price,
        CASE WHEN price_usd_foil IS NOT NULL THEN TRUE ELSE FALSE END AS has_usd_foil_price,
        CASE WHEN price_eur IS NOT NULL THEN TRUE ELSE FALSE END AS has_eur_price,
        CASE WHEN price_tix IS NOT NULL THEN TRUE ELSE FALSE END AS has_tix_price,
        CASE WHEN price_tcgplayer IS NOT NULL THEN TRUE ELSE FALSE END AS has_tcgplayer_price,
        CASE WHEN price_cardkingdom IS NOT NULL THEN TRUE ELSE FALSE END AS has_cardkingdom_price,
        CASE WHEN price_cardmarket IS NOT NULL THEN TRUE ELSE FALSE END AS has_cardmarket_price,
        
        -- Price source count
        (CASE WHEN price_usd IS NOT NULL THEN 1 ELSE 0 END +
         CASE WHEN price_eur IS NOT NULL THEN 1 ELSE 0 END +
         CASE WHEN price_tix IS NOT NULL THEN 1 ELSE 0 END +
         CASE WHEN price_tcgplayer IS NOT NULL THEN 1 ELSE 0 END +
         CASE WHEN price_cardkingdom IS NOT NULL THEN 1 ELSE 0 END +
         CASE WHEN price_cardmarket IS NOT NULL THEN 1 ELSE 0 END) AS price_source_count,
        
        -- Best available price (prefer USD, then EUR, then others)
        COALESCE(
            price_usd::NUMERIC,
            price_eur::NUMERIC * 1.1, -- Rough EUR to USD conversion
            price_tcgplayer::NUMERIC,
            price_cardkingdom::NUMERIC,
            price_cardmarket::NUMERIC,
            price_tix::NUMERIC * 0.1 -- Rough TIX to USD conversion
        ) AS best_usd_price,
        
        -- Price range indicators
        CASE 
            WHEN COALESCE(price_usd::NUMERIC, price_eur::NUMERIC * 1.1, price_tcgplayer::NUMERIC, price_cardkingdom::NUMERIC, price_cardmarket::NUMERIC) < 1 THEN 'budget'
            WHEN COALESCE(price_usd::NUMERIC, price_eur::NUMERIC * 1.1, price_tcgplayer::NUMERIC, price_cardkingdom::NUMERIC, price_cardmarket::NUMERIC) < 10 THEN 'moderate'
            WHEN COALESCE(price_usd::NUMERIC, price_eur::NUMERIC * 1.1, price_tcgplayer::NUMERIC, price_cardkingdom::NUMERIC, price_cardmarket::NUMERIC) < 50 THEN 'expensive'
            ELSE 'premium'
        END AS price_tier,
        
        -- Foil premium calculation
        CASE 
            WHEN price_usd::NUMERIC IS NOT NULL AND price_usd_foil::NUMERIC IS NOT NULL AND price_usd::NUMERIC > 0 
            THEN ROUND((price_usd_foil::NUMERIC - price_usd::NUMERIC) / price_usd::NUMERIC * 100, 2)
            ELSE NULL
        END AS foil_premium_percent
        
    FROM unified_prices
),

-- Add price volatility and trend indicators
price_metrics AS (
    SELECT 
        *,
        
        -- Price volatility (standard deviation of available prices)
        CASE 
            WHEN price_source_count >= 2 THEN
                ROUND(STDDEV(price_usd::NUMERIC) OVER (PARTITION BY set_name, rarity), 2)
            ELSE NULL
        END AS price_volatility,
        
        -- Relative price within set/rarity
        CASE 
            WHEN price_usd::NUMERIC IS NOT NULL THEN
                PERCENT_RANK() OVER (PARTITION BY set_name, rarity ORDER BY price_usd::NUMERIC)
            ELSE NULL
        END AS price_percentile_in_set_rarity,
        
        -- Price trend indicators (based on release date)
        CASE 
            WHEN released_date::DATE >= CURRENT_DATE - INTERVAL '1 year' THEN 'recent'
            WHEN released_date::DATE >= CURRENT_DATE - INTERVAL '3 years' THEN 'modern'
            WHEN released_date::DATE >= CURRENT_DATE - INTERVAL '10 years' THEN 'established'
            ELSE 'vintage'
        END AS release_era
        
    FROM price_analysis
)

SELECT * FROM price_metrics

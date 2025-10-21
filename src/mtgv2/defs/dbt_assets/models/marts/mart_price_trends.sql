-- models/marts/mart_price_trends.sql
-- Price analysis and trends for market insights

WITH price_data AS (
    SELECT 
        oracle_id,
        card_name,
        type_line,
        color_identity,
        rarity,
        set_name,
        released_date,
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
        release_era,
        primary_type,
        cmc_tier,
        color_complexity
    FROM {{ ref('mart_card_catalog') }}
),

-- Calculate price statistics by various dimensions
price_stats AS (
    SELECT 
        *,
        
        -- Price availability score
        (CASE WHEN has_usd_price THEN 1 ELSE 0 END +
         CASE WHEN has_usd_foil_price THEN 1 ELSE 0 END +
         CASE WHEN has_eur_price THEN 1 ELSE 0 END +
         CASE WHEN has_tix_price THEN 1 ELSE 0 END +
         CASE WHEN has_tcgplayer_price THEN 1 ELSE 0 END +
         CASE WHEN has_cardkingdom_price THEN 1 ELSE 0 END +
         CASE WHEN has_cardmarket_price THEN 1 ELSE 0 END) AS price_availability_score,
        
        -- Market coverage
        CASE 
            WHEN price_source_count >= 5 THEN 'High Coverage'
            WHEN price_source_count >= 3 THEN 'Medium Coverage'
            WHEN price_source_count >= 1 THEN 'Low Coverage'
            ELSE 'No Coverage'
        END AS market_coverage,
        
        -- Volatility assessment
        CASE 
            WHEN price_volatility IS NULL THEN 'Unknown'
            WHEN price_volatility <= 5 THEN 'Stable'
            WHEN price_volatility <= 15 THEN 'Moderate'
            WHEN price_volatility <= 30 THEN 'Volatile'
            ELSE 'Highly Volatile'
        END AS volatility_level,
        
        -- Price percentile interpretation
        CASE 
            WHEN price_percentile_in_set_rarity IS NULL THEN 'Unknown'
            WHEN price_percentile_in_set_rarity <= 0.25 THEN 'Budget'
            WHEN price_percentile_in_set_rarity <= 0.5 THEN 'Below Average'
            WHEN price_percentile_in_set_rarity <= 0.75 THEN 'Above Average'
            ELSE 'Premium'
        END AS relative_price_position
        
    FROM price_data
)

SELECT 
    -- Core identifiers
    oracle_id,
    card_name,
    type_line,
    color_identity,
    rarity,
    set_name,
    released_date,
    
    -- Pricing information
    best_usd_price,
    price_tier,
    foil_premium_percent,
    price_source_count,
    price_availability_score,
    market_coverage,
    price_volatility,
    volatility_level,
    price_percentile_in_set_rarity,
    relative_price_position,
    
    -- Price source flags
    has_usd_price,
    has_usd_foil_price,
    has_eur_price,
    has_tix_price,
    has_tcgplayer_price,
    has_cardkingdom_price,
    has_cardmarket_price,
    
    -- Card classification
    primary_type,
    cmc_tier,
    color_complexity,
    release_era,
    
    -- Calculated fields
    CASE 
        WHEN best_usd_price IS NULL THEN 'No Price Data'
        WHEN best_usd_price < 1 THEN 'Under $1'
        WHEN best_usd_price < 5 THEN '$1-$5'
        WHEN best_usd_price < 10 THEN '$5-$10'
        WHEN best_usd_price < 25 THEN '$10-$25'
        WHEN best_usd_price < 50 THEN '$25-$50'
        WHEN best_usd_price < 100 THEN '$50-$100'
        ELSE 'Over $100'
    END AS price_range,
    
    CASE 
        WHEN foil_premium_percent IS NULL THEN 'Unknown'
        WHEN foil_premium_percent <= 0 THEN 'Negative Premium'
        WHEN foil_premium_percent <= 25 THEN 'Low Premium'
        WHEN foil_premium_percent <= 50 THEN 'Moderate Premium'
        WHEN foil_premium_percent <= 100 THEN 'High Premium'
        ELSE 'Extreme Premium'
    END AS foil_premium_level,
    
    -- Investment indicators
    CASE 
        WHEN best_usd_price IS NULL THEN 'Unknown'
        WHEN volatility_level = 'Stable' AND price_tier IN ('expensive', 'premium') THEN 'Safe Investment'
        WHEN volatility_level = 'Moderate' AND price_tier IN ('moderate', 'expensive') THEN 'Moderate Risk'
        WHEN volatility_level IN ('Volatile', 'Highly Volatile') THEN 'High Risk'
        WHEN price_tier = 'budget' AND volatility_level = 'Stable' THEN 'Budget Stable'
        ELSE 'Variable'
    END AS investment_profile,
    
    -- Market timing indicators
    CASE 
        WHEN release_era = 'recent' AND price_tier IN ('expensive', 'premium') THEN 'New Hot'
        WHEN release_era = 'modern' AND volatility_level = 'Stable' THEN 'Established'
        WHEN release_era = 'established' AND price_tier = 'budget' THEN 'Budget Staple'
        WHEN release_era = 'vintage' AND price_tier IN ('expensive', 'premium') THEN 'Vintage Staple'
        ELSE 'Standard'
    END AS market_position

FROM price_stats

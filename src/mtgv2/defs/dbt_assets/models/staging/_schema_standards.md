-- models/staging/\_schema_standards.md
-- Standardized Schema Conventions for MTG Commander Spellbook Data

## Data Structure Overview

### Data Sources:

- **scryfall_cards_raw**: Scryfall API card data with oracle_id references
- **cs_cards_raw**: Individual card data with oracle_id references
- **cs_variants_raw**: Combo/strategy data (128k+ records) - NO oracle_id
- **cs_features_raw**: Feature definitions (4k+ records) - NO oracle_id
- **cs_templates_raw**: Scryfall query templates (270 records) - NO oracle_id

## Naming Conventions

### Primary Keys

- `oracle_id` - Universal card identifier (string) - **ONLY in cs_cards_raw**
- `scryfall_id` - Scryfall-specific identifier (string)
- `cs_card_id` - Commander Spellbook card identifier (integer)
- `variant_id` - Combo/strategy identifier (integer) - **NO oracle_id**
- `feature_id` - Feature identifier (integer) - **NO oracle_id**
- `template_id` - Template identifier (integer) - **NO oracle_id**

### Common Fields

- `card_name` - Standardized card name (string, trimmed)
- `type_line` - Card type line (string, trimmed)
- `oracle_text` - Oracle text (string, trimmed)
- `cmc` - Converted mana cost (integer)
- `color_identity` - Color identity (string, uppercase)
- `mana_cost` - Mana cost string (string)

### Boolean Fields

All boolean fields follow `is_*` or `has_*` pattern:

- `is_reserved` - Reserved list status
- `is_digital` - Digital-only card
- `is_promo` - Promotional card
- `is_reprint` - Reprint status
- `is_variation` - Card variation
- `is_spoiler` - Spoiler status
- `has_content_warning` - Content warning flag

### Image URIs

Standardized image URI naming:

- `image_small` - Small image URL
- `image_normal` - Normal image URL
- `image_large` - Large image URL
- `image_png` - PNG image URL
- `image_art_crop` - Art crop image URL
- `image_border_crop` - Border crop image URL

**Note**: Scryfall uses simplified naming vs CS cards which use `image_uri_front_*` pattern

### Price Fields

All prices rounded to 2 decimal places:

**Scryfall Prices:**

- `price_usd` - USD price
- `price_usd_foil` - USD foil price
- `price_usd_etched` - USD etched foil price
- `price_eur` - EUR price
- `price_eur_foil` - EUR foil price
- `price_tix` - MTGO tickets price

**Commander Spellbook Prices:**

- `price_tcgplayer` - TCGPlayer price
- `price_cardkingdom` - Card Kingdom price
- `price_cardmarket` - Card Market price

### Legality Fields

Format legality status (uppercase):

- `legal_commander` - Commander format legality
- `legal_standard` - Standard format legality
- `legal_modern` - Modern format legality
- `legal_legacy` - Legacy format legality
- `legal_vintage` - Vintage format legality
- `legal_pauper` - Pauper format legality
- `legal_pioneer` - Pioneer format legality
- `legal_brawl` - Brawl format legality
- `legal_oathbreaker` - Oathbreaker format legality
- `legal_paupercommander` - Pauper Commander format legality
- `legal_predh` - PreDH format legality
- `legal_premodern` - PreModern format legality

### Timestamps

- `_loaded_at` - Data load timestamp
- `_partition_date` - Partition date
- `created_at` - Record creation timestamp
- `updated_at` - Record update timestamp
- `released_date` - Card release date

### JSON Data Stored as TEXT

**Important**: These columns contain JSON data but are stored as TEXT, requiring explicit casting for JSON operations:

#### Scryfall Cards (stg_scryfall_cards):

- **Array Fields**: `colors`, `color_identity`, `color_indicator`, `keywords`, `games`
- **Price Fields**: `price_usd`, `price_usd_foil`, `price_usd_etched`, `price_eur`, `price_eur_foil`, `price_tix`
- **Date Fields**: `released_date` (stored as TEXT, needs `::DATE` casting)
- **Legality Fields**: All `legal_*` fields (extracted from JSON but stored as TEXT)

#### Commander Spellbook Cards (stg_cs_cards):

- **Array Fields**: `keywords_array` (properly parsed to TEXT[])
- **JSONB Fields**: `features` (actual JSONB)
- **Price Fields**: `price_tcgplayer`, `price_cardkingdom`, `price_cardmarket` (stored as TEXT, needs `::NUMERIC` casting)
- **Legality Fields**: All `legal_*` fields (extracted from JSON but stored as TEXT)

#### Commander Spellbook Variants (stg_cs_variants):

- **JSONB Fields**: `combo_cards`, `combo_uses`, `combo_includes`, `combo_produces`, `combo_requires` (stored as TEXT, invalid JSONB syntax)
- **Price Fields**: `price_tcgplayer`, `price_cardmarket`, `price_cardkingdom` (stored as TEXT, needs `::NUMERIC` casting)
- **Legality Fields**: All `legal_*` fields (extracted from JSON but stored as TEXT)

### Data Type Casting Requirements

When working with these fields in intermediate/mart models:

```sql
-- Price calculations
price_usd::NUMERIC * 1.1

-- Date comparisons
released_date::DATE >= CURRENT_DATE - INTERVAL '1 year'

-- JSONB operations (use with caution)
combo_cards::JSONB  -- May fail if invalid JSON syntax

-- Array operations
colors::TEXT[]  -- For JSON arrays stored as TEXT
```

### Scryfall Specific Fields

#### Card Properties:

- `power` - Creature power (string, e.g., "2", "_", "2+_")
- `toughness` - Creature toughness (string, e.g., "2", "_", "2+_")
- `loyalty` - Planeswalker loyalty (integer)
- `defense` - Battle defense (integer)
- `layout` - Card layout type (string)
- `hand_modifier` - Hand size modifier (integer)
- `life_modifier` - Life total modifier (integer)

#### Color Fields:

- `colors` - Card colors array (JSONB)
- `color_indicator` - Color indicator array (JSONB)

#### Set Information:

- `set` - Set code (string)
- `set_name` - Set name (string)
- `set_type` - Set type (string)
- `rarity` - Card rarity (string)

#### Meta Fields:

- `games` - Supported games array (JSONB)

### Commander Spellbook Specific Fields

#### CS Variants (Combo/Strategy Data):

- `combo_cards` - Cards involved in combo (JSONB)
- `combo_uses` - How cards are used (JSONB)
- `combo_includes` - Additional cards needed (JSONB)
- `combo_produces` - What the combo produces (JSONB)
- `combo_requires` - Requirements for combo (JSONB)
- `status` - Combo status (OK, etc.)
- `popularity` - Combo popularity score
- `bracket_tag` - Tournament bracket classification

#### CS Features:

- `feature_name` - Name of the feature/effect
- `is_uncountable` - Whether effect can be infinite
- `status` - Feature status (H, S, etc.)

#### CS Templates:

- `scryfall_query` - Scryfall search query
- `scryfall_api_url` - Full Scryfall API URL

### Quality Standards

- **scryfall_cards_raw**: Filter out records where `oracle_id IS NULL`
- **cs_cards_raw**: Filter out records where `oracle_id IS NULL`
- **cs_variants_raw**: Filter out records where `id IS NULL`
- **cs_features_raw**: Filter out records where `id IS NULL`
- **cs_templates_raw**: Filter out records where `id IS NULL`
- Use `NULLIF(TRIM(field), '')` for empty string handling
- Use `DISTINCT ON (oracle_id)` for deduplication where appropriate
- Consistent CTE naming: `source` → `cleaned` → final `SELECT`
- **Scryfall**: Use `UPPER(NULLIF(TRIM(field), ''))` for string fields that should be uppercase

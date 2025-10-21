# MTGv2 Pipeline + Systems

A comprehensive data pipeline for Magic: The Gathering Commander Spellbook analytics, built with Python, Dagster, and dbt.

## Project Overview

The goal is to learn modern data engineering practices while building a functional API via ASP.NET utilizing modern-day security principles such as JWT, and learning more about Python classes and inheritance.

## Architecture

### Data Sources

- **Scryfall API**: Comprehensive Magic: The Gathering card database
- **Commander Spellbook API**: Commander-specific combo and strategy data

### Data Pipeline

```
Raw Data → Staging → Intermediate → Marts → API
```

**Staging Layer**: Cleans and standardizes raw data from 5 sources

- `stg_scryfall_cards`: Scryfall card data with oracle_id references
- `stg_cs_cards`: Commander Spellbook card data
- `stg_cs_variants`: Combo/strategy data (128k+ records)
- `stg_cs_features`: Feature definitions (4k+ records)
- `stg_cs_templates`: Scryfall query templates (270 records)

**Intermediate Layer**: Business logic and data normalization

- `int_card_legalities`: Format legality normalization across sources
- `int_combo_enrichment`: Combo metrics and complexity analysis
- `int_price_analysis`: Price aggregation and market analysis

**Mart Layer**: Business-ready analytics tables

- `mart_card_catalog`: Unified card information with pricing
- `mart_format_legalities`: Cards organized by format legality
- `mart_price_trends`: Price analysis and market insights
- `mart_card_combo_lookup`: Flattened card-combo relationships
- `mart_combo_analysis`: Combo complexity and metrics

## Technology Stack

- **Orchestration**: Dagster
- **Transformation**: dbt (data build tool)
- **Database**: PostgreSQL
- **API Client**: Python with pandas
- **Future API**: ASP.NET Core with JWT authentication

## Key Features

### Data Quality

- Comprehensive schema standards and naming conventions
- Proper handling of JSON fields and data type casting
- Data validation and quality checks at each layer

### Scalability

- Modular dbt models with proper dependencies
- Separate asset groups for different pipeline stages
- Incremental data loading capabilities

### Analytics Ready

- Business-ready mart tables for reporting
- Format legality analysis across multiple formats
- Price trend analysis and market insights
- Combo complexity and difficulty scoring

## Getting Started

### Prerequisites

- Python 3.8+
- PostgreSQL
- Dagster
- dbt

### Installation

```bash
# Clone the repository
git clone <repository-url>
cd mtgv2

# Install dependencies
pip install -r requirements.txt

# Set up environment variables
cp env.example .env

# Run the pipeline
dagster dev
```

### Running the Pipeline

```bash
# Start Dagster web server
dagster dev

# Run specific asset groups
dagster asset materialize --select staging
dagster asset materialize --select intermediate
dagster asset materialize --select marts
```

## Data Schema

The pipeline follows a standardized schema documented in `src/mtgv2/defs/dbt_assets/_schema_standards.md`:

- **Primary Keys**: `oracle_id` (universal card identifier), `variant_id` (combo identifier)
- **Naming Conventions**: Consistent field naming across all models
- **Data Types**: Proper casting for prices, dates, and JSON fields
- **Quality Standards**: Data validation and deduplication rules

## API Development (Future)

The next phase will involve building an ASP.NET Core API with:

- JWT authentication
- RESTful endpoints for card and combo data
- Real-time price updates
- Advanced filtering and search capabilities

## Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Add tests if applicable
5. Submit a pull request

## 🐧 Note: WSL2 & Dagster gRPC Socket Issues

Dagster sometimes fails to launch code servers inside **WSL2** due to gRPC over Unix sockets.  
If you see errors like `UNAVAILABLE: GOAWAY received` or the code server immediately dying,  
you can work around this by manually starting the gRPC server and pointing Dagster at it.

**Steps:**

1. Start the gRPC server manually:

   ```bash
   dagster api grpc -p <PORT> -h 0.0.0.0 (localhost)
   ```

2. Create/update `workspace.yaml` to point to that server:

   ```yaml
   load_from:
     - grpc_server:
         host: "localhost"
         port: <PORT>
   ```

3. Run the Dagster web server as usual:

   ```bash
   dg dev --use-ssl -w workspace.yml
   ```

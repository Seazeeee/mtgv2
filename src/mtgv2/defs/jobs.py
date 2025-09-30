import dagster as dg

# Job for all assets
all_assets_job = dg.define_asset_job(name="all_assets_job")

# Job only for Scryfall-related assets
scryfall_job = dg.define_asset_job(
    name="raw_data_job",
    selection=[
        "get_scryfall_cards",
        "push_to_database",
        "get_commanderspellbook_cards",
        "get_commanderspellbook_variants",
        "get_commanderspellbook_features",
        "get_commanderspellbook_templates",
    ],
)

scryfall_job_test = dg.define_asset_job(
    name="scryfall_job_test", selection=["get_scryfall_cards", "push_to_temp_duckdb"]
)

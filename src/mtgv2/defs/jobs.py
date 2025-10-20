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
        "get_commanderspellbook_features",
        "get_commanderspellbook_templates",
    ],
)

scryfall_job_test = dg.define_asset_job(
    name="scryfall_job_test", selection=["get_scryfall_cards", "push_to_temp_duckdb"]
)

pull_from_db_job = dg.define_asset_job(
    name="pull_from_db_job",
    selection=[
        "pull_scryfall_table",
        "pull_cs_cards_table",
        "pull_cs_variants_table",
        "pull_cs_features_table",
        "pull_cs_templates_table",
        "clean_sf_cards",
    ],
)

large_data_job = dg.define_asset_job(
    name="large_data_job",
    selection=[
        "push_variants_to_database",
        "get_commanderspellbook_variants",
    ],
)

create_all_views_job = dg.define_asset_job(
    name="create_all_views_job",
    selection=[
        "pull_scryfall_table",
        "pull_cs_cards_table",
        "pull_cs_variants_table",
        "pull_cs_features_table",
        "pull_cs_templates_table",
        "clean_sf_cards",
        "clean_cs_cards",
    ],
)

import dagster as dg

# Job for all assets
all_assets_job = dg.define_asset_job(name="all_assets_job")

# Job only for Scryfall-related assets
scryfall_job = dg.define_asset_job(
    name="scryfall_job",
    selection=["get_scryfall_cards", "push_to_postgres"],
)

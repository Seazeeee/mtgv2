from dagster import Definitions
from .defs.assets import get_scryfall_cards, push_to_postgres, pull_scryfall_table
from .defs.jobs import all_assets_job, scryfall_job

defs = Definitions(
    jobs=[all_assets_job, scryfall_job],
    assets=[get_scryfall_cards, push_to_postgres, pull_scryfall_table],
)

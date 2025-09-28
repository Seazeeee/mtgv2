from dagster import Definitions
from .defs.assets import get_scryfall_cards, push_to_postgres, pull_scryfall_table

defs = Definitions(
    assets=[get_scryfall_cards, push_to_postgres, pull_scryfall_table],
)

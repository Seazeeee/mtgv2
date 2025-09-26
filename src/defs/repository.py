from dagster import Definitions
from src.defs.assets import get_scryfall_cards

# Modern Dagster uses Definitions instead of RepositoryDefinition
defs = Definitions(
    assets=[get_scryfall_cards],
)

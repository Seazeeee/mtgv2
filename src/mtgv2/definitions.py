from dagster import Definitions
from .defs.assets import (
    get_scryfall_cards,
    push_to_database,
    pull_scryfall_table,
    push_to_temp_duckdb,
    get_commanderspellbook_cards,
    get_commanderspellbook_variants,
    get_commanderspellbook_features,
    get_commanderspellbook_templates,
)
from .defs.jobs import all_assets_job, scryfall_job, scryfall_job_test

defs = Definitions(
    jobs=[all_assets_job, scryfall_job, scryfall_job_test],
    assets=[
        get_scryfall_cards,
        push_to_database,
        pull_scryfall_table,
        push_to_temp_duckdb,
        get_commanderspellbook_cards,
        get_commanderspellbook_variants,
        get_commanderspellbook_features,
        get_commanderspellbook_templates,
    ],
)

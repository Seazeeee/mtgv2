from dagster import Definitions, load_assets_from_modules
from .defs.jobs import all_assets_job, scryfall_job, scryfall_job_test, pull_from_db_job
from .dbt_resource import dbt_resource
from mtgv2.defs import assets

all_assets = load_assets_from_modules([assets])

defs = Definitions(
    jobs=[all_assets_job, scryfall_job, scryfall_job_test, pull_from_db_job],
    assets=all_assets,
    resources={"dbt": dbt_resource},
)

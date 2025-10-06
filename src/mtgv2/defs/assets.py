import dagster as dg
import pandas as pd
import os
from collections.abc import Mapping
from typing import Any, Optional
from dagster import asset, AssetKey
from dotenv import load_dotenv
from mtgv2.scryfall import ScryfallClient
from mtgv2.commander_spellbook import CommanderSpellbookClient
from mtgv2.internal_classes.db_client import DatabaseClient
from dagster_dbt import DbtCliResource, dbt_assets, DagsterDbtTranslator
from mtgv2.dbt_resource import dbt_project


@asset(
    description="""Gets all the ordinary cards from Scryfall bulk API""",
    group_name="RAW_DATA_Scryfall",
    kinds={"python"},
)
def get_scryfall_cards() -> pd.DataFrame:
    load_dotenv()
    DB_URI = os.getenv("DB_URI")
    client = ScryfallClient(uri="https://api.scryfall.com/bulk-data", db_uri=DB_URI)
    df = client.fetch()
    return df


@asset(
    description="""Gets all the cards from CommanderSpellbook""",
    group_name="RAW_DATA_CommanderSpellbook",
    kinds={"python"},
)
def get_commanderspellbook_cards() -> pd.DataFrame:
    load_dotenv()
    DB_URI = os.getenv("DB_URI")
    client = CommanderSpellbookClient(
        uri="https://backend.commanderspellbook.com/cards/", db_uri=DB_URI
    )
    df = client.fetch()
    return df


@asset(
    description="""Gets all combo variants from CommanderSpellbook""",
    group_name="RAW_DATA_CommanderSpellbook",
    kinds={"python"},
)
def get_commanderspellbook_variants() -> pd.DataFrame:
    load_dotenv()
    DB_URI = os.getenv("DB_URI")
    client = CommanderSpellbookClient(
        uri="https://backend.commanderspellbook.com/variants/", db_uri=DB_URI
    )
    df = client.fetch()
    return df


@asset(
    description="""Gets all Effects produced by combos from CommanderSpellbook""",
    group_name="RAW_DATA_CommanderSpellbook",
    kinds={"python"},
)
def get_commanderspellbook_features() -> pd.DataFrame:
    load_dotenv()
    DB_URI = os.getenv("DB_URI")
    client = CommanderSpellbookClient(
        uri="https://backend.commanderspellbook.com/features/", db_uri=DB_URI
    )
    df = client.fetch()
    return df


@asset(
    description="""Gets all card requirements from CommanderSpellbook""",
    group_name="RAW_DATA_CommanderSpellbook",
    kinds={"python"},
)
def get_commanderspellbook_templates() -> pd.DataFrame:
    load_dotenv()
    DB_URI = os.getenv("DB_URI")
    client = CommanderSpellbookClient(
        uri="https://backend.commanderspellbook.com/templates/", db_uri=DB_URI
    )
    df = client.fetch()
    return df


@asset(
    description="Pushes all raw data DataFrames to the configured database",
    deps=[
        "get_scryfall_cards",
        "get_commanderspellbook_cards",
        "get_commanderspellbook_variants",
        "get_commanderspellbook_features",
        "get_commanderspellbook_templates",
    ],
    group_name="RAW_TABLES_TO_DB",
    kinds={"python", "postgres"},
)
def push_to_database(
    get_scryfall_cards: pd.DataFrame,
    get_commanderspellbook_cards: pd.DataFrame,
    get_commanderspellbook_variants: pd.DataFrame,
    get_commanderspellbook_features: pd.DataFrame,
    get_commanderspellbook_templates: pd.DataFrame,
) -> dict:
    """Pushes all DataFrames to database with appropriate table names"""
    load_dotenv()
    DB_URI = os.getenv("DB_URI")
    client = DatabaseClient(uri=str(DB_URI))

    results = {}
    results["scryfall_cards"] = client.push(
        get_scryfall_cards, table_name="scryfall_cards_raw"
    )
    results["cs_cards"] = client.push(
        get_commanderspellbook_cards, table_name="cs_cards_raw"
    )
    results["cs_variants"] = client.push(
        get_commanderspellbook_variants, table_name="cs_variants_raw"
    )
    results["cs_features"] = client.push(
        get_commanderspellbook_features, table_name="cs_features_raw"
    )
    results["cs_templates"] = client.push(
        get_commanderspellbook_templates, table_name="cs_templates_raw"
    )

    return results


# PULL JOBS
@asset(
    description="""Pulls the scryfall cards table with today's date from the database""",
    deps=["push_to_database"],
    group_name="BRONZE_TO_SILVER",
    kinds={"postgres"},
)
def pull_scryfall_table(push_to_database: dict) -> pd.DataFrame:
    load_dotenv()
    DB_URI = os.getenv("DB_URI")
    client = DatabaseClient(uri=str(DB_URI))
    return client.get(table_name=push_to_database["scryfall_cards"])


@asset(
    description="""Pulls the commanderspellbook cards table with today's date from the database""",
    deps=["push_to_database"],
    group_name="BRONZE_TO_SILVER",
    kinds={"postgres"},
)
def pull_cs_cards_table(push_to_database: dict) -> pd.DataFrame:
    load_dotenv()
    DB_URI = os.getenv("DB_URI")
    client = DatabaseClient(uri=str(DB_URI))
    return client.get(table_name=push_to_database["cs_cards"])


@asset(
    description="""Pulls the commanderspellbook variants table with today's date from the database""",
    deps=["push_to_database"],
    group_name="BRONZE_TO_SILVER",
    kinds={"postgres"},
)
def pull_cs_variants_table(push_to_database: dict) -> pd.DataFrame:
    load_dotenv()
    DB_URI = os.getenv("DB_URI")
    client = DatabaseClient(uri=str(DB_URI))
    return client.get(table_name=push_to_database["cs_variants"])


@asset(
    description="""Pulls the commanderspellbook features table with today's date from the database""",
    deps=["push_to_database"],
    group_name="BRONZE_TO_SILVER",
    kinds={"postgres"},
)
def pull_cs_features_table(push_to_database: dict) -> pd.DataFrame:
    load_dotenv()
    DB_URI = os.getenv("DB_URI")
    client = DatabaseClient(uri=str(DB_URI))
    return client.get(table_name=push_to_database["cs_features"])


@asset(
    description="""Pulls the commanderspellbook templates table with today's date from the database""",
    deps=["push_to_database"],
    group_name="BRONZE_TO_SILVER",
    kinds={"postgres"},
)
def pull_cs_templates_table(push_to_database: dict) -> pd.DataFrame:
    load_dotenv()
    DB_URI = os.getenv("DB_URI")
    client = DatabaseClient(uri=str(DB_URI))
    return client.get(table_name=push_to_database["cs_templates"])


# DBT
class CustomDagsterDbtTranslator(DagsterDbtTranslator):
    def get_group_name(self, dbt_resource_props: Mapping[str, Any]) -> Optional[str]:
        return "BRONZE_TO_SILVER"

    def get_asset_key(self, dbt_resource_props):
        key = super().get_asset_key(dbt_resource_props)  # default

        # dbt_resorce_props is a dict with this metadata:
        # https://schemas.getdbt.com/dbt/manifest/v11/index.html#nodes_additionalProperties
        if dbt_resource_props["resource_type"] == "source":
            # adjust the key as necessary, here removing the prefix
            key = AssetKey(dbt_resource_props["name"])

        return key


@dbt_assets(
    manifest=dbt_project.manifest_path,
    dagster_dbt_translator=CustomDagsterDbtTranslator(),
)
def dbt_models(context: dg.AssetExecutionContext, dbt: DbtCliResource):
    yield from dbt.cli(["build"], context=context).stream()


# Test assets
@asset(
    description="Pushes the scryfall cards DataFrame to temporary DuckDB for testing",
    deps=["get_scryfall_cards"],
    group_name="PUSH_TEST",
    kinds={"python", "DuckDB"},
)
def push_to_temp_duckdb(get_scryfall_cards: pd.DataFrame) -> str:
    """Testing: Pushes to in-memory DuckDB"""
    client = DatabaseClient(uri=":memory:")
    return client.push(get_scryfall_cards, table_name="scryfall_cards_test")

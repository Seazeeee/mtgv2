from dagster import asset
from dotenv import load_dotenv
import pandas as pd
import os
from datetime import datetime
from mtgv2.scryfall import ScryfallClient
from mtgv2.internal_classes.db_client import DatabaseClient


@asset(
    description="""Gets all the ordinary cards from Scryfall bulk API""",
    group_name="scryfall",
    kinds={"python"},
)
def get_scryfall_cards() -> pd.DataFrame:
    load_dotenv()
    DB_URI = os.getenv("DB_URI")
    client = ScryfallClient(uri="https://api.scryfall.com/bulk-data", db_uri=DB_URI)
    df = client.fetch()
    return df


@asset(
    description="Pushes the scryfall cards DataFrame to the configured database",
    deps=["get_scryfall_cards"],
    group_name="database",
    kinds={"python", "postgres"},
)
def push_to_database(get_scryfall_cards: pd.DataFrame) -> str:
    """Pushes to whatever database is configured via DB_URI (Postgres or DuckDB)"""
    load_dotenv()
    DB_URI = os.getenv("DB_URI")
    client = DatabaseClient(uri=str(DB_URI))
    return client.push(get_scryfall_cards)


# Test assets
@asset(
    description="Pushes the scryfall cards DataFrame to temporary DuckDB for testing",
    deps=["get_scryfall_cards"],
    group_name="database_test",
    kinds={"python", "DuckDB"},
)
def push_to_temp_duckdb(get_scryfall_cards: pd.DataFrame) -> str:
    """Testing: Pushes to in-memory DuckDB"""
    client = DatabaseClient(uri=":memory:")
    return client.push(get_scryfall_cards)


@asset(
    description="""Pulls the scryfall cards table with today's date from the database""",
    deps=["push_to_database"],
    group_name="database",
    kinds={"postgres"},
)
def pull_scryfall_table() -> pd.DataFrame:
    load_dotenv()
    DB_URI = os.getenv("DB_URI")
    client = DatabaseClient(uri=str(DB_URI))
    get_time = datetime.now()
    table_name = f"all_card_{get_time.strftime('%Y_%m_%d')}"
    return client.get(table_name=table_name)

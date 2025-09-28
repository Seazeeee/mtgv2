from dagster import asset
from dotenv import load_dotenv
import pandas as pd
import os
from datetime import datetime
from mtgv2.scryfall import ScryfallClient
from mtgv2.internal_classes.db_client import DatabaseClient


@asset(
    description="""Gets all the ordinary cards from Scryfall bulk api and appends
        them to a table inside of Postgres with the date""",
)
def get_scryfall_cards() -> pd.DataFrame:
    load_dotenv()
    DB_URI = os.getenv("DB_URI")

    client = ScryfallClient(uri="https://api.scryfall.com/bulk-data", db_uri=DB_URI)
    df = client.fetch()

    print(type(df))

    return df


@asset(
    description="Pushes the given df to Postgres",
    deps=["get_scryfall_cards"],
)
def push_to_postgres(get_scryfall_cards: pd.DataFrame) -> str:
    # Now you have access to the df from get_scryfall_cards
    load_dotenv()
    DB_URI = os.getenv("DB_URI")
    client = DatabaseClient(uri=DB_URI)
    return client.push(get_scryfall_cards)  # Pass the DataFrame to push


@asset(
    description="""Gets the table of scryfall cards that has todays date
        appended to the end of the name. This is just our base format.""",
    deps=["push_to_postgres"],
)
def pull_scryfall_table() -> pd.DataFrame:
    load_dotenv()
    DB_URI = os.getenv("DB_URI")

    client = DatabaseClient(uri=DB_URI)

    get_time = datetime.now()

    table_name = f"all_card_{get_time.strftime('%Y_%m_%d')}"

    df = client.get(table_name=table_name)

    return df

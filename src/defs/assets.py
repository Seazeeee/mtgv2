from dagster import asset
from dotenv import load_dotenv
import pandas as pd
import os
from src.scryfall import ScryfallClient


@asset(
    name="get-scryfall-cards",
    description="""Gets all the ordinary cards from Scryfall bulk api and appends
        them to a table inside of Postgres with the date""",
)
def get_scryfall_cards() -> pd.DataFrame:
    load_dotenv()
    DB_URI = os.getenv("DB_URI")

    client = ScryfallClient(uri="https://api.scryfall.com/bulk-data", db_uri=DB_URI)
    df = client.fetch()

    return df

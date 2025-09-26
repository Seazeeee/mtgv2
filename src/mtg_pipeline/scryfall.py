import requests
import os
from dotenv import load_dotenv  # noqa: F401
from datetime import datetime
import pandas as pd
from internal_classes.api_base import APIClient
from internal_classes.db_client import PostgresClient


class ScryfallClient(APIClient):
    def __init__(self, uri: str, db_uri: str, token=None):
        super().__init__(uri, token)
        self.db_uri = db_uri

    def fetch(self):
        response = requests.get(self.uri, timeout=60)
        response.raise_for_status()

        # Rest is to get to actual card data inside of JSON dic
        #  -- All hard coded values come from manually parsing response JSON
        bulk_data_uri = [
            x for x in response.json()["data"] if x["type"] == "default_cards"
        ][0]["download_uri"]

        all_cards = requests.get(bulk_data_uri, timeout=60)
        all_cards.raise_for_status()

        # Create a dataframe from all cards
        df = pd.DataFrame(all_cards.json())

        # Insert the date on index 0
        current_date = datetime.now()
        df.insert(0, "date", current_date)

        # Ensure that all columns are lower
        df.columns.str.lower()

        return df

    def push(self, df: pd.DataFrame) -> str:
        if not self.uri:
            raise ValueError("No database URI provided for push()")

        db = PostgresClient(uri=self.db_uri, df=df)

        return db.push(df)


if __name__ == "__main__":
    dotenv_loaded = load_dotenv()
    DB_URI = os.getenv("DB_URI")

    client = ScryfallClient(uri="https://api.scryfall.com/bulk-data", db_uri=DB_URI)
    df = client.fetch()

    print(client.push(df))

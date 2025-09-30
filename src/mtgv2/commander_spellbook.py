import requests
from dotenv import load_dotenv  # noqa: F401
import pandas as pd
from mtgv2.internal_classes.api_base import APIClient
from mtgv2.internal_classes.db_client import DatabaseClient


class CommanderSpellbookClient(APIClient):
    def __init__(self, uri: str, db_uri: str, token=None):
        super().__init__(uri, token)
        self.db_uri = db_uri

    def fetch(self):
        """Supports Pagination due to how this API works."""
        uri = self.uri
        all_results = []
        offset = 0
        limit = 100
        has_next = True

        while has_next:
            params = {"limit": limit, "offset": offset}

            # Gets data
            response = requests.get(uri, params=params, timeout=60)
            response.raise_for_status()
            data = response.json()

            all_results.extend(data["results"])
            has_next = data.get("next") is not None
            offset += limit

        df = pd.DataFrame(all_results)
        df.columns = df.columns.str.lower()
        return df

    def push(self, df: pd.DataFrame) -> str:
        if not self.db_uri:
            raise ValueError("No database URI provided for push()")

        db = DatabaseClient(uri=self.db_uri)

        return db.push(df)

    def pull(self, table_name: str) -> pd.DataFrame:
        if not self.db_uri:
            raise ValueError("No database URI provided for pull()")

        db = DatabaseClient(uri=self.db_uri)

        return db.get(table_name)

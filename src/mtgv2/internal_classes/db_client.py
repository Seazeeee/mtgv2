import csv
import pandas as pd
from datetime import datetime
from sqlalchemy import create_engine, inspect
from io import StringIO


class DatabaseClient:
    def __init__(self, uri: str):
        self.engine = create_engine(uri)
        self.dialect = self.engine.dialect.name

    @staticmethod
    def psql_insert_copy(table, conn, keys, data_iter):
        # gets a DBAPI connection that can provide a cursor
        dbapi_conn = conn.connection
        with dbapi_conn.cursor() as cur:
            s_buf = StringIO()
            writer = csv.writer(s_buf)
            writer.writerows(data_iter)
            s_buf.seek(0)

            columns = ", ".join('"{}"'.format(k) for k in keys)
            if table.schema:
                table_name = "{}.{}".format(table.schema, table.name)
            else:
                table_name = table.name

            sql = "COPY {} ({}) FROM STDIN WITH CSV".format(table_name, columns)
            cur.copy_expert(sql=sql, file=s_buf)

    def push(self, df: pd.DataFrame, table_name: str | None = None) -> str:
        # push to database
        if table_name is None:
            get_time = datetime.now()
            table_name = f"all_card_{get_time.strftime('%Y_%m_%d')}"

        if self.dialect == "postgresql":
            try:
                df.to_sql(
                    table_name, self.engine, method=self.psql_insert_copy, index=False
                )
            except ValueError as e:
                print(f"{e}")
                print("Attempting to pull data from already existing table.")
                print(f"Pulling table: {table_name}")

                return (
                    f"{table_name} found in database."
                    if inspect(self.engine).has_table(table_name)
                    else None
                )
        else:
            df.to_sql(table_name, self.engine, index=False)

        return f"Pushed to the database with table name {table_name}"

    def get(self, table_name: str) -> pd.DataFrame:
        # returns a table
        return pd.read_sql_table(table_name, self.engine)

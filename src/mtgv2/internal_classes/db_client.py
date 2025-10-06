import csv
import duckdb
import hashlib
import json
import pandas as pd
from sqlalchemy import create_engine, inspect, delete, MetaData, Table
from io import StringIO
from datetime import datetime, timedelta


class DatabaseClient:
    def __init__(self, uri: str):
        self.is_duckdb = uri == ":memory:"

        if self.is_duckdb:
            # Only support in-memory DuckDB
            self.dialect = "duckdb"
        else:
            # Handle SQLAlchemy connections (PostgreSQL, SQLite, etc.)
            self.engine = create_engine(uri)
            self.dialect = self.engine.dialect.name

    def _get_duckdb_connection(self):
        """Get an in-memory DuckDB connection"""
        return duckdb.connect(":memory:")

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
        """Push DataFrame to database"""

        if table_name is None:
            raise ValueError("Please provide a table_name")

        # Partition DF
        get_time = datetime.now()
        df.insert(loc=0, column="_loaded_at", value=get_time)
        df.insert(loc=1, column="_partition_date", value=get_time.date())

        # Try to get existing data from the table
        try:
            df_existing = self.get(table_name=str(table_name))
        except ValueError:
            # Table doesn't exist yet, use empty DataFrame
            df_existing = pd.DataFrame()

        # Combine old and new data
        if not df_existing.empty:
            df_combined = pd.concat([df_existing, df], ignore_index=True)
        else:
            df_combined = df

        # Remove partitions older than 30 days
        df_final = self.delete_partition(df=df_combined)

        if self.is_duckdb:
            return self._push_duckdb(df_final, table_name)
        else:
            return self._push_sqlalchemy(df_final, table_name)

    def _push_duckdb(self, df: pd.DataFrame, table_name: str) -> str:
        """Push to in-memory DuckDB"""
        conn = self._get_duckdb_connection()
        try:
            # Check if table exists
            try:
                existing_count = conn.execute(
                    f"SELECT COUNT(*) FROM {table_name}"
                ).fetchone()[0]
                return f"{table_name} found in database with {existing_count} rows."
            except ValueError:
                # Table doesn't exist, create it
                pass

            # Register the DataFrame and create table
            conn.register("temp_df", df)
            conn.execute(f"CREATE TABLE {table_name} AS SELECT * FROM temp_df")

            # Verify the insert
            count = conn.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]
            return (
                f"Pushed {count} rows to in-memory DuckDB with table name {table_name}"
            )

        except Exception as e:
            return f"Error pushing to DuckDB: {str(e)}"
        finally:
            conn.close()

    def _push_sqlalchemy(self, df: pd.DataFrame, table_name: str) -> str:
        """Push using SQLAlchemy (PostgreSQL, SQLite, etc.)"""
        if self.dialect == "postgresql":
            try:
                if inspect(self.engine).has_table(table_name):
                    with self.engine.begin() as conn:
                        metadata = MetaData()
                        table = Table(table_name, metadata, autoload_with=self.engine)
                        conn.execute(delete(table))

                        df.to_sql(
                            table_name,
                            self.engine,
                            method=self.psql_insert_copy,
                            index=False,
                            if_exists="append",
                        )
            except ValueError as e:
                return f"Error: {e}"
        else:
            # SQLite or other databases
            df.to_sql(table_name, self.engine, index=False, if_exists="fail")

        return table_name

    def get(self, table_name: str) -> pd.DataFrame:
        """Get DataFrame from database"""
        if self.is_duckdb:
            return self._get_duckdb(table_name)
        else:
            return self._get_sqlalchemy(table_name)

    def _get_duckdb(self, table_name: str) -> pd.DataFrame:
        conn = self._get_duckdb_connection()
        try:
            df = conn.execute(f"SELECT * FROM {table_name}").df()
            return df
        except Exception as e:
            print(f"Error reading from DuckDB table {table_name}: {e}")
            print("Note: In-memory DuckDB doesn't persist across connections")
            return pd.DataFrame()
        finally:
            conn.close()

    def _get_sqlalchemy(self, table_name: str) -> pd.DataFrame:
        """Get data using SQLAlchemy"""
        try:
            return pd.read_sql_table(table_name, self.engine)
        except Exception as e:
            print(f"Error reading from table {table_name}: {e}")
            return pd.DataFrame()

    def has_table(self, table_name: str) -> bool:
        """Check if table exists"""
        if self.is_duckdb:
            conn = self._get_duckdb_connection()
            try:
                conn.execute(f"SELECT 1 FROM {table_name} LIMIT 1")
                return True
            except ValueError:
                return False
            finally:
                conn.close()
        else:
            return inspect(self.engine).has_table(table_name)

    def delete_partition(self, df: pd.DataFrame) -> pd.DataFrame:
        """Checks to see if partition date is older than 30 days.
        Drops rows that are.

        Args:
            df (pd.DataFrame): A pandas df that contains table data.

        Returns:
            pd.DataFrame: A pandas df that contains table data where
            partition_date <= 30 days.
        """
        ref_date = datetime.now().date()
        time_difference = timedelta(days=30)

        df["_partition_date"] = pd.to_datetime(df["_partition_date"]).dt.date

        return df[df["_partition_date"] <= (ref_date + time_difference)]

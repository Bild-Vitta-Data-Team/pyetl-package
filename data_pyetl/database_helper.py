from sqlalchemy.engine import Engine
from datetime import datetime
import numpy as np
import pandas as pd


class DbHelper:
    """
    Helper to select data from an origin transactional database and create a new stage table on the destiny database.
    """
    CHUNKSIZE = 100000

    def __init__(self, table_name, columns, ds_engine, cutoff_columns=[], query=""):
        if type(table_name) is not str:
            raise Exception("Table name must be a string")
        else:
            self.table_name = table_name

        if type(columns) is not str:
            raise Exception("Columns names must be a string")
        else:
            self.columns = columns
        if type(cutoff_columns) is not list:
            raise Exception("Cuttof columns must be a list")
        else:
            self.cutoff_columns = cutoff_columns
        if type(query) is not str:
            raise Exception("The query must be a string")
        else:
            self.query = query
        if type(ds_engine) is not Engine:
            raise Exception(
                "Data source engine must be an sqlalchemy engine instance")
        else:
            self.ds_engine = ds_engine

    def __str__(self):
        return f"StgTable({self.table_name}, {self.columns}, {self.cutoff_columns})"

    def select_query(self, cutoff_date=None, limit_date=None):
        """
        Construct the sql query to get the iterest data for the stage
        """

        if not cutoff_date:
            cutoff_date = "1900-01-01"

        if not self.query:
            self.query = f"SELECT {self.columns} FROM {self.table_name}"
        else:
            self.query = f"SELECT * FROM ({self.query})"

        where = None
        for col in self.cutoff_columns:
            if where:
                self.query += " OR "
            else:
                self.query += " WHERE "

            where = f"(CAST({col} as DATE) >= CAST('{cutoff_date}' as DATE)"

            if limit_date:
                where += f" AND CAST({col} as DATE) <= CAST('{limit_date}' as DATE)"

            where += ")"

            self.query += where

        return self.query

    def execute_query(self, db_engine, chunksize=100000):
        """
        Execute the self.query on a given database
        """
        executed = None
        try:
            executed = pd.read_sql(self.query, db_engine, chunksize=chunksize)
            return executed
        except Exception as e:
            return e

    def insert_dw(self, dw_con, table_prefix, dw_schema):
        """
        Function to insert data in your DW source
        """

        df = self.execute_query(self.ds_engine, self.CHUNKSIZE)

        df = df.replace("", np.nan)
        df = df.drop_duplicates()

        df.columns = df.columns.str.upper()
        df["DATA_PROCESSAMENTO"] = datetime.now()

        try:
            df.to_sql(
                f"{table_prefix}{self.table_name}",
                dw_con,
                schema=dw_schema,
                if_exists="replace",
                index=False,
                chunksize=self.CHUNKSIZE
            )
            del df
            return True
        except Exception as e:
            return e

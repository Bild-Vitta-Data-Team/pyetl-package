from sqlalchemy.engine import create_engine
import urllib


class DB_Connector:

    def __init__(self, data_source_engine, credentials):

        if type(data_source_engine) is not str:
            raise Exception("Data source engine must be a string")
        else:
            self.data_source_engine = data_source_engine

        if type(credentials) is not dict:
            raise Exception(
                "The variable 'redentials' must be a dict of the data source credentials")
        else:
            self.credentials = credentials

    def create_data_source_connection(self):
        """
        Function to create a sqlalchemy based database connection
        """
        if self.data_source_engine == "SQL":
            params = urllib.parse.quote_plus(
                "DRIVER={ODBC Driver 17 for SQL Server};"
                + "SERVER="
                + self.credentials["host"]
                + ";DATABASE="
                + self.credentials["db"]
                + ";UID="
                + self.credentials["username"]
                + ";PWD="
                + self.credentials["pwd"]
            )
            engine = create_engine("mssql+pyodbc:///?odbc_connect=%s" % params)

        elif self.data_source_engine == "Oracle":
            engine = create_engine(
                f"oracle+cx_oracle://{self.credentials['username']}:{self.credentials['pwd']}@{self.credentials['host']}:{self.credentials['port']}/?service_name={self.credentials['service_name']}"
            )

        elif self.data_source_engine == "MySQL":
            engine = create_engine(
                f"mysql+pymysql://{self.credentials['username']}:{self.credentials['pwd']}@{self.credentials['host']}/{self.credentials['db']}",
                pool_recycle=3600,
            )

        elif self.data_source_engine == "Firebird":
            engine = create_engine(
                f"firebird://{self.credentials['username']}:{self.credentials['pwd']}@{self.credentials['host']}:{self.credentials['port']}/{self.credentials['db']}",
                pool_recycle=3600,
            )

        elif self.data_source_engine == "Postgres":
            engine = create_engine(
                f"postgresql://{self.credentials['username']}:{self.credentials['pwd']}@{self.credentials['host']}:{self.credentials['port']}/{self.credentials['db']}",
                pool_recycle=3600,
            )

        else:
            engine = None

        return engine

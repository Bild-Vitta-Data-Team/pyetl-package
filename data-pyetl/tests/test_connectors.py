import pytest
import unittest
from data_pyetl.connectors import DB_Connector


class TestConnectors(unittest.TestCase):

    def test_error_on_init_data_source(self):
        ds_con = ["Not a string"]
        credentials = {
            "host": "host",
            "db": "db",
            "username": "username",
            "pwd": "pwd"
        }

        with pytest.raises(Exception, match=r"Data source engine must be a string"):
            connector = DB_Connector(ds_con, credentials)

    def test_error_on_init_credentials(self):
        ds_con = "SQL"
        credentials = ["Not a dict"]

        with pytest.raises(Exception, match=r"The variable 'redentials' must be a dict of the data source credentials"):
            connector = DB_Connector(ds_con, credentials)

    def test_instance_created(self):
        ds_con = "SQL"
        credentials = {
            "host": "host",
            "db": "db",
            "username": "username",
            "pwd": "pwd"
        }
        connector = DB_Connector(ds_con, credentials)

        self.assertIsInstance(connector, DB_Connector)

    def test_create_sqlserver_connector(self):
        ds_con = "SQL"
        credentials = {
            "host": "host",
            "db": "db",
            "username": "username",
            "pwd": "pwd"
        }
        connector = DB_Connector(ds_con, credentials)
        engine = connector.create_data_source_connection()

        self.assertEqual(engine.name, "mssql")

    def test_create_mysql_connector(self):
        ds_con = "MySQL"
        credentials = {
            "host": "host",
            "db": "db",
            "username": "username",
            "pwd": "pwd"
        }
        connector = DB_Connector(ds_con, credentials)
        engine = connector.create_data_source_connection()

        self.assertEqual(engine.name, "mysql")

    def test_create_postgres_connector(self):
        ds_con = "Postgres"
        credentials = {
            "host": "host",
            "db": "db",
            "username": "username",
            "pwd": "pwd",
            "port": "1234"
        }
        connector = DB_Connector(ds_con, credentials)
        engine = connector.create_data_source_connection()

        self.assertEqual(engine.name, "postgresql")

    def test_create_firebird_connector(self):
        ds_con = "Firebird"
        credentials = {
            "host": "host",
            "db": "db",
            "username": "username",
            "pwd": "pwd",
            "port": "1234"
        }
        connector = DB_Connector(ds_con, credentials)
        engine = connector.create_data_source_connection()

        self.assertEqual(engine.name, "firebird")

    def test_create_oracle_connector(self):
        ds_con = "Oracle"
        credentials = {
            "host": "host",
            "db": "db",
            "username": "username",
            "pwd": "pwd",
            "port": "1234",
            "service_name": "name1"
        }
        connector = DB_Connector(ds_con, credentials)
        engine = connector.create_data_source_connection()

        self.assertEqual(engine.name, "oracle")

    def test_create_none_connector(self):
        ds_con = "None"
        credentials = {
            "host": "host",
            "db": "db",
            "username": "username",
            "pwd": "pwd",
            "port": "1234"
        }
        connector = DB_Connector(ds_con, credentials)
        engine = connector.create_data_source_connection()

        self.assertEqual(engine, None)

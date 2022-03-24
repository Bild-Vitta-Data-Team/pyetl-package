import pytest
import unittest
import pandas as pd
from unittest.mock import MagicMock, Mock, patch
from data_pyetl.database_helper import DbHelper
from data_pyetl.connectors import DB_Connector


class TestDbHelpers(unittest.TestCase):

    def setUp(self):
        ds_con = "SQL"
        credentials = {
            "host": "host",
            "db": "db",
            "username": "username",
            "pwd": "pwd"
        }
        connector = DB_Connector(ds_con, credentials)
        self.engine = connector.create_data_source_connection()
        self.table_name = "user"
        self.columns = "id, name, age"
        self.cutoff_columns = []
        self.query = f"SELECT * FROM {self.table_name}"
        self.helper = DbHelper(self.table_name, self.columns, self.engine,
                               self.cutoff_columns, self.query)

    def test_error_on_init_table_name(self):
        table_name = 1
        columns = "id, name, age"
        cutoff_columns = []
        query = f"SELECT * FROM {table_name}"

        with pytest.raises(Exception, match=r"Table name must be a string"):
            helper = DbHelper(table_name, columns, self.engine,
                              cutoff_columns, query)

    def test_error_on_init_columns(self):
        table_name = "user"
        columns = ["id", "name", "age"]
        cutoff_columns = []
        query = f"SELECT * FROM {table_name}"

        with pytest.raises(Exception, match=r"Columns names must be a string"):
            helper = DbHelper(table_name, columns, self.engine,
                              cutoff_columns, query)

    def test_error_on_init_cutoff_columns(self):
        table_name = "user"
        columns = "id, name, age"
        cutoff_columns = "id, name, age"
        query = f"SELECT * FROM {table_name}"

        with pytest.raises(Exception, match=r"Cuttof columns must be a list"):
            helper = DbHelper(table_name, columns, self.engine,
                              cutoff_columns, query)

    def test_error_on_init_query(self):
        table_name = "user"
        columns = "id, name, age"
        cutoff_columns = []
        query = [f"SELECT * FROM {table_name}"]

        with pytest.raises(Exception, match=r"The query must be a string"):
            helper = DbHelper(table_name, columns, self.engine,
                              cutoff_columns, query)

    def test_error_on_init_engine(self):
        table_name = "user"
        columns = "id, name, age"
        cutoff_columns = []
        query = f"SELECT * FROM {table_name}"
        engine = "SQLServer engine"
        with pytest.raises(Exception, match=r"Data source engine must be an sqlalchemy engine instance"):
            helper = DbHelper(table_name, columns, engine,
                              cutoff_columns, query)

    def test_init_pass(self):
        table_name = "user"
        columns = "id, name, age"
        cutoff_columns = []
        query = f"SELECT * FROM {table_name}"

        helper = DbHelper(table_name, columns, self.engine,
                          cutoff_columns, query)

        self.assertIsInstance(helper, DbHelper)

    def test_str_method(self):
        table_name = "user"
        columns = "id, name, age"
        cutoff_columns = []
        query = f"SELECT * FROM {table_name}"

        helper = DbHelper(table_name, columns, self.engine,
                          cutoff_columns, query)

        self.assertEqual(helper.__str__(
        ), "StgTable(user, id, name, age, [])")

    def test_select_query_happy_way(self):
        query = "SELECT * FROM (SELECT * FROM user)"
        select_query = self.helper.select_query(None, None)

        self.assertEqual(query, select_query)

    def test_select_query_no_query(self):
        self.helper.query = ""
        query = "SELECT id, name, age FROM user"
        select_query = self.helper.select_query('2022-01-01', None)

        self.assertEqual(query, select_query)

    def test_select_query_cutoff_column_limit_date(self):
        self.helper.cutoff_columns = ["updated_at"]
        query = "SELECT * FROM (SELECT * FROM user) WHERE " + \
            "(CAST(updated_at as DATE) >= CAST('2022-01-01' as DATE) AND " + \
            "CAST(updated_at as DATE) <= CAST('2022-03-01' as DATE))"
        select_query = self.helper.select_query('2022-01-01', '2022-03-01')

        self.assertEqual(query, select_query)

    def test_select_query_cutoff_column(self):
        self.helper.cutoff_columns = ["updated_at"]
        query = "SELECT * FROM (SELECT * FROM user) WHERE " + \
            "(CAST(updated_at as DATE) >= CAST('2022-01-01' as DATE))"
        select_query = self.helper.select_query('2022-01-01', None)

        self.assertEqual(query, select_query)

    def test_select_query_cutoff_columns(self):
        self.helper.cutoff_columns = ["updated_at", "resolved_at"]
        query = "SELECT * FROM (SELECT * FROM user) WHERE " + \
            "(CAST(updated_at as DATE) >= CAST('2022-01-01' as DATE)) OR " + \
            "(CAST(resolved_at as DATE) >= CAST('2022-01-01' as DATE))"
        select_query = self.helper.select_query('2022-01-01', None)

        self.assertEqual(query, select_query)

    def test_select_query_cutoff_columns_limit_date(self):
        self.helper.cutoff_columns = ["updated_at", "resolved_at"]
        query = "SELECT * FROM (SELECT * FROM user) " + \
            "WHERE (CAST(updated_at as DATE) >= CAST('2022-01-01' as DATE) AND " + \
            "CAST(updated_at as DATE) <= CAST('2022-03-01' as DATE)) OR " + \
            "(CAST(resolved_at as DATE) >= CAST('2022-01-01' as DATE) AND " + \
            "CAST(resolved_at as DATE) <= CAST('2022-03-01' as DATE))"
        select_query = self.helper.select_query('2022-01-01', '2022-03-01')

        self.assertEqual(query, select_query)

    @patch('data_pyetl.database_helper.pd.read_sql')
    def test_execute_query(self, read_sql_mock: Mock):
        read_sql_mock.return_value = pd.DataFrame({"foo_id": [1, 2, 3]})

        executed = self.helper.execute_query(self.engine)
        read_sql_mock.assert_called_once()
        pd.testing.assert_frame_equal(
            executed, pd.DataFrame({"foo_id": [1, 2, 3]}))
        self.assertIsInstance(executed, pd.DataFrame)

    @patch('data_pyetl.database_helper.pd.read_sql', **{'return_value.raiseError.side_effect': Exception()})
    def test_execute_query_exception(self, read_sql_mock: Mock):
        read_sql_mock.side_effect = Exception("Error on read sql")

        executed = self.helper.execute_query(self.engine)

        read_sql_mock.assert_called_once()
        self.assertIsInstance(executed, Exception)
        self.assertEqual(executed, read_sql_mock.side_effect)

    @patch('pandas.DataFrame.to_sql')
    def test_insert_dw(self, to_sql_mock: Mock):
        to_sql_mock.return_value = None
        self.helper.select_query = MagicMock(
            return_value="SELECT id, name, age FROM user")
        self.helper.execute_query = MagicMock(
            return_value=pd.DataFrame({"foo_id": [1, 2, 3]}))

        inserted = self.helper.insert_dw(
            self.engine, "STGTest_", "TEST_SCHEMA")

        to_sql_mock.assert_called_once()
        self.assertTrue(inserted)

    @patch('pandas.DataFrame.to_sql', **{'return_value.raiseError.side_effect': Exception()})
    def test_insert_dw_df_error(self, to_sql_mock: Mock):
        to_sql_mock.side_effect = Exception("Error dataframe to sql")
        self.helper.select_query = MagicMock(
            return_value="SELECT id, name, age FROM user")
        self.helper.execute_query = MagicMock(
            return_value=pd.DataFrame({"foo_id": [1, 2, 3]}))

        inserted = self.helper.insert_dw(
            self.engine, "STGTest_", "TEST_SCHEMA")

        to_sql_mock.assert_called_once()
        self.assertIsInstance(inserted, Exception)
        self.assertEqual(inserted, to_sql_mock.side_effect)

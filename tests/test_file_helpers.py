import pytest
import unittest
import requests_mock
import pandas as pd
from pandas import util
from unittest.mock import MagicMock, Mock, patch
from data_pyetl.file_helper import FileHelper
from data_pyetl.connectors import DB_Connector


class TestFileHelpers(unittest.TestCase):

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
        test_data = {"Unnamed: 0": {"0": 1, "1": 2, "2": 3, "3": 4}, "a": {"0": "q", "1": "t", "2": "o", "3": "d"}, "b": {
            "0": "w", "1": "y", "2": "p", "3": "f"}, "c": {"0": "e", "1": "u", "2": "a", "3": "g"}, "d": {"0": "r", "1": "i", "2": "s", "3": "h"}}
        self.df_test = pd.DataFrame(test_data)
        self.filetype = "xlsx"
        self.filepath = f"/test.{self.filetype}"
        self.helper = FileHelper(self.filepath, self.filetype)

    def test_error_on_init_filepath(self):
        filepath = ["./test.xlsx"]
        filetype = "xlsx"

        with pytest.raises(Exception, match=r"Filepath must be a string"):
            helper = FileHelper(filepath, filetype)

    def test_error_on_init_filetype(self):
        filepath = "./test.xlsx"
        filetype = ["xlsx"]

        with pytest.raises(Exception, match=r"Filetype must be a string"):
            helper = FileHelper(filepath, filetype)

    def test_init_success(self):
        filepath = "./test.xlsx"
        filetype = "xlsx"
        helper = FileHelper(filepath, filetype)

        self.assertIsInstance(helper, FileHelper)

    def test_get_xsl_data_dataframe(self):
        self.helper.filetype = "xls"
        self.helper.filepath = "tests/test.xls"
        df_data = self.helper.read_xls_to_dataframe()

        self.assertIsInstance(df_data, pd.DataFrame)

    @patch('pandas.read_excel', **{'return_value.raiseError.side_effect': Exception()})
    def test_get_xsl_data_dataframe_error(self, excel_mock: Mock):
        excel_mock.side_effect = Exception("Failed to mount dataframe")
        self.helper.filetype = "xls"
        self.helper.filepath = "tests/test.xls"
        df_data = self.helper.read_xls_to_dataframe()

        excel_mock.assert_called()
        self.assertIsInstance(df_data, Exception)
        self.assertEqual(df_data, excel_mock.side_effect)

    def test_get_xslx_data_dataframe(self):
        self.helper.filetype = "xlsx"
        self.helper.filepath = "tests/test.xlsx"
        df_data = self.helper.read_xls_to_dataframe()

        self.assertIsInstance(df_data, pd.DataFrame)

    @patch('pandas.read_excel', **{'return_value.raiseError.side_effect': Exception()})
    def test_get_xslx_data_dataframe_error(self, excel_mock: Mock):
        excel_mock.side_effect = Exception("Failed to mount dataframe")
        self.helper.filetype = "xlsx"
        self.helper.filepath = "tests/test.xlsx"
        df_data = self.helper.read_xls_to_dataframe()

        excel_mock.assert_called()
        self.assertIsInstance(df_data, Exception)
        self.assertEqual(df_data, excel_mock.side_effect)

    def test_get_ods_data_dataframe(self):
        self.helper.filetype = "ods"
        self.helper.filepath = "tests/test.ods"
        df_data = self.helper.read_xls_to_dataframe()

        self.assertIsInstance(df_data, pd.DataFrame)

    @patch('pandas.read_excel', **{'return_value.raiseError.side_effect': Exception()})
    def test_get_ods_data_dataframe_error(self, excel_mock: Mock):
        excel_mock.side_effect = Exception("Failed to mount dataframe")
        self.helper.filetype = "ods"
        self.helper.filepath = "tests/test.ods"
        df_data = self.helper.read_xls_to_dataframe()

        excel_mock.assert_called()
        self.assertIsInstance(df_data, Exception)
        self.assertEqual(df_data, excel_mock.side_effect)

    def test_get_csv_data_dataframe(self):
        self.helper.filetype = "csv"
        self.helper.filepath = "tests/test.csv"
        df_data = self.helper.read_csv_to_dataframe()

        self.assertIsInstance(df_data, pd.DataFrame)

    @patch('pandas.read_csv', **{'return_value.raiseError.side_effect': Exception()})
    def test_get_csv_data_dataframe_error(self, csv_mock: Mock):
        csv_mock.side_effect = Exception("Failed to mount dataframe")
        self.helper.filetype = "csv"
        self.helper.filepath = "tests/test.csv"
        df_data = self.helper.read_csv_to_dataframe()

        csv_mock.assert_called()
        self.assertIsInstance(df_data, Exception)
        self.assertEqual(df_data, csv_mock.side_effect)

    def test_get_json_data_dataframe(self):
        self.helper.filetype = "json"
        self.helper.filepath = "tests/test.json"
        df_data = self.helper.read_json_to_dataframe()

        self.assertIsInstance(df_data, pd.DataFrame)

    @patch('pandas.read_json', **{'return_value.raiseError.side_effect': Exception()})
    def test_get_json_data_dataframe_error(self, json_mock: Mock):
        json_mock.side_effect = Exception("Failed to mount dataframe")
        self.helper.filetype = "json"
        self.helper.filepath = "tests/test.json"
        df_data = self.helper.read_json_to_dataframe()

        json_mock.assert_called()
        self.assertIsInstance(df_data, Exception)
        self.assertEqual(df_data, json_mock.side_effect)

    def test_get_xml_data_dataframe(self):
        self.helper.filetype = "xml"
        self.helper.filepath = "tests/test.xml"
        df_data = self.helper.read_xml_to_dataframe()

        self.assertIsInstance(df_data, pd.DataFrame)

    @patch('pandas.read_xml', **{'return_value.raiseError.side_effect': Exception()})
    def test_get_xml_data_dataframe_error(self, xml_mock: Mock):
        xml_mock.side_effect = Exception("Failed to mount dataframe")
        self.helper.filetype = "xml"
        self.helper.filepath = "tests/test.xml"
        df_data = self.helper.read_xml_to_dataframe()

        xml_mock.assert_called()
        self.assertIsInstance(df_data, Exception)
        self.assertEqual(df_data, xml_mock.side_effect)

    def test_get_parquet_data_dataframe(self):
        self.helper.filetype = "parquet"
        self.helper.filepath = "tests/test.parquet"
        df_data = self.helper.read_parquet_to_dataframe()

        self.assertIsInstance(df_data, pd.DataFrame)

    @patch('pandas.read_parquet', **{'return_value.raiseError.side_effect': Exception()})
    def test_get_parquet_data_dataframe_error(self, parquet_mock: Mock):
        parquet_mock.side_effect = Exception("Failed to mount dataframe")
        self.helper.filetype = "parquet"
        self.helper.filepath = "tests/test.parquet"
        df_data = self.helper.read_parquet_to_dataframe()

        parquet_mock.assert_called()
        self.assertIsInstance(df_data, Exception)
        self.assertEqual(df_data, parquet_mock.side_effect)

    @patch('pandas.DataFrame.to_sql')
    def test_dataframe_to_db(self, to_sql_mock: Mock):
        to_sql_mock.return_value = None
        self.helper.filetype = "csv"
        self.helper.filepath = "tests/test.csv"
        df_data = self.helper.read_csv_to_dataframe()
        table_prefix = "STGTest_"
        table_name = "Test"
        dw_schema = "TESTdb"
        inserted = self.helper.dataframe_to_db(
            df_data,
            table_prefix,
            table_name,
            self.engine,
            dw_schema
        )

        to_sql_mock.assert_called_once()
        self.assertTrue(inserted)

    @patch('pandas.DataFrame.to_sql', **{'return_value.raiseError.side_effect': Exception()})
    def test_insert_dataframe_to_db_error(self, to_sql_mock: Mock):
        to_sql_mock.side_effect = Exception("Error dataframe to sql")
        self.helper.filetype = "csv"
        self.helper.filepath = "tests/test.csv"
        df_data = self.helper.read_csv_to_dataframe()
        table_prefix = "STGTest_"
        table_name = "Test"
        dw_schema = "TESTdb"
        inserted = self.helper.dataframe_to_db(
            df_data,
            table_prefix,
            table_name,
            self.engine,
            dw_schema
        )

        to_sql_mock.assert_called_once()
        self.assertIsInstance(inserted, Exception)
        self.assertEqual(inserted, to_sql_mock.side_effect)

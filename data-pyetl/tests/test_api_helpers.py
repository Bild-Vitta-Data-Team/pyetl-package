import pytest
import unittest
import requests_mock
import pandas as pd
from pandas import util
from unittest.mock import MagicMock, Mock, patch
from data_pyetl.api_helper import APIHelper
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
        self.origin_url = "mock://test.com"
        self.request_headers = {'Authentication': "Token <faketoken>"}
        self.request_data = {}
        self.helper = APIHelper(
            self.origin_url, self.request_headers, self.request_data
        )

    def test_error_on_init_url(self):
        origin_url = ["mock://test.com"]
        request_headers = {'Authentication': "Token <faketoken>"}
        request_data = {}

        with pytest.raises(Exception, match=r"URL must be a string"):
            helper = APIHelper(origin_url)

    def test_error_on_init_reuqest_headers(self):
        origin_url = "mock://test.com"
        request_headers = ("Authentication", "Token <faketoken>")
        request_data = {}

        with pytest.raises(Exception, match=r"Headers must be a dict"):
            helper = APIHelper(origin_url, request_headers, request_data)

    def test_error_on_init_request_data(self):
        origin_url = "mock://test.com"
        request_headers = {'Authentication': "Token <faketoken>"}
        request_data = ()

        with pytest.raises(Exception, match=r"Request datas must be a dict"):
            helper = APIHelper(origin_url, request_headers, request_data)

    @requests_mock.Mocker()
    def test_get_data_dataframe(self, request_mock):
        request_mock.get(
            'mock://test.com',
            json={'resp': 'fine'},
            status_code=200
        )
        list_dataframe = list()
        list_dataframe.extend({'resp': 'fine'})

        df_data = self.helper.get_data(dataframe=True)
        pd.testing.assert_frame_equal(
            df_data, pd.DataFrame(list_dataframe))

    @requests_mock.Mocker()
    def test_get_data_json(self, request_mock):
        expected_json = {'resp': 'fine'}

        request_mock.get(
            'mock://test.com',
            json={'resp': 'fine'},
            status_code=200
        )

        response_json = self.helper.get_data(dataframe=False)

        self.assertAlmostEqual(expected_json, response_json)

    @requests_mock.Mocker()
    def test_get_data_not_200(self, request_mock):
        expected_json = {'resp': 'fine'}

        request_mock.get(
            'mock://test.com',
            json={'resp': 'fine'},
            status_code=300
        )

        response_json = self.helper.get_data(dataframe=False)

        self.assertIsNone(response_json)

    @patch('pandas.DataFrame.to_sql')
    def test_dataframe_to_dw(self, to_sql_mock: Mock):
        to_sql_mock.return_value = None
        df = util.testing.makeMissingDataframe()
        table_prefix = "STGTest_"
        table_name = "Test"
        dw_schema = "TESTdb"
        inserted = self.helper.dataframe_to_dw(
            df,
            table_prefix,
            table_name,
            self.engine,
            dw_schema
        )

        to_sql_mock.assert_called_once()
        self.assertTrue(inserted)

    @patch('pandas.DataFrame.to_sql', **{'return_value.raiseError.side_effect': Exception()})
    def test_insert_dataframe_to_dw_error(self, to_sql_mock: Mock):
        to_sql_mock.side_effect = Exception("Error dataframe to sql")
        df = util.testing.makeMissingDataframe()
        table_prefix = "STGTest_"
        table_name = "Test"
        dw_schema = "TESTdb"
        inserted = self.helper.dataframe_to_dw(
            df,
            table_prefix,
            table_name,
            self.engine,
            dw_schema
        )

        to_sql_mock.assert_called_once()
        self.assertIsInstance(inserted, Exception)
        self.assertEqual(inserted, to_sql_mock.side_effect)

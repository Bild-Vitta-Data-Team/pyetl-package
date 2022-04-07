from datetime import datetime
import numpy as np
import pandas as pd
import requests


class APIHelper:
    CHUNKSIZE = 100000

    def __init__(self, origin_url, request_headers={}, request_data={}):
        if type(origin_url) is not str:
            raise Exception("URL must be a string")
        else:
            self.url = origin_url
        if type(request_headers) is not dict:
            raise Exception("Headers must be a dict")
        else:
            self.request_headers = request_headers
        if type(request_data) is not dict:
            raise Exception("Request datas must be a dict")
        else:
            self.request_data = request_data

    def get_data(self, dataframe=False):
        """
        Get data form api request. Returns a JSON or a Pandas Dataframe in case of need by user
        """
        response = requests.get(
            self.url, headers=self.request_headers, data=self.request_data
        )
        df_list = list()
        if response.status_code == 200:
            if dataframe:
                df_list.extend(response.json())
                df = pd.DataFrame(df_list)
                return df
            else:
                response = response.json()
                return response

    def dataframe_to_dw(self, df, table_prefix, table_name, dw_con, dw_schema):
        """
        Save the worked pandas DataFrame to the source DW
        """
        df = df.replace("", np.nan)
        df = df.drop_duplicates()

        df.columns = df.columns.str.upper()
        df["DATA_PROCESSAMENTO"] = datetime.now()
        try:
            df.to_sql(
                f"{table_prefix}{table_name}",
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

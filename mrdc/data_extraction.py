import requests
import pandas as pd
import tabula


class DataExtractor:

    @staticmethod
    def read_rds_table(data_connector, table_name: str) -> pd.DataFrame:
        """
        Read a table from a database using the data connector instance
        :param data_connector:
        :param table_name:
        :return:
        """

        all_tables = data_connector.list_db_table()
        for table in all_tables:
            if table_name == table:
                engine = data_connector.init_db_engine()
                conn = engine.connect()
                df = pd.read_sql_table(table, conn)
                return df

    @staticmethod
    def retrieve_pdf_data(pdf_path: str) -> pd.DataFrame:
        """
        Read a table from a pdf file
        :param pdf_path:
        :return:
        """
        dfs = tabula.read_pdf(pdf_path, lattice=True, pages="all")
        df = pd.concat(dfs)
        return df

    @staticmethod
    def get_api_response(url: str, headers_dict: str) -> dict:
        """
        Get the response from an API
        :param url:
        :param headers_dict:
        :return:
        """
        response = requests.get(url, headers_dict)
        return response.json()

    @staticmethod
    def list_number_of_stores(url: str, headers_dict: str) -> int:
        """
        Get the number of stores from an API
        :param url:
        :param headers_dict:
        :return:
        """
        response = requests.get(url, headers=headers_dict)
        response_json = response.json()
        return response_json['number_stores']

    @staticmethod
    def retrieve_store_data(store_number: int, url: str, headers_dict: str) -> pd.DataFrame:
        """
        Retrieve the data from a number of stores
        :param store_number:
        :param url:
        :param headers_dict:
        :return:
        """

        list_of_dict = []
        for i in range(store_number):
            store_url = f"{url}/{i}"
            response = requests.get(store_url, headers=headers_dict)
            list_of_dict.append(response.json())
        store_df = pd.DataFrame(list_of_dict)
        return store_df

import requests
import pandas as pd
import tabula


class DataExtractor:

    @staticmethod
    def read_rds_table(data_connector, table_name):

        all_tables = data_connector.list_db_table()
        for table in all_tables:
            if table_name == table:
                engine = data_connector.init_db_engine()
                conn = engine.connect()
                df = pd.read_sql_table(table, conn)
                return df

    @staticmethod
    def retrieve_pdf_data(pdf_path):
        dfs = tabula.read_pdf(pdf_path, lattice=True, pages="all")
        df = pd.concat(dfs)
        return df

    @staticmethod
    def get_api_response(url, headers_dict):
        response = requests.get(url, headers_dict)
        return response.json()

    @staticmethod
    def list_number_of_stores(url, headers_dict):
        response = requests.get(url, headers=headers_dict)  # url = "https://api.yourcompany.com/stores"
        response_json = response.json()
        return response_json['number_stores']

    @staticmethod
    def retrieve_store_data(store_number, url, headers_dict):

        list_of_dict = []
        for i in range(store_number):
            store_url = f"{url}/{i}"
            response = requests.get(store_url, headers=headers_dict)
            list_of_dict.append(response.json())
        store_df = pd.DataFrame(list_of_dict)
        return store_df















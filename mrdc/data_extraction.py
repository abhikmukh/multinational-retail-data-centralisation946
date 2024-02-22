import pandas as pd
import tabula


class DataExtractor:

    @staticmethod
    def read_rds_table(data_connector, table_name):

        all_tables = data_connector.list_db_table()
        for table in all_tables:
            if table_name == table:
                df = pd.read_sql_table(table, data_connector.init_db_engine())
                return df

    @staticmethod
    def retrieve_pdf_data(pdf_path):
        dfs = tabula.read_pdf(pdf_path, lattice=True, pages="all")
        df = pd.concat(dfs)
        return df









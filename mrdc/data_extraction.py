from database_utils import DatabaseConnector
import pandas as pd
import tabula


class DataExtractor:

    @classmethod
    def read_rds_table(cls, foo, table_name):

        all_tables = foo.list_db_table()
        for table in all_tables:
            if table_name == table_name:
                df = pd.read_sql_table(table, foo.init_db_engine())
                return df

    def retrieve_pdf_data(self, pdf_path):
        dfs = tabula.read_pdf(pdf_path, lattice=True, pages="all")
        df = pd.concat(dfs)
        return df




data_connector = DatabaseConnector\
    (base_path="C:\Users\abhik\Documents\aicore\multinational-retail-data-centralisation946",
     cred_file="db_creds.yaml")

data_extractor = DataExtractor()
data_df = data_extractor.read_rds_table(data_connector, table_name="customer")






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









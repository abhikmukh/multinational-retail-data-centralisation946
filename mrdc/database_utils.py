import yaml
import os

from sqlalchemy import create_engine
from sqlalchemy import inspect
from sqlalchemy import text


class DatabaseConnector:
    def __init__(self, base_path, cred_file):
        self.base_path = base_path
        self.cred_file = cred_file

    def read_db_creds(self):
        full_file_path = os.path.join(self.base_path, self.cred_file)
        with open(full_file_path, 'r') as file:
            data = yaml.safe_load(file)
            return data

    def init_db_engine(self):
        database_cred = self.read_db_creds()
        database_uri = f"postgresql+psycopg2://{database_cred['USER']}" \
                       f":{database_cred['PASSWORD']}@{database_cred['HOST']}:" \
                       f"{database_cred['PORT']}/{database_cred['DATABASE']}"
        database_engine = create_engine(database_uri)
        return database_engine

    def list_db_table(self):
        engine = self.init_db_engine()
        inspector = inspect(engine)
        list_all_tables = inspector.get_table_names()
        return list_all_tables

    def upload_to_db(self, df, table_name):
        engine = self.init_db_engine()
        df.to_sql(name=table_name, con=engine, if_exists='replace', index=False)

    def alter_table_data_type(self, table_name, column_name, data_type):
        engine = self.init_db_engine()
        with engine.connect() as con:
            con.execute(text(f"ALTER TABLE {table_name} ALTER COLUMN {column_name} TYPE {data_type};"))
            con.commit()
        print(f"Column {column_name} of type {data_type} updated to table {table_name}")

    def alter_table_data_verbose(self, table_name, column_name, data_type):
        engine = self.init_db_engine()
        with engine.connect() as con:
            con.execute(text(f"ALTER TABLE {table_name} ALTER COLUMN {column_name} TYPE {data_type} "
                             f"using {column_name}::{data_type};"))
            con.commit()
        print(f"Column {column_name} updated to table {table_name}")

    def trim_special_character(self, table_name, column_name, special_character):
        engine = self.init_db_engine()
        with engine.connect() as con:
            con.execute(text(f"UPDATE {table_name} SET {column_name} = regexp_replace({column_name}, "
                             f"'{special_character}', '', 'g');"))
            con.commit()
        print(f"Special characters removed from column {column_name} in table {table_name}")

    def add_weight_category(self, table_name, new_column_name, existing_column_name, data_type):
        engine = self.init_db_engine()
        with engine.connect() as con:
            con.execute(text(f"ALTER TABLE {table_name} ADD COLUMN {new_column_name} TYPE {data_type} "
                             f"INSERT INTO {table_name} ({new_column_name}) ( "
                             f"SELECT {existing_column_name},"
                             f"CASE "
                             f"WHEN {existing_column_name} < 2 THEN 'LIGHT' "
                             f"WHEN {existing_column_name} >= 2 and {existing_column_name} < 40 THEN 'Mid_Sized' "
                             f"WHEN {existing_column_name} >= 40 and {existing_column_name} < 140 THEN 'Heavy' "
                             f"WHEN {existing_column_name} >= 140 THEN 'Truck_required' "
                             f"END"
                             f"FROM {table_name});"))
            con.commit()
        print(f"Column {existing_column_name} of type {data_type} added to table {table_name}")
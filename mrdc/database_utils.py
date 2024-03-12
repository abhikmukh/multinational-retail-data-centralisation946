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
            con.execute(text(f"ALTER TABLE {table_name} ALTER COLUMN \"{column_name}\" TYPE {data_type} "
                             f"using \"{column_name}\"::{data_type};"))
            con.commit()
        print(f"Column {column_name} updated to table {table_name}")

    def trim_special_character(self, table_name, column_name, special_character):
        engine = self.init_db_engine()
        with engine.connect() as con:
            con.execute(text(f"UPDATE {table_name} SET {column_name} = regexp_replace({column_name}, "
                             f"'{special_character}', '', 'g');"))
            con.commit()
        print(f"Special characters removed from column {column_name} in table {table_name}")

    def add_new_column(self, table_name, new_column_name, data_type):
        engine = self.init_db_engine()
        with engine.connect() as con:
            con.execute(text(f"ALTER TABLE {table_name} ADD COLUMN {new_column_name} {data_type};"))
            con.commit()
        print(f"Column {new_column_name} of type {data_type} added to table {table_name}")

    def remove_column(self, table_name, column_name):
        engine = self.init_db_engine()
        with engine.connect() as con:
            con.execute(text(f"ALTER TABLE {table_name} DROP COLUMN {column_name};"))
            con.commit()
        print(f"Column {column_name} removed from table {table_name}")

    def update_colum_with_new_data(self, table_name, column_name, new_data):
        engine = self.init_db_engine()
        with engine.connect() as con:
            con.execute(text(f"UPDATE {table_name} SET {column_name} = {new_data};"))
            con.commit()
        print(f"New data updated to column {column_name} in table {table_name}")

    def get_max_length(self, table_name, column_name):
        engine = self.init_db_engine()
        with engine.connect() as con:
            result = con.execute(text(f"SELECT MAX(LENGTH('{column_name}')) FROM {table_name};"))
            max_length = result.fetchone()
            return int(max_length[0])

    def add_primary_key(self, table_name, column_name):
        engine = self.init_db_engine()
        with engine.connect() as con:
            con.execute(text(f"ALTER TABLE {table_name} ADD PRIMARY KEY ({column_name});"))
            con.commit()
        print(f"Primary key added to column {column_name} in table {table_name}")

    def add_foreign_key(self, table_name, constraint_key, column_name, ref_table, ref_column):
        engine = self.init_db_engine()
        with engine.connect() as con:
            con.execute(text(f"ALTER TABLE {table_name} ADD CONSTRAINT {constraint_key} "
                             f"FOREIGN KEY ({column_name}) REFERENCES {ref_table} ({ref_column});"))
            con.commit()
        print(f"Foreign key added to column {column_name} in table {table_name} referencing {ref_table}({ref_column})")

    def delete_missing_data(self, table_name, column_name, ref_column, ref_table):
        engine = self.init_db_engine()
        with engine.connect() as con:
            con.execute(text(f"DELETE FROM {table_name} WHERE {column_name} IN "
                             f"(SELECT {column_name} from {table_name} "
                             f"EXCEPT "
                             f"SELECT {ref_column} from {ref_table});"))
            con.commit()
        print(f"Deleted missing data from column {ref_column} in table {ref_table}")

    def run_query(self, query):
        engine = self.init_db_engine()
        with engine.connect() as con:
            con.execute(text(query))
            con.commit()
        print(f"Query executed successfully")

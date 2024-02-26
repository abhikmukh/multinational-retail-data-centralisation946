import yaml
import os

from sqlalchemy import create_engine
from sqlalchemy import inspect


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

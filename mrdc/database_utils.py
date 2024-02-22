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

    def _init_db_engine(self):
        database_cred = self.read_db_creds()
        database_uri = f"postgresql+psycopg2://{database_cred['RDS_USER']}" \
                       f":{database_cred['RDS_PASSWORD']}@{database_cred['RDS_HOST']}:" \
                       f"{database_cred['RDS_PORT']}/{database_cred['RDS_DATABASE']}"
        database_engine = create_engine(database_uri)
        return database_engine

    def list_db_table(self):
        engine = self._init_db_engine()
        inspector = inspect(engine)
        list_all_tables = inspector.get_table_names()
        return list_all_tables

    def upload_to_db(self, df, table_name):
        engine = self._init_db_engine()
        df.to_sql(name=table_name, con=engine)

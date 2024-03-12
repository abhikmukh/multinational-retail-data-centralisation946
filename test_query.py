import os
import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy import inspect
from tabulate import tabulate
from mrdc import database_utils

base_path = os.getcwd()
postgres_data_connector = database_utils.DatabaseConnector(base_path=base_path, cred_file="local_db_creds.yaml")
sql_file_path = "sql_files"
query1_file_path = os.path.join(sql_file_path, "query_1.sql")

print(tabulate(postgres_data_connector.run_query(query1_file_path)))

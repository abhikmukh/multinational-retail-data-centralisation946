import os
import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy import inspect

from mrdc import database_utils


base_path = os.getcwd()
postgres_data_connector = database_utils.DatabaseConnector(base_path=base_path, cred_file="local_db_creds.yaml")
print(postgres_data_connector.list_db_table())

query1 = f"select country_code, count('*') as store_numbers from dim_store_details " \
         f"group by country_code " \
         f"order by store_numbers desc;"

print(postgres_data_connector.run_query(query1))

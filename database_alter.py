import configparser
import os
import pandas as pd
from sqlalchemy import text

from mrdc import database_utils


def main():
    config = configparser.ConfigParser()
    config.read(r'config.ini')
    base_path = os.getcwd()
    postgres_data_connector = database_utils.DatabaseConnector(base_path=base_path, cred_file="local_db_creds.yaml")

    # update orders_table table
    postgres_data_connector.alter_table_data_verbose(table_name="orders_table", column_name="date_uuid",
                                                     data_type="UUID")
    postgres_data_connector.alter_table_data_verbose(table_name="orders_table", column_name="user_uuid",
                                                     data_type="UUID")
    postgres_data_connector.alter_table_data_verbose(table_name="orders_table", column_name="card_number",
                                                     data_type="VARCHAR(19)")
    postgres_data_connector.alter_table_data_verbose(table_name="orders_table", column_name="product_code",
                                                     data_type="VARCHAR(11)")
    postgres_data_connector.alter_table_data_verbose(table_name="orders_table", column_name="store_code",
                                                     data_type="VARCHAR(12)")
    postgres_data_connector.alter_table_data_verbose(table_name="orders_table", column_name="product_quantity",
                                                     data_type="SMALLINT")
    # update dim_users table

    postgres_data_connector.alter_table_data_verbose(table_name="dim_users", column_name="first_name",
                                                     data_type="VARCHAR(255)")
    postgres_data_connector.alter_table_data_verbose(table_name="dim_users", column_name="last_name",
                                                     data_type="VARCHAR(255)")
    postgres_data_connector.alter_table_data_verbose(table_name="dim_users", column_name="date_of_birth",
                                                     data_type="DATE")
    postgres_data_connector.alter_table_data_type(table_name="dim_users", column_name="country_code",
                                                  data_type="VARCHAR(2)")
    postgres_data_connector.alter_table_data_verbose(table_name="dim_users", column_name="user_uuid", data_type="UUID")
    postgres_data_connector.alter_table_data_verbose(table_name="dim_users", column_name="join_date", data_type="DATE")

    # update dim_store_details table
    postgres_data_connector.alter_table_data_verbose(table_name="dim_store_details", column_name="longitude",
                                                     data_type="FLOAT")
    postgres_data_connector.alter_table_data_verbose(table_name="dim_store_details", column_name="latitude",
                                                     data_type="FLOAT")
    postgres_data_connector.alter_table_data_type(table_name="dim_store_details", column_name="locality",
                                                  data_type="VARCHAR(255)")
    postgres_data_connector.alter_table_data_type(table_name="dim_store_details", column_name="store_code",
                                                  data_type="VARCHAR(11)")
    postgres_data_connector.alter_table_data_verbose(table_name="dim_store_details", column_name="staff_numbers",
                                                     data_type="SMALLINT")
    postgres_data_connector.alter_table_data_verbose(table_name="dim_store_details", column_name="opening_date",
                                                     data_type="DATE")
    postgres_data_connector.alter_table_data_verbose(table_name="dim_store_details", column_name="store_type",
                                                     data_type="VARCHAR(255)")
    postgres_data_connector.alter_table_data_verbose(table_name="dim_store_details", column_name="country_code",
                                                     data_type="VARCHAR(2)")
    postgres_data_connector.alter_table_data_verbose(table_name="dim_store_details", column_name="continent",
                                                     data_type="VARCHAR(255)")

    # update dim_products table
    postgres_data_connector.trim_special_character(table_name="dim_products", column_name="product_price",
                                                   special_character="£")
    postgres_data_connector.add_weight_category(table_name="dim_products", new_column_name="weight_class",
                                                existing_column_name="weight", data_type="VARCHAR(14)")


if __name__ == "__main__":
    main()

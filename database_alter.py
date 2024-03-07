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
    postgres_data_connector.alter_table_data_verbose(table_name="dim_users", column_name="country_code",
                                                     data_type="VARCHAR(2)")
    postgres_data_connector.alter_table_data_verbose(table_name="dim_users", column_name="user_uuid", data_type="UUID")
    postgres_data_connector.alter_table_data_verbose(table_name="dim_users", column_name="join_date", data_type="DATE")

    # update dim_store_details table
    postgres_data_connector.alter_table_data_verbose(table_name="dim_store_details", column_name="longitude",
                                                     data_type="FLOAT")
    postgres_data_connector.alter_table_data_verbose(table_name="dim_store_details", column_name="latitude",
                                                     data_type="FLOAT")
    postgres_data_connector.alter_table_data_verbose(table_name="dim_store_details", column_name="locality",
                                                     data_type="VARCHAR(255)")
    postgres_data_connector.alter_table_data_verbose(table_name="dim_store_details", column_name="store_code",
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

    # update dim_products table adding extra column and removing special characters
    postgres_data_connector.trim_special_character(table_name="dim_products", column_name="product_price",
                                                   special_character="£")

    # update dim_product table add weight_class column and update the column with new values
    postgres_data_connector.add_new_column(table_name="dim_products", new_column_name="weight_class",
                                           data_type="VARCHAR(14)")
    new_product_column_values = "CASE " \
                                "WHEN weight < 2 THEN 'Light' " \
                                "WHEN weight >= 2 and weight < 40 THEN 'Mid_Sized' " \
                                "WHEN weight >= 40 and weight < 140 THEN 'Heavy' " \
                                "WHEN weight >= 140 THEN 'Truck_Required' END"
    postgres_data_connector.update_colum_with_new_data(table_name="dim_products", column_name="weight_class",
                                                       new_data=new_product_column_values)
    # update dim_products table remove column removed and add new column still_available
    postgres_data_connector.add_new_column(table_name="dim_products", new_column_name="still_available",
                                           data_type="BOOLEAN")
    new_boolean_column_values = "CASE " \
                                "WHEN removed = 'Still_avaliable' THEN TRUE " \
                                "WHEN removed = 'Removed' THEN FALSE END"
    postgres_data_connector.update_colum_with_new_data(table_name="dim_products", column_name="still_available",
                                                       new_data=new_boolean_column_values)
    # postgres_data_connector.remove_column(table_name="dim_products", column_name="removed")

    # update dim_products table's data type
    postgres_data_connector.alter_table_data_verbose(table_name="dim_products", column_name="product_price",
                                                     data_type="FLOAT")
    postgres_data_connector.alter_table_data_verbose(table_name="dim_products", column_name="weight",
                                                     data_type="FLOAT")
    postgres_data_connector.alter_table_data_verbose(table_name="dim_products", column_name="EAN",
                                                     data_type="VARCHAR(17)")
    postgres_data_connector.alter_table_data_verbose(table_name="dim_products", column_name="product_code",
                                                     data_type="VARCHAR(11)")
    postgres_data_connector.alter_table_data_verbose(table_name="dim_products", column_name="date_added",
                                                     data_type="DATE")
    postgres_data_connector.alter_table_data_verbose(table_name="dim_products", column_name="uuid",
                                                     data_type="UUID")
    postgres_data_connector.alter_table_data_verbose(table_name="dim_products", column_name="weight_class",
                                                     data_type="VARCHAR(14)")

    # update dim_date_times table's data type
    postgres_data_connector.alter_table_data_verbose(table_name="dim_date_times", column_name="month",
                                                     data_type="VARCHAR(2)")
    postgres_data_connector.alter_table_data_verbose(table_name="dim_date_times", column_name="year",
                                                     data_type="VARCHAR(4)")
    postgres_data_connector.alter_table_data_verbose(table_name="dim_date_times", column_name="day",
                                                     data_type="VARCHAR(2)")
    postgres_data_connector.alter_table_data_verbose(table_name="dim_date_times", column_name="time_period",
                                                     data_type="VARCHAR(10)")
    postgres_data_connector.alter_table_data_verbose(table_name="dim_date_times", column_name="date_uuid",
                                                     data_type="UUID")

    # update dim_card_details table's data type
    postgres_data_connector.alter_table_data_verbose(table_name="dim_card_details", column_name="card_number",
                                                     data_type="VARCHAR(19)")
    postgres_data_connector.alter_table_data_verbose(table_name="dim_card_details", column_name="expiry_date",
                                                     data_type="VARCHAR(5)")
    postgres_data_connector.alter_table_data_verbose(table_name="dim_card_details",
                                                     column_name="date_payment_confirmed", data_type="DATE")

    # add primary key to dim_* tables
    postgres_data_connector.add_primary_key(table_name="dim_users", column_name="user_uuid")
    postgres_data_connector.add_primary_key(table_name="dim_products", column_name="product_code")
    postgres_data_connector.add_primary_key(table_name="dim_store_details", column_name="store_code")
    postgres_data_connector.add_primary_key(table_name="dim_date_times", column_name="date_uuid")
    postgres_data_connector.add_primary_key(table_name="dim_card_details", column_name="card_number")

    # remove missing rows from dim_* tables and add foreign key to orders_table
    # dim_users table
    postgres_data_connector.delete_missing_data(table_name="orders_table", column_name="user_uuid",
                                                ref_column="user_uuid", ref_table="dim_users")

    postgres_data_connector.add_foreign_key(table_name="orders_table", constraint_key="fk_dim_user_uuid",
                                            column_name="user_uuid", ref_table="dim_users", ref_column="user_uuid")

    # dim_card_details table
    postgres_data_connector.delete_missing_data(table_name="orders_table", column_name="card_number",
                                                ref_column="card_number", ref_table="dim_card_details")
    # dim_card_details table
    postgres_data_connector.add_foreign_key(table_name="orders_table", constraint_key="fk_dim_card_number",
                                            column_name="card_number", ref_table="dim_card_details",
                                            ref_column="card_number")
    # dim_products table
    postgres_data_connector.delete_missing_data(table_name="orders_table", column_name="product_code",
                                                ref_column="product_code", ref_table="dim_products")
    postgres_data_connector.add_foreign_key(table_name="orders_table", constraint_key="fk_dim_product_code",
                                            column_name="product_code", ref_table="dim_products",
                                            ref_column="product_code")
    # dim_store_details table
    postgres_data_connector.delete_missing_data(table_name="orders_table", column_name="store_code",
                                                ref_column="store_code", ref_table="dim_store_details")
    postgres_data_connector.add_foreign_key(table_name="orders_table", constraint_key="fk_dim_store_code",
                                            column_name="store_code", ref_table="dim_store_details",
                                            ref_column="store_code")
    # dim_date_times table
    postgres_data_connector.add_foreign_key(table_name="orders_table", constraint_key="fk_dim_date_uuid",
                                            column_name="date_uuid", ref_table="dim_date_times",
                                            ref_column="date_uuid")
    print("Updated database schema")


if __name__ == "__main__":
    main()

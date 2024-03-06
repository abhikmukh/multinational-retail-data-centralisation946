import configparser
import os
import pandas as pd

from mrdc import database_utils
from mrdc import data_extraction
from mrdc import data_cleaning


def main():
    config = configparser.ConfigParser()
    config.read(r'config.ini')
    base_path = os.getcwd()
    aws_data_connector = database_utils.DatabaseConnector(base_path=base_path, cred_file="db_creds.yaml")
    postgres_data_connector = database_utils.DatabaseConnector(base_path=base_path, cred_file="local_db_creds.yaml")
    data_extractor = data_extraction.DataExtractor()

    # Extracting and cleaning user data and upload to db
    data_df = data_extractor.read_rds_table(aws_data_connector, table_name="legacy_users")

    clean_data_df = data_cleaning.DataCleaning.clean_user_data(data_df)
    postgres_data_connector.upload_to_db(clean_data_df, table_name="dim_users")
    print("User data cleaning and uploading to db is done")

    # Extracting and cleaning card data and upload to db
    pdf_path = config['pdf_file_path']['pdf_path']
    card_df = data_extractor.retrieve_pdf_data(pdf_path)
    clean_card_df = data_cleaning.DataCleaning.clean_card_data(card_df)  # clean card data
    postgres_data_connector.upload_to_db(clean_card_df, table_name="dim_card_details")
    print("Card data cleaning and uploading to db is done")

    # Extracting and cleaning store data and upload to db
    headers_dict = {
        "Content-Type": "application/json",
        "x-api-key": config['store_api']['api_key']
    }
    store_number = data_extractor.list_number_of_stores(config['store_api']['store_number_url'], headers_dict)
    store_df = data_extractor.retrieve_store_data(store_number, config['store_api']['retrieve_store_url'], headers_dict)
    clean_store_df = data_cleaning.DataCleaning.clean_store_data(store_df)  # clean store data
    postgres_data_connector.upload_to_db(clean_store_df, table_name="dim_store_details")
    print("Store data cleaning and uploading to db is done")

    # Extracting and cleaning order data and upload to db
    order_df = data_extractor.read_rds_table(aws_data_connector, table_name="orders_table")
    clean_order_df = data_cleaning.DataCleaning.clean_order_data(order_df)  # clean order data
    postgres_data_connector.upload_to_db(clean_order_df, table_name="orders_table")
    print("Order data cleaning and uploading to db is done")

    # Extracting and cleaning product data and upload to db
    product_file_path = config['product_file_path']['csv_path']
    product_df = pd.read_csv(product_file_path)
    clean_product_df = data_cleaning.DataCleaning.clean_product_data(product_df)  # clean product data
    postgres_data_connector.upload_to_db(clean_product_df, table_name="dim_products")
    print("Product data cleaning and uploading to db is done")

    # Extracting and cleaning date_events data and upload to db
    date_events_file_path = config['date_events_json']['json_path']
    date_events_df = pd.read_json(date_events_file_path)
    clean_date_events_df = data_cleaning.DataCleaning.clean_date_events(date_events_df)  # clean date_events data
    postgres_data_connector.upload_to_db(clean_date_events_df, table_name="dim_date_times")
    print("Date events data cleaning and uploading to db is done")


if __name__ == "__main__":
    main()

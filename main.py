import configparser
import os

from mrdc import database_utils
from mrdc import data_extraction
from mrdc import data_cleaning


def main():
    base_path = os.getcwd()
    aws_data_connector = database_utils.DatabaseConnector(base_path=base_path, cred_file="db_creds.yaml")
    postgres_data_connector = database_utils.DatabaseConnector(base_path=base_path, cred_file="local_db_creds.yaml")
    data_extractor = data_extraction.DataExtractor()

    # Extracting and cleaning user data and upload to db
    data_df = data_extractor.read_rds_table(aws_data_connector, table_name="legacy_users")
    data_df = data_cleaning.DataCleaning.clean_user_data(data_df)
    postgres_data_connector.upload_to_db(data_df, table_name="dim_users")

    # Extracting and cleaning card data and upload to db
    config = configparser.ConfigParser()
    config.read(r'config.ini')

    pdf_path = config['pdf_file_path']['pdf_path']
    card_df = data_extractor.retrieve_pdf_data(pdf_path)
    card_df = data_cleaning.DataCleaning.clean_card_data(card_df)
    postgres_data_connector.upload_to_db(card_df, table_name="dim_card_details")


if __name__ == "__main__":
    main()

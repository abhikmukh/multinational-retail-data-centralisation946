from mrdc import database_utils
from mrdc import data_extraction
from mrdc import data_cleaning


def main():
    base_path = r"C:\Users\abhik\Documents\aicore\multinational-retail-data-centralisation946"
    aws_data_connector = database_utils.DatabaseConnector(base_path=base_path, cred_file="db_creds.yaml")
    postgres_data_connector = database_utils.DatabaseConnector(base_path=base_path, cred_file="local_db_creds.yaml")
    data_extractor = data_extraction.DataExtractor()
    data_df = data_extractor.read_rds_table(aws_data_connector, table_name="legacy_users")
    d = data_cleaning.DataCleaning()
    clean_df = d.clean_user_data(df=data_df)
    postgres_data_connector.upload_to_db(clean_df, table_name="dim_users")





if __name__ == "__main__":
    main()

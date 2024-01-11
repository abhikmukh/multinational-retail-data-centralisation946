data_connector = DatabaseConnector\
    (base_path="C:\Users\abhik\Documents\aicore\multinational-retail-data-centralisation946",
     cred_file="db_creds.yaml")

data_extractor = DataExtractor()
data_df = data_extractor.read_rds_table(data_connector, table_name="customer")


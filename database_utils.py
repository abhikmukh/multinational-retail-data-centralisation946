import yaml


class DatabaseConnector:

    def read_db_creds(self, cred_file):
        with open(cred_file, 'r') as file:
            data = yaml.safe_load(file)
            return data


    pass

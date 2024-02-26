import pandas as pd


class DataCleaning:

    @staticmethod
    def _clean_date(df, col_name, date_format):
        """
        This method will clean the date column in the dataframe
        format='%Y-%m-%d'
        :param df:
        :param col_name:
        :param date_format:
        :return:
        """
        # update this method later
        nan_df = pd.to_datetime(df[col_name], format=date_format, errors="coerce")
        # above will return NaN when the format not match
        df = df[nan_df.notna()]
        return df

    @staticmethod
    def clean_user_data(df):
        """
        This method will clean the user data
        :param df:
        :return:
        """
        # remove rows with NULL values
        df = df[~df.isin(["NULL"]).any(axis=1)]

        # remove rows with wrong country names
        df = df[~df["country"].str.contains(r'[0-9]')]

        # replace wrong country codes
        df['country_code'] = df['country_code'].str.replace('GGB', 'GB')

        # remove first and last name with numbers
        df = df[~df["first_name"].str.contains(r'[0-9]')]
        df = df[~df["last_name"].str.contains(r'[0-9]')]

        # remove rows with wrong email format
        email_pattern = r'[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}'
        df = df[df["email_address"].str.contains(email_pattern)]

        # remove rows with wrong phone number
        pattern_numeric = r'[0-9]'
        df = df[df["phone_number"].str.contains(pattern_numeric)]

        # remove rows with wrong date format in date_of_birth column
        df = DataCleaning._clean_date(df, col_name="date_of_birth", date_format='%Y-%m-%d')

        # remove rows with wrong date format in join_date column
        df = DataCleaning._clean_date(df, col_name="join_date", date_format='%Y-%m-%d')
        return df

    @staticmethod
    def clean_card_data(df):
        """ This method will clean the card data
        :param df:
        :return:
        """

        # remove rows with NULL values
        df = df[~df.isin(["NULL"]).any(axis=1)]

        # remove rows with wrong card numbers
        df = df[~df["card_number"].str.contains(r'[0-9]', na=False)]

        # remove wrong date format in date_of_expiry column
        df = DataCleaning._clean_date(df, col_name="expiry_date", date_format='%m/%d')

        # remove rows with wrong date_payment_confirmed column
        df = DataCleaning._clean_date(df, col_name="date_payment_confirmed", date_format='%Y-%m-%d')

        return df

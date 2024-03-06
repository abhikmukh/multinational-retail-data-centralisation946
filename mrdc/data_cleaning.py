import pandas as pd


class DataCleaning:

    @staticmethod
    def _clean_date(df: pd.DataFrame, col_name: str, date_format: str) -> pd.DataFrame:
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
    def clean_user_data(df: pd.DataFrame) -> pd.DataFrame:
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
    def clean_card_data(df: pd.DataFrame) -> pd.DataFrame:
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

    @staticmethod
    def clean_store_data(df: pd.DataFrame) -> pd.DataFrame:
        """ This method will clean the store data
        :param df:
        :return:
        """

        # remove rows with NULL values
        df = df[~df.isin(["NULL"]).any(axis=1)]
        df = df[~df.isin(["N/A"]).any(axis=1)]

        # drop lat columns since most of the values are NULL
        df.drop(["lat"], inplace=True, axis=1)

        # remove rows with wrong country
        df = df[~df["country_code"].str.contains(r'[0-9]')]
        df = df[~(df["country_code"].str.len() > 2)]

        # remove staff_numbers that has alphabets

        df = df[~df["staff_numbers"].str.contains(r'[a-zA-Z]')]

        # remove rows with wrong continent
        df['continent'] = df['continent'].str.replace('ee', '')

        # remove rows with wrong store address
        df = df[~df["locality"].str.contains(r'[0-9]')]

        # remove rows with wrong longitude
        df = df[~df["longitude"].str.contains(r'[a-zA-Z]', na=False)]

        # remove rows with wrong latitude
        df = df[~df["latitude"].str.contains(r'[a-zA-Z]', na=False)]

        # remove rows with wrong opening date
        df = DataCleaning._clean_date(df, col_name="opening_date", date_format='%Y-%m-%d')

        return df

    @staticmethod
    def clean_order_data(df: pd.DataFrame) -> pd.DataFrame:
        """ This method will clean the order data
        :param df:
        :return:
        """

        # remove rows with NULL values
        df = df[~df.isin(["NULL"]).any(axis=1)]
        df = df[~df.isin(["N/A"]).any(axis=1)]

        # drop first_name, last_name and 1 columns
        df = df.drop(["first_name", "last_name", "1"], axis=1)
        return df

    @staticmethod
    def _convert_product_weights(product: str) -> float:
        """ This method will convert the product weights
        :param product:
        :return:
        """
        if "kg" in product:
            product = product.split("kg")[0]
            product = float(product)
        elif "g" in product and "x" not in product:
            product = product.split("g")[0]
            product = float(product) / 1000
        elif "ml" in product:
            product = product.split("ml")[0]
            product = float(product) / 1000
        elif "oz" in product:
            product = product.split("oz")[0]
            product = float(product) * 0.0283495
        elif "x" in product:
            product = product.split("x")
            product = (float(product[0].strip())) * float(product[1].strip().replace("g", "")) / 1000
        else:
            product = "NAN"
        return product

    @staticmethod
    def clean_product_data(df: pd.DataFrame) -> pd.DataFrame:
        """ This method will clean the product data
        :param df:
        :return:
        """
        # remove rows with NULL values
        df.dropna(inplace=True)
        df = df[~df.isin(["NULL"]).any(axis=1)]
        df = df[~df.isin(["N/A"]).any(axis=1)]

        # remove rows with wrong product weights
        df["weight"] = df["weight"].map(DataCleaning._convert_product_weights)
        df = df[~df.isin(["NAN"]).any(axis=1)]
        df = DataCleaning._clean_date(df, col_name="date_added", date_format='%Y-%m-%d')

        return df

    @staticmethod
    def clean_date_events(df: pd.DataFrame) -> pd.DataFrame:
        """ This method will clean the date events data
        :param df:
        :return:
        """

        # remove rows with NULL, N/A and NAN values
        df = df[~df.isin(["NULL"]).any(axis=1)]
        df = df[~df.isin(["N/A"]).any(axis=1)]

        # remove rows with wrong date format in timestamp column
        df = df[~df["timestamp"].str.contains(r'[a-zA-Z]')]

        # remove rows with wrong date format in month, year, day and time_period column
        df = df[~df["month"].str.contains(r'[a-zA-Z]')]
        df = df[~df["year"].str.contains(r'[a-zA-Z]')]
        df = df[~df["day"].str.contains(r'[a-zA-Z]')]
        df = df[~df["time_period"].str.contains(r'[0-9]')]
        return df



import pandas as pd


class DataCleaning:

    def _clean_date(self, df, col_name):
        nan_df = pd.to_datetime(df[col_name], format='%Y-%m-%d', errors="coerce")
        # above will return NaN when the format not match
        clean_df = df[nan_df.notna()]
        return clean_df

    def clean_user_data(self, df):
        clean_dob_df = self._clean_date(df=df, col_name="date_of_birth")
        clean_join_date_df = self._clean_date(df=clean_dob_df, col_name="join_date")
        clean_df = clean_join_date_df.dropna()
        return clean_df


class DataCleaning:
    def _init__(self, df):
        self.df = df

    def clean_user_data(self):
        if self.df.isnull():
            corrected_df = self.df.dropna()
            return corrected_df



    pass

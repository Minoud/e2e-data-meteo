from sqlalchemy import create_engine
import pandas as pd

class WeatherLoader:
    def __init__(self, db_path="data/weather.db"):
        self.db_path = db_path
        self.engine = create_engine(f"sqlite:///{self.db_path}")

    def load(self, df: pd.DataFrame, table_name="weather", if_exists="append"):
        #Loads a clean dataframe into the db
        df.to_sql(
            table_name,
            self.engine,
            if_exists=if_exists,
            index=False)
        print(f"Loaded data in {self.db_path}")
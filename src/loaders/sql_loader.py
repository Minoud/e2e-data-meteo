from sqlalchemy import (
    create_engine,
    Table,
    Column,
    Integer,
    Float,
    String,
    Date,
    DateTime,
    MetaData,
    UniqueConstraint
)
import pandas as pd
from datetime import datetime

class WeatherLoader:
    def __init__(self, db_path="data/weather.db"):
        self.db_path = db_path
        self.engine = create_engine(f"sqlite:///{self.db_path}")
        self.metadata = MetaData()
        self._define_tables()
        self._create_tables()

    def _define_tables(self):
        #Define tables and constraints
        self.weather_clean = Table(
            "weather_clean",
            self.metadata,
            Column("id", Integer, primary_key=True),
            Column("city", String, nullable=False),
            Column("datetime", DateTime, nullable=False),
            Column("temperature", Float),
            Column("wind_speed", Float),
            Column("precipitation", Float),
            Column("ingestion_ts", DateTime, default=datetime.utcnow),
            UniqueConstraint("city", "datetime", name="uq_city_datetime")
        )
        self.weather_daily = Table(
            "weather_daily",
            self.metadata,
            Column("id", Integer, primary_key=True),
            Column("city", String, nullable=False),
            Column("date", Date, nullable=False),
            Column("temp_min", Float),
            Column("temp_max", Float),
            Column("temp_avg", Float),
            Column("wind_avg", Float),
            Column("precipitation_sum", Float),
            Column("computed_ts", DateTime, default=datetime.utcnow),
            UniqueConstraint("city", "date", name="uq_city_date")
        )
        self.weather_city_stats = Table(
            "weather_city_stats",
            self.metadata,
            Column("id", Integer, primary_key=True),
            Column("city", String, unique=True, nullable=False),
            Column("temp_min_all_time", Float),
            Column("temp_max_all_time", Float),
            Column("temp_avg_all_time", Float),
            Column("wind_avg_all_time", Float),
            Column("days_observed", Integer),
            Column("last_update", DateTime)
        )

    def _create_tables(self):
        #Create tables in DB if they don'texist
        self.metadata.create_all(self.engine)

    def _delete_existing_rows(self, df: pd.DataFrame, table_name: str):
        with self.engine.begin() as conn:
            for _, row in (
                df[["city", "datetime"]]
                .drop_duplicates()
                .iterrows()
            ):
                conn.execute(
                    text(
                        """
                        DELETE FROM weather_clean
                        WHERE city = :city AND datetime = :datetime
                        """
                    ),
                    {
                        "city": row["city"],
                        "datetime": row["datetime"]
                    }
                )

    def load(self, df: pd.DataFrame, table_name: str, if_exists="append"):
        if if_exists == "replace_rows":
            self._delete_existing_rows(df, table_name)

        df.to_sql(
            table_name,
            self.engine,
            if_exists="append",
            index=False,
            method="multi"
        )
        print(f"Loaded data into table '{table_name}'")
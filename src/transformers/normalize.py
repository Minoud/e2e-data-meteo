import pandas as pd
from datetime import datetime
from pathlib import Path
import json

class WeatherNormalizer:
    def __init__(self, raw_dir="data/raw/open_meteo"):
        self.raw_dir = Path(raw_dir)

    def read_raw_file(self, filepath):
        with open(filepath, "r") as f:
            return json.load(f)

    def normalize_city(self, city, city_data):
        df = pd.DataFrame(city_data["hourly"])
        df["city"] = city
        df["datetime"] = pd.to_datetime(df["time"])
        df["ingestion_ts"] = datetime.utcnow()
        df = df.rename(columns={
            "temperature_2m": "temperature",
            "windspeed_10m": "wind_speed"
        })
        df = df[["city","datetime","temperature","wind_speed","precipitation","ingestion_ts",]]
        return df

    def build_clean_df(self):
        #Hourly / city level
        dfs = []
        for file_path in self.raw_dir.glob("*.json"):
            city = file_path.stem
            raw_data = self.read_raw_file(file_path)
            dfs.append(self.normalize_city(city, raw_data))
        return pd.concat(dfs, ignore_index=True)

    def build_daily_df(self, df_clean):
        #Daily aggregation
        df = df_clean.copy()
        df["date"] = df["datetime"].dt.date
        df_daily = (
            df.groupby(["city", "date"])
            .agg(
                temp_min=("temperature", "min"),
                temp_max=("temperature", "max"),
                temp_avg=("temperature", "mean"),
                wind_avg=("wind_speed", "mean"),
                precipitation_sum=("precipitation", "sum"),
            )
            .reset_index()
        )
        df_daily["computed_ts"] = datetime.utcnow()
        return df_daily

    def build_city_stats_df(self, df_clean):
        #Global stats per city
        df_stats = (
            df_clean.groupby("city")
            .agg(
                temp_min_all_time=("temperature", "min"),
                temp_max_all_time=("temperature", "max"),
                temp_avg_all_time=("temperature", "mean"),
                wind_avg_all_time=("wind_speed", "mean"),
                days_observed=("datetime", lambda x: x.dt.date.nunique()),
            )
            .reset_index()
        )
        df_stats["last_update"] = datetime.utcnow()
        return df_stats

    def normalize_all(self):
        #Returns the 3 dfs
        df_clean = self.build_clean_df()
        df_daily = self.build_daily_df(df_clean)
        df_stats = self.build_city_stats_df(df_clean)
        return df_clean, df_daily, df_stats

    def save_clean(self, df, name="weather_clean", output_dir="data/clean"):
        #Saves clean df to a csv file
        output_dir = Path(output_dir)
        output_dir.mkdir(parents=True, exist_ok=True)
        timestamp = datetime.utcnow().strftime("%Y-%m-%d_%H-%M")
        filename = f"{name}_{timestamp}.csv"
        out_path = output_dir / filename
        df.to_csv(out_path, index=False)
        print(f"Saved {name} at {out_path}")
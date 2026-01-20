import pandas as pd
from datetime import datetime
from pathlib import Path
import json

class WeatherNormalizer:

    def __init__(self, raw_dir="data/raw/open_meteo", clean_dir="data/clean"):
        self.raw_dir = Path(raw_dir)
        self.clean_dir = Path(clean_dir)
        self.clean_dir.mkdir(parents=True, exist_ok=True)

    def read_raw_file(self, filepath):
        with open(filepath, "r") as f:
            data = json.load(f)
        return data

    def normalize_city(self, city, city_data):
        df = pd.DataFrame(city_data["hourly"])
        df["city"] = city
        # convert time to datetime
        df["time"] = pd.to_datetime(df["time"])
        return df

    def normalize_all(self):
        """Loops on raw data file and concatenates"""
        all_dfs = []

        for file_path in self.raw_dir.glob("*.json"):
            # deduce city name from filename : paris.json → paris
            city = file_path.stem.split(".")[0]
            raw_data = self.read_raw_file(file_path)
            df_city = self.normalize_city(city, raw_data)
            all_dfs.append(df_city)

        # concatenation
        df_all = pd.concat(all_dfs, ignore_index=True)
        return df_all

    def save_clean(self, df, filename=None):
        if not filename:
            timestamp = datetime.utcnow().strftime("%Y-%m-%d_%H-%M")
            filename = f"weather_clean_{timestamp}.csv"

        out_path = self.clean_dir / filename
        df.to_csv(out_path, index=False)
        print(f"Saved clean data at {out_path}")


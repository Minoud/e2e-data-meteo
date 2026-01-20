import requests
import json
from datetime import datetime
from pathlib import Path

from .base_collector import BaseCollector
from cities import CITIES

BASE_URL = "https://api.open-meteo.com/v1/forecast"

HOURLY_VARS = [
    # Temperature
    "temperature_2m",
    "apparent_temperature",

    # Precipitation
    "precipitation",
    "rain",
    "snowfall",

    # Atmosphere
    "cloudcover",
    "relativehumidity_2m",
    "surface_pressure",

    # Wind
    "windspeed_10m",
    "winddirection_10m",
    "windgusts_10m",

    # Radiation
    "shortwave_radiation",

    # Condition
    "weathercode"
]

class OpenMeteoCollector(BaseCollector):

    def __init__(self):
        self.output_dir = Path("data/raw/open_meteo")
        self.output_dir.mkdir(parents=True, exist_ok=True)

    def fetch_city(self, city_name, lat, lon):
        params = {
            "latitude": lat,
            "longitude": lon,
            "hourly": ",".join(HOURLY_VARS),
            "timezone": "UTC"
        }

        response = requests.get(BASE_URL, params=params, timeout=10)
        response.raise_for_status()

        return response.json()

    def fetch(self):
        all_data = {}

        for city, coords in CITIES.items():
            data = self.fetch_city(city, coords["lat"], coords["lon"])
            all_data[city] = data

        return all_data

    def save_raw(self, data):
        timestamp = datetime.utcnow().strftime("%Y%m%d_%H%M")

        for city, city_data in data.items():
            file_path = self.output_dir / f"{city}.json"

            with open(file_path, "w") as f:
                json.dump(city_data, f)
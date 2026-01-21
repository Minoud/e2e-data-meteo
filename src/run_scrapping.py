from collectors.open_meteo import OpenMeteoCollector
from transformers.normalize import WeatherNormalizer
from loaders.sql_loader import WeatherLoader

def launch_scrapping():
    collector = OpenMeteoCollector()
    data = collector.fetch()
    collector.save_raw(data)

    transformer = WeatherNormalizer()
    df_clean, df_daily, df_stats= transformer.normalize_all()
    transformer.save_clean(df_clean)

    loader = WeatherLoader()
    loader.load(df_clean, table_name="weather_clean")
    loader.load(df_daily, table_name="weather_daily")
    loader.load(df_stats, table_name="weather_city_stats")
from collectors.open_meteo import OpenMeteoCollector
from transformers.normalize import WeatherNormalizer
from loaders.sql_loader import WeatherLoader

if __name__ == "__main__":
    collector = OpenMeteoCollector()
    data = collector.fetch()
    collector.save_raw(data)

    transformer = WeatherNormalizer()
    df_clean = transformer.normalize_all()
    transformer.save_clean(df_clean)

    loader = WeatherLoader()
    loader.load(df_clean)
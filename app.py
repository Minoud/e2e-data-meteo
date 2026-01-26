import streamlit as st
import pandas as pd
from sqlalchemy import create_engine
from app.plots import plot_daily_temperature, plot_precipitation, plot_wind
from pathlib import Path

db_path = Path("data/weather.db").resolve()
print(db_path)
engine = create_engine(f"sqlite:///{db_path}")

@st.cache_data
def load_table(table_name):
    return pd.read_sql_table(table_name, engine)

df_clean = load_table("weather_clean")
df_daily = load_table("weather_daily")
df_stats = load_table("weather_city_stats")

st.title("Meteo Dashboard")
st.markdown("Meteo data visualization per day and city.")

city = st.selectbox("Choose a city", df_stats["city"].unique())

date_range = st.date_input(
    "Period",
    [df_daily["date"].min(), df_daily["date"].max()]
)

start_date = pd.to_datetime(date_range[0])
end_date   = pd.to_datetime(date_range[1])


df_daily_filtered = df_daily[
    (df_daily["city"] == city) &
    (df_daily["date"].between(start_date,end_date))
]

st.metric("Max temperature", f"{df_daily_filtered['temp_max'].max():.1f}°C")
st.metric("Min temperature", f"{df_daily_filtered['temp_min'].min():.1f}°C")
st.metric("Total precipitations", f"{df_daily_filtered['precipitation_sum'].sum():.1f} mm")

st.plotly_chart(plot_daily_temperature(df_daily_filtered))
st.plotly_chart(plot_precipitation(df_daily_filtered))
st.plotly_chart(plot_wind(df_daily_filtered))
import streamlit as st
import pandas as pd
import folium
from streamlit_folium import st_folium
from sqlalchemy import create_engine
from pathlib import Path
from app.plots import plot_daily_temperature, plot_precipitation, plot_wind
import requests
from functools import lru_cache
from src.run_scrapping import launch_scrapping
from datetime import date

today = str(date.today())

if "scraping_done" not in st.session_state or st.session_state.scraping_done != today:
    st.session_state.scraping_done = today
    launch_scrapping()

############################################################################### Page config
st.set_page_config(
    page_title="Meteo Dashboard",
    page_icon="🌤",
    layout="wide",
    initial_sidebar_state="expanded",
)

############################################################################# Custom CSS
st.markdown("""
<style>
@import url('https://fonts.googleapis.com/css2?family=DM+Serif+Display:ital@0;1&family=DM+Sans:wght@300;400;500;600&display=swap');

:root {
    --bg:       #0f1117;
    --surface:  #181c27;
    --border:   #252b3b;
    --accent:   #4f9cf9;
    --accent2:  #f97b4f;
    --text:     #e8ecf4;
    --muted:    #7a84a0;
}

html, body, [data-testid="stAppViewContainer"] {
    background: var(--bg) !important;
    color: var(--text);
    font-family: 'DM Sans', sans-serif;
}

/* Sidebar */
[data-testid="stSidebar"] {
    background: var(--surface) !important;
    border-right: 1px solid var(--border);
}
[data-testid="stSidebar"] * { color: var(--text) !important; }

/* Metric cards */
[data-testid="metric-container"] {
    background: var(--surface);
    border: 1px solid var(--border);
    border-radius: 14px;
    padding: 1.1rem 1.4rem !important;
    transition: border-color .2s;
}
[data-testid="metric-container"]:hover { border-color: var(--accent); }
[data-testid="stMetricLabel"]  { color: var(--muted)  !important; font-size: .8rem; letter-spacing: .06em; text-transform: uppercase; }
[data-testid="stMetricValue"]  { color: var(--text)   !important; font-family: 'DM Serif Display', serif; font-size: 2rem; }
[data-testid="stMetricDelta"]  { color: var(--accent)  !important; }

/* Selectbox / date input */
[data-testid="stSelectbox"] label,
[data-testid="stDateInput"]  label { color: var(--muted) !important; font-size: .78rem; letter-spacing: .07em; text-transform: uppercase; }

/* Section titles */
.section-title {
    font-family: 'DM Serif Display', serif;
    font-size: 1.35rem;
    color: var(--text);
    margin: 2rem 0 .8rem;
    border-left: 3px solid var(--accent);
    padding-left: .75rem;
}

/* Divider */
hr { border-color: var(--border) !important; }

/* Charts background */
.js-plotly-plot .plotly { background: transparent !important; }

/* Hide Streamlit branding */
#MainMenu, footer, header { visibility: hidden; }
</style>
""", unsafe_allow_html=True)

############################################################################## DB
db_path = Path("data/weather.db").resolve()
engine  = create_engine(f"sqlite:///{db_path}")

@st.cache_data
def load_table(name):
    return pd.read_sql_table(name, engine)

@st.cache_data
def get_coords(city_name: str) -> tuple[float, float]:
    try:
        print(f"Fetching coordinates for {city_name}...")
        r = requests.get(
            "https://nominatim.openstreetmap.org/search",
            params={"q": city_name, "format": "json", "limit": 1},
            headers={"User-Agent": "meteo-dashboard"},
            timeout=5,
        )
        data = r.json()
        if data:
            return float(data[0]["lat"]), float(data[0]["lon"])
    except Exception:
        pass
    return (46.603354, 1.888334)  # center of France as fallback

df_clean = load_table("weather_clean")
df_daily = load_table("weather_daily")
df_stats = load_table("weather_city_stats")

for df in (df_clean, df_daily, df_stats):
    df["city"] = df["city"].str.replace("_", " ").str.title()

# ── Sidebar ────────────────────────────────────────────────────────────────────
with st.sidebar:
    st.markdown("## Meteo")
    st.markdown("<small style='color:#7a84a0'>Climate dashboard</small>", unsafe_allow_html=True)
    st.divider()
    
    cities = sorted(df_stats["city"].unique())
    city   = st.selectbox("City ", cities)

    date_min = pd.to_datetime(df_daily["date"].min()).date()
    date_max = pd.to_datetime(df_daily["date"].max()).date()
    date_range = st.date_input("Period", [date_min, date_max],
                               min_value=date_min, max_value=date_max)

    st.divider()
    st.markdown("<small style='color:#7a84a0'>© Météo Dashboard</small>", unsafe_allow_html=True)

####################################################################### Filter data
if len(date_range) == 2:
    start_date, end_date = pd.to_datetime(date_range[0]), pd.to_datetime(date_range[1])
else:
    start_date = end_date = pd.to_datetime(date_range[0])

df_f = df_daily[
    (df_daily["city"] == city) &
    (df_daily["date"].between(start_date, end_date))
]

# ── Header ─────────────────────────────────────────────────────────────────────
st.markdown(f"<h1 style='font-family:DM Serif Display,serif;font-size:2.6rem;margin-bottom:.2rem'>"
            f"🌡 {city}</h1>", unsafe_allow_html=True)
st.markdown(f"<p style='color:#7a84a0;margin-top:0'>"
            f"{start_date.strftime('%d %b %Y')} → {end_date.strftime('%d %b %Y')}</p>",
            unsafe_allow_html=True)

st.divider()

##################################################### KPI row 
k1, k2, k3, k4 = st.columns(4)
k1.metric("Max temp.",      f"{df_f['temp_max'].max():.1f} °C")
k2.metric("Min temp.",      f"{df_f['temp_min'].min():.1f} °C")
k3.metric("Precipitations", f"{df_f['precipitation_sum'].sum():.1f} mm")
k4.metric("Days analyzed", str(len(df_f)))

st.divider()

################################################### Map + city stats
st.markdown('<div class="section-title">Localisation</div>', unsafe_allow_html=True)
col_map, col_stats = st.columns([3, 2], gap="large")

with col_map:
    lat, lon = get_coords(city)

    # Build folium map
    m = folium.Map(
        location=[lat, lon],
        zoom_start=7,
        tiles="CartoDB dark_matter",
    )

    # Add a marker for the selected city
    folium.CircleMarker(
        location=[lat, lon],
        radius=12,
        color="#4f9cf9",
        fill=True,
        fill_color="#4f9cf9",
        fill_opacity=0.8,
        popup=folium.Popup(
            f"<b>{city}</b><br>"
            f"Tmax: {df_f['temp_max'].max():.1f}°C<br>"
            f"Tmin: {df_f['temp_min'].min():.1f}°C",
            max_width=180,
        ),
        tooltip=city,
    ).add_to(m)

    # Faint markers for all other cities
    for c in cities:
        if c == city:
            continue
        clat, clon = get_coords(c)
        folium.CircleMarker(
            location=[clat, clon],
            radius=6,
            color="#7a84a0",
            fill=True,
            fill_color="#7a84a0",
            fill_opacity=0.4,
            tooltip=c,
        ).add_to(m)

    st_folium(m, height=360, width='stretch')

with col_stats:
    row = df_stats[df_stats["city"] == city]
    if not row.empty:
        r = row.iloc[0]
        st.write(r.to_dict())

        def fmt(col, decimals=1, unit=""):
            val = r.get(col)
            if val is None or (isinstance(val, float) and pd.isna(val)):
                return "—"
            return f"{float(val):.{decimals}f}{(' ' + unit) if unit else ''}"
        
        st.markdown(f"""
        <div style='background:#181c27;border:1px solid #252b3b;border-radius:14px;padding:1.4rem'>
            <p style='color:#7a84a0;font-size:.75rem;text-transform:uppercase;letter-spacing:.07em;margin-bottom:1rem'>Statistiques globales — {city}</p>
            <table style='width:100%;border-collapse:collapse;font-size:.95rem'>
              <tr>
                <td style='color:#7a84a0;padding:.4rem 0'>Avg. temperature</td>
                <td style='text-align:right;font-weight:600'>{fmt('temp_avg_all_time', 1, '°C')}</td>
              </tr>
              <tr>
                <td style='color:#7a84a0;padding:.4rem 0'>All-time max temperature</td>
                <td style='text-align:right;font-weight:600'>{fmt('temp_max_all_time', 1, '°C')}</td>
              </tr>
              <tr>
                <td style='color:#7a84a0;padding:.4rem 0'>All-time min temperature</td>
                <td style='text-align:right;font-weight:600'>{fmt('temp_min_all_time', 1, '°C')}</td>
              </tr>
              <tr>
                <td style='color:#7a84a0;padding:.4rem 0'>Avg. wind speed</td>
                <td style='text-align:right;font-weight:600'>{fmt('wind_avg_all_time', 1, " km/h")}</td>
              </tr>
            </table>
        </div>
        """, unsafe_allow_html=True)

st.divider()

##################################################################### Charts
if df_f.empty:
    st.warning("No data available for the selected period.")
else:
    CHART_LAYOUT = dict(
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor ="rgba(0,0,0,0)",
        font_color   ="#e8ecf4",
        font_family  ="DM Sans",
    )

    def apply_dark(fig):
        fig.update_layout(**CHART_LAYOUT)
        fig.update_xaxes(gridcolor="#252b3b", zerolinecolor="#252b3b")
        fig.update_yaxes(gridcolor="#252b3b", zerolinecolor="#252b3b")
        return fig

    st.markdown('<div class="section-title">Daily Temperatures</div>', unsafe_allow_html=True)
    st.plotly_chart(apply_dark(plot_daily_temperature(df_f)), width='stretch')

    c1, c2 = st.columns(2, gap="large")
    with c1:
        st.markdown('<div class="section-title">Precipitations</div>', unsafe_allow_html=True)
        st.plotly_chart(apply_dark(plot_precipitation(df_f)), width='stretch')
    with c2:
        st.markdown('<div class="section-title">Wind</div>', unsafe_allow_html=True)
        st.plotly_chart(apply_dark(plot_wind(df_f)), width='stretch')
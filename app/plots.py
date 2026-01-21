import plotly.express as px

def plot_daily_temperature(df):
    fig = px.line(
        df,
        x="date",
        y="temp_avg",
        title=f"Average daily temperature - {df['city'].iloc[0]}",
        markers=True
    )
    return fig

def plot_precipitation(df):
    fig = px.bar(
        df,
        x="date",
        y="precipitation_sum",
        title=f"Daily precipitations - {df['city'].iloc[0]}"
    )
    return fig

def plot_wind(df):
    fig = px.line(
        df,
        x="date",
        y="wind_avg",
        title=f"Average daily wind speed - {df['city'].iloc[0]}",
        markers=True
    )
    return fig
import pandas as pd
import numpy as np

def cap_outliers_iqr(series: pd.Series, factor: float = 1.5) -> pd.Series:
    Q1 = series.quantile(0.25)
    Q3 = series.quantile(0.75)
    IQR = Q3 - Q1
    lower = Q1 - factor * IQR
    upper = Q3 + factor * IQR
    return series.clip(lower=lower, upper=upper)


def clean_data(df: pd.DataFrame) -> pd.DataFrame:
    columns_to_keep = [
        "event_timestamp", "pm25_pollutants", "pm10_pollutants", "no2_pollutants",
        "so2_pollutants", "o3_pollutants", "co_pollutants",
        "temperature_weather", "humidity_weather", "wind_speed_weather",
        "wind_direction_weather", "pressure_weather", "precipitation_weather"
    ]
    df = df[columns_to_keep].copy()
    df.rename(columns={
        "pm25_pollutants": "pm25",
        "pm10_pollutants": "pm10",
        "temperature_weather": "temperature",
        "humidity_weather": "humidity",
        "wind_speed_weather": "wind_speed",
        "wind_direction_weather": "wind_direction",
        "pressure_weather": "pressure",
        "precipitation_weather": "precipitation"
    }, inplace=True)

    # Outlier capping
    for feature in ["pm10", "co_pollutants"]:
        df[feature] = cap_outliers_iqr(df[feature])

    # Ensure datetime
    df["event_timestamp"] = pd.to_datetime(df["event_timestamp"])
    return df

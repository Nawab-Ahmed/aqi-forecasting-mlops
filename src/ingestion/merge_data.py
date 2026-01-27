import pandas as pd
from datetime import datetime, timedelta
from ingestion import weather_client
from src.ingestion.aqi_client_live import AQICNLiveClient
from src.ingestion.openmeteo_historical_client import OpenMeteoHistoricalAQIClient
from src.ingestion.openmeteo_weather_historical_client import OpenMeteoWeatherHistoricalClient
from src.ingestion.station_resolver import AQICNStationResolver
from src.utils.logger import get_logger

logger = get_logger(__name__)

def flatten_dataframe(df: pd.DataFrame, column_to_flatten: str, suffix: str) -> pd.DataFrame:
    """Flatten a nested dict column and suffix new columns"""
    if column_to_flatten in df.columns:
        flat_cols = df[column_to_flatten].apply(pd.Series)
        flat_cols = flat_cols.add_suffix(f"_{suffix}")
        df = pd.concat([df.drop(columns=[column_to_flatten]), flat_cols], axis=1)
    return df

def merge_historical_data(city: str, token: str):
    """Fetch and merge historical pollutants + weather into a single dataframe"""
    logger.info(f"Fetching and merging historical data for {city}")

    # --- Resolve station ---
    resolver = AQICNStationResolver(token=token, city=city)
    station = resolver.resolve()
    lat, lon = station["geo"]

    # --- Historical pollutants (AQI) ---
    end_date = datetime.utcnow().date()
    start_date = end_date - timedelta(days=120)  # last 4 months
    pollutants_client = OpenMeteoHistoricalAQIClient(latitude=lat, longitude=lon,
                                                     start_date=start_date, end_date=end_date)
    pollutants_df = pd.DataFrame(pollutants_client.fetch())
    pollutants_df = flatten_dataframe(pollutants_df, "pollutants", "pollutants")

    # --- Historical weather ---
    weather_client = OpenMeteoWeatherHistoricalClient(city=city, latitude=lat, longitude=lon)
    end_date = datetime.utcnow()
    start_date = end_date - timedelta(days=120)
    weather_df = pd.DataFrame(weather_client.fetch(start_date=start_date, end_date=end_date))
    weather_df = flatten_dataframe(weather_df, "weather", "weather")

    # --- Ensure UTC timestamps ---
    pollutants_df["event_timestamp"] = pd.to_datetime(pollutants_df["event_timestamp"], utc=True)
    weather_df["event_timestamp"] = pd.to_datetime(weather_df["event_timestamp"], utc=True)

    # --- Merge ---
    merged_df = pd.merge(pollutants_df, weather_df, on="event_timestamp", how="inner")
    logger.info(f"Merged historical data shape: {merged_df.shape}")
    return merged_df

def merge_live_data(city: str, token: str, weather_df: pd.DataFrame):
    """Fetch live AQI and merge with closest weather record"""
    logger.info(f"Fetching live AQI for {city}")
    live_client = AQICNLiveClient(city=city, token=token)

    # Fetch live data with retries
    for attempt in range(3):
        try:
            live_data = live_client.fetch()
            break
        except Exception as e:
            logger.warning(f"Attempt {attempt+1} failed: {e}")
    else:
        raise RuntimeError("Failed to fetch live AQI data after 3 attempts")

    live_df = pd.DataFrame([live_data])
    live_df = flatten_dataframe(live_df, "pollutants", "pollutants")

    # Align closest weather timestamp
    weather_df["event_timestamp"] = pd.to_datetime(weather_df["event_timestamp"], utc=True)
    live_ts = pd.to_datetime(live_df["event_timestamp"].iloc[0], utc=True)
    closest_weather = weather_df.iloc[(weather_df["event_timestamp"] - live_ts).abs().argsort()[:1]]

    live_merged_df = pd.merge(
        live_df,
        closest_weather,
        on="event_timestamp",
        how="left",
        suffixes=('_pollutants', '_weather')
    )

    logger.info(f"Merged live data shape: {live_merged_df.shape}")
    return live_merged_df

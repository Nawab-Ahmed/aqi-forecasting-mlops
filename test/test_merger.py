import os
import sys
from dotenv import load_dotenv
import pandas as pd
from datetime import datetime, timezone

# --- Fix Python module path ---
CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.join(CURRENT_DIR, "..")
sys.path.insert(0, PROJECT_ROOT)

# --- Import project modules ---
from src.ingestion.merge_data import merge_historical_data, merge_live_data
from src.utils.logger import get_logger

# --- Load environment variables ---
load_dotenv()
logger = get_logger(__name__)

token = os.getenv("AQICN_API_TOKEN")
if not token:
    raise RuntimeError("AQICN_API_TOKEN not set in .env")

city = "karachi"

# --- Step 1: Merge historical pollutants + weather ---
logger.info("Starting historical data merge")
historical_df = merge_historical_data(city=city, token=token)

print("\n=== Historical Data Sample ===")
print(historical_df.head())
print(f"Shape: {historical_df.shape}")

# Basic sanity checks
required_pollutants = ["pm25_pollutants", "pm10_pollutants", "no2_pollutants"]
required_weather = ["temperature_weather", "humidity_weather", "wind_speed_weather"]
for col in required_pollutants + required_weather:
    assert col in historical_df.columns, f"Missing column: {col}"

# --- Step 2: Merge live AQI with latest weather ---
logger.info("Starting live data merge")
live_df = merge_live_data(city=city, token=token, weather_df=historical_df)

print("\n=== Live Data Sample ===")
print(live_df.head())
print(f"Shape: {live_df.shape}")

# Sanity checks for live data
assert "aqi" in live_df.columns or "aqi_pollutants" in live_df.columns, "Live data missing AQI"
assert "temperature_weather" in live_df.columns, "Live data missing weather info"

# --- Step 3: Timestamp checks ---
historical_times = historical_df["event_timestamp"]
live_times = live_df["event_timestamp"]
assert historical_times.max() >= live_times.min(), "Historical data does not cover live timestamp"

logger.info("Merge test completed successfully")
print("\n✅ Merge test completed successfully")

import os
from datetime import datetime
import pandas as pd
from datetime import timezone
from pymongo import MongoClient
from src.ingestion.merge_data import merge_historical_data
from src.preprocessing.feature_engineering import (
    cap_outliers_iqr,
    create_time_features,
    create_pollutant_change,
    create_lags,
    create_rolling_stats,
    create_targets,
    scale_features
)

# ---------------- Constants ----------------
PROJECT_ROOT = os.getcwd()
DATA_DIR = os.path.join(PROJECT_ROOT, "data/processed")
os.makedirs(DATA_DIR, exist_ok=True)

CITY = "karachi"
TOKEN = "361cf32474f50ec4ff9f3f677392739e1bfd8384"
VERSION = "v1.0"
FEATURE_SET_NAME = "aqi_features"

POLLUTANT_FEATURES = ['pm25', 'pm10', 'co_pollutants', 'no2_pollutants', 'so2_pollutants', 'o3_pollutants']
WEATHER_TIME_FEATURES = ["temperature", "humidity", "wind_speed", "wind_direction", "pressure", "precipitation", "hour", "day", "weekday", "month"]
LAG_HOURS = [1, 3, 6, 12, 24]
ROLLING_WINDOWS = [3, 6, 12]
TARGET_HORIZONS = [24, 48, 72]

# ---------------- Step 1: Fetch Historical Data ----------------
df = merge_historical_data(city=CITY, token=TOKEN)

# ---------------- Step 2: Basic Cleaning & Column Selection ----------------
columns_to_keep = [
    "event_timestamp",
    "pm25_pollutants", "pm10_pollutants", "no2_pollutants",
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

# ---------------- Step 3: Cap Outliers ----------------
for f in ["pm10", "co_pollutants"]:
    df[f] = cap_outliers_iqr(df[f])

# ---------------- Step 4: Feature Engineering ----------------
df = create_time_features(df)
df = create_pollutant_change(df, POLLUTANT_FEATURES)
df = create_lags(df, POLLUTANT_FEATURES, LAG_HOURS)
df = create_rolling_stats(df, POLLUTANT_FEATURES, ROLLING_WINDOWS)
df = create_targets(df, "pm25", TARGET_HORIZONS)
df = scale_features(df, POLLUTANT_FEATURES, WEATHER_TIME_FEATURES)

# ---------------- Step 5: Drop rows with missing target values ----------------
target_cols = [f"pm25_t_plus_{h}h" for h in TARGET_HORIZONS]
df.dropna(subset=target_cols, inplace=True)
df.reset_index(drop=True, inplace=True)

# ---------------- Step 6: Save Locally ----------------
feature_cols = [c for c in df.columns if c not in ["event_timestamp"] + target_cols]
df.to_parquet(os.path.join(DATA_DIR, "aqi_features_v1.parquet"))
pd.Series(feature_cols).to_json(os.path.join(DATA_DIR, "feature_columns_v1.json"))

# ---------------- Step 7: Upload to MongoDB Feature Store ----------------
client = MongoClient("mongodb://localhost:27017/")
db = client["aqi_feature_store"]
collection = db["features_v1"]

records = df.to_dict(orient="records")
for r in records:
    r.update({
        "feature_set_name": FEATURE_SET_NAME,
        "version": VERSION,
        "created_at": datetime.now(timezone.utc)
    })

collection.delete_many({"feature_set_name": FEATURE_SET_NAME, "version": VERSION})
collection.insert_many(records)

print(f"[INFO] Inserted {len(records)} records into MongoDB feature store, version={VERSION}")

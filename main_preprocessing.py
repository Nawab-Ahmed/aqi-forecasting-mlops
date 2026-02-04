import os
from datetime import datetime, timezone
import pandas as pd
from pymongo import MongoClient

from src.ingestion.merge_data import merge_historical_data
from src.preprocessing.data_cleaning import clean_data
from src.preprocessing.feature_engineering import (
    create_time_features,
    create_pollutant_change,
    create_lags,
    create_rolling_stats,
)
from src.preprocessing.target_engineering import create_future_targets
from src.preprocessing.scaling import scale_features

# ============================================================
# Configuration
# ============================================================

PROJECT_ROOT = os.getcwd()
DATA_DIR = os.path.join(PROJECT_ROOT, "data/processed")
os.makedirs(DATA_DIR, exist_ok=True)

CITY = "karachi"
TOKEN = "361cf32474f50ec4ff9f3f677392739e1bfd8384"

FEATURE_SET_NAME = "aqi_features"
VERSION = "v2.0"   # bump version because logic changed

POLLUTANT_FEATURES = [
    "pm10", "co_pollutants",
    "no2_pollutants", "so2_pollutants", "o3_pollutants"
]


WEATHER_FEATURES = [
    "temperature", "humidity", "wind_speed",
    "wind_direction", "pressure", "precipitation"
]

LAG_HOURS = [1, 3, 6, 12, 24]
ROLLING_WINDOWS = [3, 6, 12]
TARGET_HORIZONS = [24, 48, 72]

TARGET_COL = "pm25"

# ============================================================
# Step 1: Fetch raw historical data
# ============================================================

df_raw = merge_historical_data(city=CITY, token=TOKEN)

# ============================================================
# Step 2: Cleaning & canonical column names
# ============================================================

df_raw = clean_data(df_raw)

# IMPORTANT:
# Keep a RAW copy for target creation
df_features = df_raw.copy()

# ============================================================
# Step 3: Feature engineering (NO TARGETS, NO SCALING)
# ============================================================

df_features = create_time_features(df_features)
df_features = create_pollutant_change(df_features, POLLUTANT_FEATURES)
df_features = create_lags(df_features, POLLUTANT_FEATURES, LAG_HOURS)
df_features = create_rolling_stats(df_features, POLLUTANT_FEATURES, ROLLING_WINDOWS)

# ============================================================
# Step 4: Target engineering (RAW pm25 ONLY)
# ============================================================

df_targets = create_future_targets(
    df=df_raw[[ "event_timestamp", TARGET_COL ]],
    target_col=TARGET_COL,
    horizons=TARGET_HORIZONS
)

# ============================================================
# Step 5: Merge features + targets
# ============================================================

df = df_features.merge(
    df_targets,
    on="event_timestamp",
    how="inner"
)

# Drop rows with missing future targets
target_cols = [f"{TARGET_COL}_t_plus_{h}h" for h in TARGET_HORIZONS]
df.dropna(subset=target_cols, inplace=True)
df.reset_index(drop=True, inplace=True)

# ============================================================
# Step 6: Scale FEATURES ONLY
# ============================================================

TIME_FEATURES = ["hour", "weekday", "month", "is_weekend"]

df, pollutant_scaler, weather_scaler = scale_features(
    df=df,
    pollutant_features=POLLUTANT_FEATURES,
    weather_features=WEATHER_FEATURES + TIME_FEATURES
)

# ============================================================
# Step 7: Persist locally
# ============================================================

feature_cols = [
    c for c in df.columns
    if c not in ["event_timestamp"] + target_cols
]

df.to_parquet(
    os.path.join(DATA_DIR, f"aqi_features_{VERSION}.parquet"),
    index=False
)

pd.Series(feature_cols).to_json(
    os.path.join(DATA_DIR, f"feature_columns_{VERSION}.json")
)

# ============================================================
# Step 8: Upload to MongoDB feature store
# ============================================================

client = MongoClient("mongodb://localhost:27017/")
db = client["aqi_feature_store"]
collection = db["features"]

records = df.to_dict(orient="records")

for r in records:
    r.update({
        "feature_set_name": FEATURE_SET_NAME,
        "version": VERSION,
        "created_at": datetime.now(timezone.utc)
    })

collection.delete_many({
    "feature_set_name": FEATURE_SET_NAME,
    "version": VERSION
})

collection.insert_many(records)

print(
    f"[INFO] Inserted {len(records)} records "
    f"into feature store | version={VERSION}"
)

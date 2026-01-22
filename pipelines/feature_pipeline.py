import os
import requests
import pandas as pd
from datetime import datetime, timedelta
from pymongo import MongoClient, UpdateOne
from dotenv import load_dotenv
from loguru import logger

# Load environment variables
load_dotenv()

# ------------------------------
# Configuration
# ------------------------------
AQICN_TOKEN = os.getenv("AQICN_TOKEN")
OPENWEATHER_API_KEY = os.getenv("OPENWEATHER_API_KEY")
MONGO_URI = os.getenv("MONGO_URI")

CITY = "karachi"
COLLECTION_NAME = "features_aqi_v1"
FEATURE_VERSION = "v1"

# ------------------------------
# MongoDB client
# ------------------------------
client = MongoClient(MONGO_URI)
db = client.get_database()  # uses database specified in URI
collection = db[COLLECTION_NAME]

# ------------------------------
# Fetch AQI from AQICN
# ------------------------------
def fetch_aqi(city: str):
    url = f"https://api.waqi.info/feed/{city}/?token={AQICN_TOKEN}"
    try:
        response = requests.get(url, timeout=10)
        response.raise_for_status()
        data = response.json()
        aqi = data.get("data", {}).get("aqi")
        ts = datetime.utcfromtimestamp(data.get("data", {}).get("time", {}).get("v", datetime.utcnow().timestamp()))
        return {"aqi": aqi, "timestamp": ts}
    except Exception as e:
        logger.error(f"Error fetching AQI: {e}")
        return None

# ------------------------------
# Fetch weather from OpenWeather
# ------------------------------
def fetch_weather(city: str):
    url = f"http://api.openweathermap.org/data/2.5/weather?q={city}&appid={OPENWEATHER_API_KEY}&units=metric"
    try:
        response = requests.get(url, timeout=10)
        response.raise_for_status()
        data = response.json()
        return {
            "temperature": data["main"]["temp"],
            "humidity": data["main"]["humidity"],
            "wind_speed": data["wind"]["speed"]
        }
    except Exception as e:
        logger.error(f"Error fetching weather: {e}")
        return None

# ------------------------------
# Compute lag and trend features
# ------------------------------
def compute_features(aqi_doc):
    # Convert timestamp to datetime
    ts = aqi_doc["timestamp"]

    # Get previous AQI values for lag features
    lag_1_doc = collection.find_one({"event_timestamp": ts - timedelta(hours=1)})
    lag_3_doc = collection.find_one({"event_timestamp": ts - timedelta(hours=3)})
    lag_24_doc = collection.find_one({"event_timestamp": ts - timedelta(hours=24)})

    aqi_lag_1 = lag_1_doc["features"]["aqi"] if lag_1_doc else None
    aqi_lag_3 = lag_3_doc["features"]["aqi"] if lag_3_doc else None
    aqi_lag_24 = lag_24_doc["features"]["aqi"] if lag_24_doc else None

    # Trend feature
    aqi_change_rate = ((aqi_doc["aqi"] - aqi_lag_1)/aqi_lag_1) if aqi_lag_1 else None

    # Temporal features
    features = {
        "aqi": aqi_doc["aqi"],
        "aqi_lag_1": aqi_lag_1,
        "aqi_lag_3": aqi_lag_3,
        "aqi_lag_24": aqi_lag_24,
        "aqi_change_rate": aqi_change_rate,
        "hour": ts.hour,
        "day": ts.day,
        "month": ts.month
    }

    # Merge weather
    weather = fetch_weather(CITY)
    if weather:
        features.update(weather)

    return features

# ------------------------------
# Insert features into MongoDB
# ------------------------------
def store_features(ts, features):
    doc = {
        "entity": {"city": CITY},
        "event_timestamp": ts,
        "features": features,
        "feature_version": FEATURE_VERSION,
        "source": "aqicn",
        "created_at": datetime.utcnow()
    }

    # Idempotent insert: update if timestamp already exists
    collection.update_one(
        {"entity.city": CITY, "event_timestamp": ts},
        {"$set": doc},
        upsert=True
    )
    logger.info(f"Stored features for {ts}")

# ------------------------------
# Main pipeline
# ------------------------------
def run_pipeline():
    aqi_data = fetch_aqi(CITY)
    if not aqi_data or aqi_data["aqi"] is None:
        logger.warning("No AQI data, skipping pipeline run.")
        return

    features = compute_features(aqi_data)
    store_features(aqi_data["timestamp"], features)
    logger.info("Feature pipeline run completed successfully.")

# ------------------------------
# Execute
# ------------------------------
if __name__ == "__main__":
    run_pipeline()

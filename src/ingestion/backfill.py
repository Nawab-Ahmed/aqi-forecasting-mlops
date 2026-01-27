from datetime import datetime, timedelta
from mongo import MongoHandler
from hourly_weather_client import fetch_weather_hourly
from ingestion.aqi_client_live import fetch_aqi
import os
from dotenv import load_dotenv
from time import sleep

load_dotenv()

CITY = os.getenv("CITY", "Karachi")
STATE = os.getenv("STATE", "Sindh")
COUNTRY = os.getenv("COUNTRY", "Pakistan")
LATITUDE = float(os.getenv("LATITUDE", 24.8607))
LONGITUDE = float(os.getenv("LONGITUDE", 67.0011))

mongo = MongoHandler()


def run_hourly_backfill(start_date: str, end_date: str):
    """
    Fetch hourly weather + AQI data and upsert into MongoDB
    """
    start = datetime.strptime(start_date, "%Y-%m-%d")
    end = datetime.strptime(end_date, "%Y-%m-%d")
    current = start

    while current <= end:
        day_str = current.strftime("%Y-%m-%d")
        print(f"[Backfill] Fetching {CITY} hourly data for {day_str}")

        # 1️⃣ Fetch hourly weather
        weather_data = fetch_weather_hourly(LATITUDE, LONGITUDE, start_date=day_str, end_date=day_str)

        # 2️⃣ Merge AQI from IQAir for the day
        aqi_data = fetch_aqi(CITY, STATE, COUNTRY)

        # 3️⃣ Combine weather + AQI
        records_to_upsert = []
        for hour_record in weather_data:
            record = {
                "city": CITY,
                **hour_record,
                **aqi_data,
                "source_aqi": "IQAir",
                "source_weather": "Open-Meteo",
                "created_at": datetime.utcnow().isoformat()
            }
            records_to_upsert.append(record)

        # 4️⃣ Upsert batch
        mongo.upsert_records(records_to_upsert)

        # Sleep between days to respect API limits
        sleep(1)
        current += timedelta(days=1)


if __name__ == "__main__":
    # Example: backfill last 7 days
    run_hourly_backfill("2025-01-20", "2025-01-26")

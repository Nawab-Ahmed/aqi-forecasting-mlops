from datetime import datetime, timedelta
from storage.mongo import MongoHandler
from ingestion.weather_client import fetch_weather_batch
from ingestion.aqi_client import fetch_aqi
import os
from dotenv import load_dotenv
from time import sleep

load_dotenv()

CITY = os.getenv("CITY")
STATE = os.getenv("STATE")
COUNTRY = os.getenv("COUNTRY")
LATITUDE = float(os.getenv("LATITUDE"))
LONGITUDE = float(os.getenv("LONGITUDE"))

mongo = MongoHandler()

def run_backfill(start_date: str, end_date: str, batch_size: int = 10):
    """
    Production-ready sequential backfill:
    - Fetch weather in batches
    - Fetch AQI per day with retries
    - Skip duplicates
    - Upsert into MongoDB
    """
    start = datetime.strptime(start_date, "%Y-%m-%d")
    end = datetime.strptime(end_date, "%Y-%m-%d")
    current = start

    try:
        while current <= end:
            batch_end = min(current + timedelta(days=batch_size - 1), end)
            print(f"[Backfill] Fetching batch: {current.date()} → {batch_end.date()}")

            # Fetch weather batch
            weather_batch = fetch_weather_batch(
                LATITUDE,
                LONGITUDE,
                current.strftime("%Y-%m-%d"),
                batch_end.strftime("%Y-%m-%d")
            )

            for day in weather_batch:
                # Skip if record already exists
                existing = mongo.get_record(CITY, day["date"])
                if existing and existing.get("aqi_avg") is not None:
                    print(f"[Mongo] Skipping {CITY} on {day['date']} (already exists)")
                    continue

                # Fetch AQI with retries
                aqi_data = fetch_aqi(CITY, STATE, COUNTRY)

                # Build record
                record = {
                    "city": CITY,
                    "date": day["date"],
                    **day,
                    **aqi_data,
                    "source_aqi": "IQAir",
                    "source_weather": "Open-Meteo",
                    "created_at": datetime.utcnow()
                }

                # Upsert into MongoDB
                mongo.upsert_record(record)

                # Sleep to respect rate limits
                sleep(1.5)

            current = batch_end + timedelta(days=1)

    except KeyboardInterrupt:
        print("\n[Backfill] Interrupted by user. Exiting safely...")

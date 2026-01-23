from pymongo import MongoClient
from datetime import datetime
import os
from dotenv import load_dotenv

load_dotenv()

MONGO_URI = os.getenv("MONGO_URI")
DB_NAME = "aqi_feature_store"
COLLECTION_NAME = "raw_aqi_weather_daily"

class MongoHandler:
    def __init__(self, uri=MONGO_URI):
        self.client = MongoClient(uri)
        self.db = self.client[DB_NAME]
        self.collection = self.db[COLLECTION_NAME]

    def upsert_record(self, record: dict):
        """Upsert a single record based on city + date."""
        if "city" not in record or "date" not in record:
            raise ValueError("Record must have 'city' and 'date' fields")

        self.collection.update_one(
            {"city": record["city"], "date": record["date"]},
            {"$set": record},
            upsert=True
        )
        print(f"[Mongo] Upserted record for {record['city']} on {record['date']}")

    def get_record(self, city: str, date: str):
        """Return a single record if it exists, else None."""
        return self.collection.find_one({"city": city, "date": date})

from pymongo import MongoClient, ASCENDING
from datetime import datetime
import os
from dotenv import load_dotenv

load_dotenv()

MONGO_URI = os.getenv("MONGO_URI")
DB_NAME = "aqi_feature_store"
HOURLY_COLLECTION = "raw_aqi_weather_hourly"


class MongoHandler:
    def __init__(self, uri: str = MONGO_URI):
        self.client = MongoClient(uri)
        self.db = self.client[DB_NAME]
        self.collection = self.db[HOURLY_COLLECTION]

        # Ensure unique constraint (industry best practice)
        self.collection.create_index(
            [("city", ASCENDING), ("timestamp", ASCENDING)],
            unique=True
        )

    def upsert_hourly_record(self, record: dict):
        """
        Upsert a single hourly AQI + weather record.
        Enforces city + timestamp uniqueness.
        """
        required_fields = ["city", "timestamp", "aqi", "weather"]

        for field in required_fields:
            if field not in record:
                raise ValueError(f"Missing required field: {field}")

        self.collection.update_one(
            {
                "city": record["city"],
                "timestamp": record["timestamp"]
            },
            {"$set": record},
            upsert=True
        )

    def get_hourly_record(self, city: str, timestamp: datetime):
        return self.collection.find_one(
            {"city": city, "timestamp": timestamp}
        )

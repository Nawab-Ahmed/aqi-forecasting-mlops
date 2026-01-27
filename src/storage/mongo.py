from pymongo import MongoClient, UpdateOne
from datetime import datetime
import os
from dotenv import load_dotenv

load_dotenv()

MONGO_URI = os.getenv("MONGO_URI", "mongodb://localhost:27017/")
DB_NAME = os.getenv("DB_NAME", "aqi_feature_store")
COLLECTION_NAME = os.getenv("COLLECTION_NAME", "hourly_aqi_weather")


class MongoHandler:
    def __init__(self, uri=MONGO_URI, db_name=DB_NAME, collection_name=COLLECTION_NAME):
        self.client = MongoClient(uri)
        self.db = self.client[db_name]
        self.collection = self.db[collection_name]

    def upsert_record(self, record: dict):
        """Upsert a single record based on city + timestamp."""
        if "city" not in record or "timestamp" not in record:
            raise ValueError("Record must have 'city' and 'timestamp' fields")

        self.collection.update_one(
            {"city": record["city"], "timestamp": record["timestamp"]},
            {"$set": record},
            upsert=True
        )

    def upsert_records(self, records: list):
        """Batch upsert multiple records using bulk operations (optimized for large datasets)."""
        if not records:
            return

        operations = []
        for record in records:
            if "city" not in record or "timestamp" not in record:
                continue
            operations.append(
                UpdateOne(
                    {"city": record["city"], "timestamp": record["timestamp"]},
                    {"$set": record},
                    upsert=True
                )
            )

        if operations:
            result = self.collection.bulk_write(operations)
            print(f"[Mongo] Upserted {result.upserted_count + result.modified_count} records")

    def get_record(self, city: str, timestamp: str):
        """Retrieve a single record by city + timestamp."""
        return self.collection.find_one({"city": city, "timestamp": timestamp})

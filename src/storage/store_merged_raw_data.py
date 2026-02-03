from datetime import datetime, timezone
from typing import Optional
import pandas as pd
from pymongo import MongoClient, ASCENDING

MONGO_URI = "mongodb://localhost:27017"
DB_NAME = "aqi_project"
COLLECTION_NAME = "raw_merged_data"


class RawMergedDataRepository:
    def __init__(self, uri: str = MONGO_URI):
        self.client = MongoClient(uri)
        self.collection = self.client[DB_NAME][COLLECTION_NAME]
        self._ensure_indexes()

    def _ensure_indexes(self):
        self.collection.create_index(
            [("city", ASCENDING), ("event_timestamp", ASCENDING)],
            unique=True
        )

    def insert_dataframe(
        self,
        df: pd.DataFrame,
        city: str,
        version: Optional[str] = None
    ) -> str:
        if df.empty:
            return ""

        version = version or datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")

        df = df.copy()
        df["event_timestamp"] = pd.to_datetime(df["event_timestamp"], utc=True)

        records = [
            {
                "city": city.lower(),
                "event_timestamp": row["event_timestamp"].to_pydatetime(),
                "data": row.drop(labels=["event_timestamp"]).to_dict(),
                "version": version,
                "ingested_at": datetime.now(timezone.utc)
            }
            for _, row in df.iterrows()
        ]

        self.collection.insert_many(records, ordered=False)
        return version

    def close(self):
        self.client.close()

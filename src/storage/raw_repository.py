from datetime import datetime, timezone
from typing import Optional
import pandas as pd
from pymongo import MongoClient, ASCENDING
import os
from dotenv import load_dotenv
import logging

# Load environment variables
load_dotenv()

MONGO_URI = os.getenv("MONGO_URI", "mongodb://localhost:27017")
DB_NAME = "aqi_project"
RAW_COLLECTION = "raw_aqi_weather"

logger = logging.getLogger("RawMergedDataRepository")
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s"
)


def prepare_for_mongo(df: pd.DataFrame) -> pd.DataFrame:
    """
    Flatten, sanitize, and convert all timestamps to UTC for MongoDB insertion.
    Drops rows with missing event_timestamp and converts remaining timestamps
    to Python datetime objects to avoid NaT issues.
    """
    df = df.copy()

    for col in df.columns:
        if "timestamp" in col or "ingested_at" in col:
            df[col] = pd.to_datetime(df[col], errors="coerce", utc=True)

    # Drop rows with missing main event_timestamp
    missing_ts = df["event_timestamp"].isna().sum()
    if missing_ts > 0:
        logger.warning(f"Dropping {missing_ts} rows with missing event_timestamp")
        df = df.dropna(subset=["event_timestamp"])

    # Convert remaining timestamps to python datetime
    for col in df.columns:
        if "timestamp" in col or "ingested_at" in col:
            df[col] = df[col].apply(lambda x: x.to_pydatetime() if pd.notna(x) else None)

    # Optional: rename confusing columns
    rename_map = {
        "city_x": "city",
        "ingested_at_x": "pollutants_ingested_at",
        "ingested_at_y": "weather_ingested_at"
    }
    df = df.rename(columns={k: v for k, v in rename_map.items() if k in df.columns})

    return df


class RawMergedDataRepository:
    """
    Repository class to handle raw merged AQI + weather data ingestion into MongoDB.
    Ensures:
      - UTC timestamps
      - Versioning
      - Unique constraint on city + timestamp
      - Safe handling of missing timestamps
    """
    def __init__(self, uri: str = MONGO_URI):
        self.client = MongoClient(uri)
        self.collection = self.client[DB_NAME][RAW_COLLECTION]
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
            logger.warning("Empty dataframe provided, skipping insertion.")
            return ""

        version = version or datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
        df = df.copy()

        # Use the helper to prepare the dataframe
        df = prepare_for_mongo(df)

        # Prepare records for MongoDB
        records = []
        for _, row in df.iterrows():
            ts = row["event_timestamp"]
            data_dict = row.drop(labels=["event_timestamp"]).to_dict()

            # Convert any timestamps inside data_dict to Python datetime
            for k, v in data_dict.items():
                if isinstance(v, pd.Timestamp):
                    data_dict[k] = v.to_pydatetime() if pd.notna(v) else None

            records.append({
                "city": city.lower(),
                "event_timestamp": ts.to_pydatetime(),
                "data": data_dict,
                "version": version,
                "ingested_at": datetime.now(timezone.utc)
            })

        if records:
            self.collection.insert_many(records, ordered=False)
            logger.info(f"Inserted {len(records)} records for city '{city}' (version: {version})")
        else:
            logger.warning("No valid records to insert after processing timestamps.")

        return version

    def close(self):
        self.client.close()

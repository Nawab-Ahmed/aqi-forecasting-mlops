import sys
from pathlib import Path
import pandas as pd
import logging
from dotenv import load_dotenv

# Step 1: Project root
PROJECT_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(PROJECT_ROOT))
print("Project root added to sys.path:", PROJECT_ROOT)

# Step 2: Imports
from src.ingestion.merge_data import merge_historical_data, merge_live_data
from src.storage.raw_repository import RawMergedDataRepository, prepare_for_mongo
from src.utils.logger import get_logger

# Step 3: Load env
load_dotenv()
TOKEN = "361cf32474f50ec4ff9f3f677392739e1bfd8384"
CITY = "Karachi"

# Logging
logger = get_logger("raw_ingestion")
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")

def main():
    logger.info(f"Starting raw data ingestion for {CITY}")

    # --- Historical data
    historical_df = merge_historical_data(city=CITY, token=TOKEN)

    # --- Live data
    live_df = merge_live_data(city=CITY, token=TOKEN, weather_df=historical_df)

    # --- Combine
    raw_df = pd.concat([historical_df, live_df], ignore_index=True)
    if raw_df.empty:
        logger.warning("No data to ingest. Exiting.")
        return

    # --- Prepare for MongoDB
    raw_df = prepare_for_mongo(raw_df)

    # --- Insert
    repo = RawMergedDataRepository()
    version = repo.insert_dataframe(df=raw_df, city=CITY)
    repo.close()

    logger.info(f"Raw merged data stored successfully. Version: {version}, Rows: {len(raw_df)}")

    # --- Preview last 5 rows
    last_records = raw_df.sort_values("event_timestamp", ascending=False).head(5)
    logger.info("Last 5 records preview:")
    logger.info(last_records.to_dict(orient="records"))

if __name__ == "__main__":
    main()

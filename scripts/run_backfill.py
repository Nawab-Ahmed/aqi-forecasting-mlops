import sys
import os
import yaml

# Add src/ to path
sys.path.append(os.path.join(os.path.dirname(__file__), "..", "src"))

from ingestion.backfill import run_backfill

# Load config
with open(os.path.join(os.path.dirname(__file__), "..", "src", "config", "ingestion.yaml")) as f:
    config = yaml.safe_load(f)

start_date = config["date_range"]["start_date"]
end_date = config["date_range"]["end_date"]
batch_size = config.get("batch_size_days", 10)

if __name__ == "__main__":
    run_backfill(start_date, end_date, batch_size)

# scripts/data_health_check.py
from pymongo import MongoClient
import os
from dotenv import load_dotenv
import pandas as pd

load_dotenv()

MONGO_URI = os.getenv("MONGO_URI")
DB_NAME = "aqi_feature_store"
COLLECTION_NAME = "raw_aqi_weather_daily"

# Connect to MongoDB
client = MongoClient(MONGO_URI)
db = client[DB_NAME]
collection = db[COLLECTION_NAME]

# Load all data into a DataFrame
data = pd.DataFrame(list(collection.find()))

if data.empty:
    print("No data found in MongoDB!")
else:
    # 1️⃣ Total records
    print(f"Total records: {len(data)}\n")

    # 2️⃣ Records per city
    print("Records per city:")
    print(data['city'].value_counts(), "\n")

    # 3️⃣ Check missing/null values
    print("Null counts:")
    print(data.isnull().sum(), "\n")

    # 4️⃣ Date coverage per city
    print("Date range per city:")
    for city in data['city'].unique():
        city_dates = pd.to_datetime(data[data['city'] == city]['date'])
        print(f"{city}: {city_dates.min().date()} → {city_dates.max().date()}")

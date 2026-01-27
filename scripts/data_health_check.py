# scripts/data_health_check.py
import os
from dotenv import load_dotenv
import pandas as pd
import matplotlib.pyplot as plt
from pymongo import MongoClient

load_dotenv()

MONGO_URI = os.getenv("MONGO_URI")
DB_NAME = "aqi_feature_store"
COLLECTION_NAME = "raw_aqi_weather_hourly"  # Update for hourly

# Connect to MongoDB
client = MongoClient(MONGO_URI)
db = client[DB_NAME]
collection = db[COLLECTION_NAME]

# Load data safely (hourly datasets may be huge)
cursor = collection.find({}, batch_size=1000)
data = pd.DataFrame(list(cursor))

if data.empty:
    print("❌ No data found in MongoDB!")
else:
    print(f"✅ Total records: {len(data)}\n")

    # Convert dates safely
    data['date'] = pd.to_datetime(data['date'], errors='coerce')

    # Records per city
    print("Records per city:")
    print(data['city'].value_counts(), "\n")

    # Missing values per city
    print("Missing values per city:")
    print(data.groupby('city').apply(lambda x: x.isnull().sum()), "\n")

    # Duplicates
    print("Duplicate records (city + date):", data.duplicated(subset=['city', 'date']).sum(), "\n")

    # Date coverage per city
    print("Date range per city:")
    for city in data['city'].unique():
        city_dates = data[data['city'] == city]['date']
        print(f"{city}: {city_dates.min()} → {city_dates.max()}")

    # Optional plot
    data['city'].value_counts().plot(kind='bar', title="Records per City")
    plt.show()

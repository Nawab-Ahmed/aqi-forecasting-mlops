from pymongo import MongoClient
import requests
from datetime import datetime, timedelta
import time
import os
from dotenv import load_dotenv

load_dotenv()

AQICN_TOKEN = os.getenv("AQICN_TOKEN")
OPENWEATHER_API_KEY = os.getenv("OPENWEATHER_API_KEY")
MONGO_URI = os.getenv("MONGO_URI")

CITY = "karachi"

# Connect to MongoDB
client = MongoClient(MONGO_URI)
db = client.get_database()
raw_collection = db['raw_aqi_weather']

# Historical date range
start_date = datetime(2025, 10, 25)  # 90 days ago
end_date = datetime(2026, 1, 23)
current = start_date

while current <= end_date:
    ts_unix = int(current.timestamp())

    # 1️⃣ Fetch AQI
    try:
        url_aqi = f"https://api.waqi.info/feed/{CITY}/?token={AQICN_TOKEN}"
        r = requests.get(url_aqi, timeout=10)
        r.raise_for_status()
        aqi = r.json().get("data", {}).get("aqi")
    except Exception as e:
        print(f"AQI fetch failed for {current}: {e}")
        aqi = None

    # 2️⃣ Fetch Weather
    try:
        url_weather = f"http://api.openweathermap.org/data/2.5/weather?q={CITY}&appid={OPENWEATHER_API_KEY}&units=metric"
        r = requests.get(url_weather, timeout=10)
        r.raise_for_status()
        data = r.json()
        temperature = data["main"]["temp"]
        humidity = data["main"]["humidity"]
        wind_speed = data["wind"]["speed"]
    except Exception as e:
        print(f"Weather fetch failed for {current}: {e}")
        temperature = humidity = wind_speed = None

    # 3️⃣ Store in MongoDB
    raw_collection.update_one(
        {"entity.city": CITY, "event_timestamp": current},
        {"$set": {
            "entity": {"city": CITY},
            "event_timestamp": current,
            "aqi": aqi,
            "temperature": temperature,
            "humidity": humidity,
            "wind_speed": wind_speed,
            "source": "aqicn/openweather",
            "created_at": datetime.utcnow()
        }},
        upsert=True
    )
    print(f"Stored data for {current}")

    # Move to next hour
    current += timedelta(hours=1)

    # 4️⃣ Throttle requests to avoid API limits
    time.sleep(1)

import os
from datetime import datetime, timedelta
from src.ingestion.openmeteo_weather_historical_client import OpenMeteoWeatherHistoricalClient

# Karachi coordinates
latitude = 24.8415
longitude = 67.0091
city = "karachi"

client = OpenMeteoWeatherHistoricalClient(city=city, latitude=latitude, longitude=longitude)

# Fetch last 4 months hourly data
end_date = datetime.utcnow()
start_date = end_date - timedelta(days=120)

data = client.fetch(start_date=start_date, end_date=end_date)

print(f"Fetched {len(data)} hourly weather records")
print(data[:3])  # preview first 3 records

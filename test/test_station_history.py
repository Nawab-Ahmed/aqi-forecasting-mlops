import os
from dotenv import load_dotenv

from src.ingestion.station_resolver import AQICNStationResolver
from src.ingestion.aqi_station_historical import AQICNStationHistoricalClient

load_dotenv()

token = os.getenv("AQICN_API_TOKEN")
if not token:
    raise RuntimeError("AQICN_API_TOKEN not set")

# 1️⃣ Resolve Karachi → station
resolver = AQICNStationResolver(token=token, city="karachi")
station = resolver.resolve()

print("Resolved station:", station)

# 2️⃣ Fetch hourly historical data
client = AQICNStationHistoricalClient(
    token=token,
    station_id=station["station_id"]
)

data = client.fetch()

print(f"\nFetched {len(data)} hourly records")
print("Sample records:")
for row in data[:3]:
    print(row)

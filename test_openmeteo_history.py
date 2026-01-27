from datetime import date, timedelta

from src.ingestion.openmeteo_historical_client import (
    OpenMeteoHistoricalAQIClient
)

# Karachi coordinates
LAT = 24.8607
LON = 67.0011

end_date = date.today()
start_date = end_date - timedelta(days=120)

client = OpenMeteoHistoricalAQIClient(
    latitude=LAT,
    longitude=LON,
    start_date=start_date.isoformat(),
    end_date=end_date.isoformat()
)

data = client.fetch()

print(f"Fetched {len(data)} hourly records")
print(data[:3])

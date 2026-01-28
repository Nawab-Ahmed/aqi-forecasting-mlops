import os
from src.ingestion.aqi_client_live import AQICNLiveClient
from dotenv import load_dotenv

# Load .env
load_dotenv()

token = os.getenv("AQICN_API_TOKEN")
if not token:
    raise RuntimeError("AQICN_API_TOKEN not set")

# Create client
client = AQICNLiveClient(token=token, city="karachi")

# Fetch live AQI
data = client.fetch()

# Print normalized data
print(data)

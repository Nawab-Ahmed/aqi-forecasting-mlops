import requests
import os
from dotenv import load_dotenv
from time import sleep
from datetime import datetime

load_dotenv()
IQAIR_API_KEY = os.getenv("IQAIR_API_KEY")


def fetch_aqi(city: str, state: str = "Sindh", country: str = "Pakistan",
              retries: int = 5, backoff_factor: int = 2,
              initial_wait: int = 5, max_wait: int = 60):
    """
    Robust AQI fetch:
    - Handles rate limit (429)
    - Exponential backoff
    - Returns structured dict
    """
    url = f"https://api.airvisual.com/v2/city?city={city}&state={state}&country={country}&key={IQAIR_API_KEY}"
    wait_time = initial_wait

    for attempt in range(1, retries + 1):
        try:
            response = requests.get(url, timeout=10)

            # Handle rate limit
            if response.status_code == 429:
                print(f"[AQI] Rate limit hit for {city}. Waiting {wait_time}s...")
                sleep(wait_time)
                wait_time = min(wait_time * backoff_factor, max_wait)
                continue

            response.raise_for_status()
            data = response.json().get("data", {}).get("current", {}).get("pollution", {})

            if not data:
                print(f"[AQI] No data returned for {city}.")
                return {"aqi_avg": None, "aqi_min": None, "aqi_max": None, "timestamp": None}

            return {
                "aqi_avg": data.get("aqius"),
                "aqi_min": None,
                "aqi_max": None,
                "timestamp": datetime.utcnow()
            }

        except requests.RequestException as e:
            print(f"[AQI] Attempt {attempt} failed for {city}: {e}. Retrying in {wait_time}s...")
            sleep(wait_time)
            wait_time = min(wait_time * backoff_factor, max_wait)

    print(f"[AQI] Permanent failure for {city} after {retries} attempts.")
    return {"aqi_avg": None, "aqi_min": None, "aqi_max": None, "timestamp": None}

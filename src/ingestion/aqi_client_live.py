import requests
import os
from dotenv import load_dotenv
from time import sleep
from datetime import datetime

load_dotenv()
IQAIR_API_KEY = os.getenv("IQAIR_API_KEY")


def fetch_aqi(city: str, state: str = "Sindh", country: str = "Pakistan",
              retries: int = 5, backoff_factor: int = 2,
              initial_wait: int = 5, max_wait: int = 60) -> dict:
    """
    Fetch current AQI from IQAir with retries and exponential backoff.
    Returns a dict with 'aqi_avg', 'aqi_min', 'aqi_max', 'timestamp'.
    """
    url = f"https://api.airvisual.com/v2/city?city={city}&state={state}&country={country}&key={IQAIR_API_KEY}"
    wait_time = initial_wait

    for attempt in range(1, retries + 1):
        try:
            response = requests.get(url, timeout=10)

            if response.status_code == 429:
                print(f"[AQI] Rate limit hit. Waiting {wait_time}s...")
                sleep(wait_time)
                wait_time = min(wait_time * backoff_factor, max_wait)
                continue

            response.raise_for_status()
            pollution = response.json().get("data", {}).get("current", {}).get("pollution", {})

            if not pollution:
                return {"aqi_avg": None, "aqi_min": None, "aqi_max": None, "timestamp": datetime.utcnow().isoformat()}

            return {
                "aqi_avg": pollution.get("aqius"),
                "aqi_min": None,
                "aqi_max": None,
                "timestamp": datetime.utcnow().isoformat()
            }

        except requests.RequestException as e:
            print(f"[AQI] Attempt {attempt} failed: {e}. Retrying in {wait_time}s...")
            sleep(wait_time)
            wait_time = min(wait_time * backoff_factor, max_wait)

    return {"aqi_avg": None, "aqi_min": None, "aqi_max": None, "timestamp": datetime.utcnow().isoformat()}

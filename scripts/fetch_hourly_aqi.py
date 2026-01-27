import requests
from datetime import datetime
import os
from dotenv import load_dotenv
from time import sleep

load_dotenv()

OPEN_METEO_URL = "https://air-quality-api.open-meteo.com/v1/air-quality"


def fetch_weather_hourly(latitude: float, longitude: float,
                         start_date: str = None, end_date: str = None,
                         variables: list = None, retries: int = 3):
    """
    Fetch hourly weather + air quality variables from Open-Meteo
    Args:
        latitude, longitude: coordinates
        start_date, end_date: yyyy-mm-dd
        variables: list of strings like ['pm10', 'pm2_5', 'co', 'no2']
    Returns:
        List of hourly dicts: {"timestamp": ..., "pm10": ..., "pm2_5": ..., ...}
    """
    if variables is None:
        variables = ["pm10", "pm2_5", "co", "no2", "so2", "o3", "temperature_2m", "humidity_2m", "windspeed_10m"]

    params = {
        "latitude": latitude,
        "longitude": longitude,
        "hourly": ",".join(variables),
        "timezone": "auto"
    }
    if start_date:
        params["start_date"] = start_date
    if end_date:
        params["end_date"] = end_date

    for attempt in range(1, retries + 1):
        try:
            response = requests.get(OPEN_METEO_URL, params=params, timeout=15)
            response.raise_for_status()
            data = response.json().get("hourly", {})

            # Convert to list of dicts
            hourly_records = []
            time_list = data.get("time", [])
            for i, timestamp in enumerate(time_list):
                record = {"timestamp": timestamp}
                for var in variables:
                    record[var] = data.get(var, [None]*len(time_list))[i]
                hourly_records.append(record)
            return hourly_records

        except requests.RequestException as e:
            print(f"[Weather] Attempt {attempt} failed: {e}. Retrying...")
            sleep(2 ** attempt)

    print("[Weather] Permanent failure for Open-Meteo request")
    return []

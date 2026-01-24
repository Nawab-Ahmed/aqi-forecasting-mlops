import requests
from time import sleep
from typing import List, Dict

def fetch_weather_batch(lat: float, lon: float, start_date: str, end_date: str, retries: int = 3) -> List[Dict]:
    """
    Fetch daily weather data from Open-Meteo for a batch of dates.
    
    Parameters:
        lat, lon: Coordinates of the city
        start_date, end_date: YYYY-MM-DD
        retries: Number of retries on failure
    Returns:
        List of daily weather dictionaries
    """
    url = (
        f"https://archive-api.open-meteo.com/v1/archive?"
        f"latitude={lat}&longitude={lon}"
        f"&start_date={start_date}&end_date={end_date}"
        f"&daily=temperature_2m_mean,temperature_2m_min,temperature_2m_max,"
        f"precipitation_sum,wind_speed_10m_max,relative_humidity_2m_mean"
        f"&timezone=UTC"
    )

    for attempt in range(retries):
        try:
            response = requests.get(url, timeout=15)
            response.raise_for_status()
            data = response.json()
            results = []
            for i, date in enumerate(data["daily"]["time"]):
                results.append({
                    "date": date,
                    "temperature_avg": data["daily"]["temperature_2m_mean"][i],
                    "temperature_min": data["daily"]["temperature_2m_min"][i],
                    "temperature_max": data["daily"]["temperature_2m_max"][i],
                    "humidity_avg": data["daily"]["relative_humidity_2m_mean"][i],
                    "wind_speed_avg": data["daily"]["wind_speed_10m_max"][i],
                    "precipitation_sum": data["daily"]["precipitation_sum"][i],
                })
            return results
        except Exception as e:
            print(f"[Weather] Attempt {attempt+1} failed: {e}")
            sleep(2 ** attempt)  # Exponential backoff
    print(f"[Weather] Failed to fetch data for {start_date} → {end_date}")
    return []

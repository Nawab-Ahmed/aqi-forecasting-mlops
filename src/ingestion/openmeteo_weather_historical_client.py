from typing import List, Dict, Any
from datetime import datetime, timezone
import requests

from src.utils.logger import get_logger

logger = get_logger(__name__)


class OpenMeteoWeatherHistoricalClient:
    """
    Fetches hourly historical weather data from Open-Meteo
    """

    BASE_URL = "https://archive-api.open-meteo.com/v1/archive"

    def __init__(self, city: str, latitude: float, longitude: float):
        self.city = city
        self.latitude = latitude
        self.longitude = longitude

    def fetch(
        self,
        start_date: datetime,
        end_date: datetime
    ) -> List[Dict[str, Any]]:
        """
        Fetch hourly historical weather data.
        """

        logger.info(
            "Fetching historical weather from Open-Meteo",
            extra={"city": self.city}
        )

        params = {
            "latitude": self.latitude,
            "longitude": self.longitude,
            "start_date": start_date.date().isoformat(),
            "end_date": end_date.date().isoformat(),
            "hourly": ",".join([
                "temperature_2m",
                "relative_humidity_2m",
                "wind_speed_10m",
                "wind_direction_10m",
                "surface_pressure",
                "precipitation"
            ]),
            "timezone": "UTC"
        }

        response = requests.get(self.BASE_URL, params=params, timeout=20)
        response.raise_for_status()
        payload = response.json()

        hourly = payload.get("hourly", {})
        timestamps = hourly.get("time", [])

        records = []

        for idx, ts in enumerate(timestamps):
            record = {
                "city": self.city,
                "event_timestamp": datetime.fromisoformat(ts).replace(tzinfo=timezone.utc),
                "weather": {
                    "temperature": hourly["temperature_2m"][idx],
                    "humidity": hourly["relative_humidity_2m"][idx],
                    "wind_speed": hourly["wind_speed_10m"][idx],
                    "wind_direction": hourly["wind_direction_10m"][idx],
                    "pressure": hourly["surface_pressure"][idx],
                    "precipitation": hourly["precipitation"][idx],
                },
                "source": "open-meteo-weather",
                "ingested_at": datetime.now(timezone.utc)
            }
            records.append(record)

        logger.info(
            "Historical weather normalized",
            extra={"records": len(records)}
        )

        return records

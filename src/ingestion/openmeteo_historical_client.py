from typing import List, Dict, Any
from datetime import datetime, timezone
import requests

from src.utils.logger import get_logger

logger = get_logger(__name__)


class OpenMeteoHistoricalAQIClient:
    """
    Fetches hourly historical air quality data from Open-Meteo.
    """

    BASE_URL = "https://air-quality-api.open-meteo.com/v1/air-quality"

    def __init__(
        self,
        latitude: float,
        longitude: float,
        start_date: str,
        end_date: str
    ):
        self.latitude = latitude
        self.longitude = longitude
        self.start_date = start_date
        self.end_date = end_date

    def fetch(self) -> List[Dict[str, Any]]:
        logger.info(
            "Fetching historical AQI from Open-Meteo",
            extra={
                "lat": self.latitude,
                "lon": self.longitude,
                "start": self.start_date,
                "end": self.end_date
            }
        )

        params = {
            "latitude": self.latitude,
            "longitude": self.longitude,
            "start_date": self.start_date,
            "end_date": self.end_date,
            "hourly": [
                "pm2_5",
                "pm10",
                "nitrogen_dioxide",
                "sulphur_dioxide",
                "ozone",
                "carbon_monoxide"
            ],
            "timezone": "UTC"
        }

        response = requests.get(self.BASE_URL, params=params, timeout=20)
        response.raise_for_status()
        payload = response.json()

        return self._normalize(payload)

    def _normalize(self, payload: Dict[str, Any]) -> List[Dict[str, Any]]:
        hourly = payload.get("hourly", {})
        timestamps = hourly.get("time", [])

        records = []

        for i, ts in enumerate(timestamps):
            record = {
                "city": "karachi",
                "event_timestamp": datetime.fromisoformat(ts).replace(
                    tzinfo=timezone.utc
                ),
                "pollutants": {
                    "pm25": hourly.get("pm2_5", [None])[i],
                    "pm10": hourly.get("pm10", [None])[i],
                    "no2": hourly.get("nitrogen_dioxide", [None])[i],
                    "so2": hourly.get("sulphur_dioxide", [None])[i],
                    "o3": hourly.get("ozone", [None])[i],
                    "co": hourly.get("carbon_monoxide", [None])[i],
                },
                "source": "open-meteo",
                "ingested_at": datetime.now(timezone.utc)
            }

            records.append(record)

        logger.info(
            "Historical AQI normalized",
            extra={"records": len(records)}
        )

        return records

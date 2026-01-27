from typing import Dict, Any
from datetime import datetime, timezone
import requests

from src.utils.logger import get_logger

logger = get_logger(__name__)


class AQICNLiveClient:
    """
    Fetches current AQI data for a city from AQICN
    """

    BASE_URL = "https://api.waqi.info/feed"

    def __init__(self, token: str, city: str):
        self.token = token
        self.city = city.lower()

    def fetch(self) -> Dict[str, Any]:
        """
        Fetch current AQI data and normalize schema.
        """
        url = f"{self.BASE_URL}/{self.city}/"
        params = {"token": self.token}

        logger.info("Fetching live AQI data", extra={"city": self.city})

        try:
            response = requests.get(url, params=params, timeout=10)
            response.raise_for_status()
            payload = response.json()

        except requests.RequestException as e:
            logger.error("AQICN request failed", exc_info=True)
            raise RuntimeError("AQICN API request failed") from e

        if payload.get("status") != "ok":
            logger.error("AQICN returned non-ok status", extra={"payload": payload})
            raise ValueError("Invalid AQICN response")

        return self._normalize(payload["data"])

    def _normalize(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Normalize AQICN payload into canonical schema.
        """
        time_info = data["time"]["iso"]
        event_time = (
            datetime.fromisoformat(time_info.replace("Z", "+00:00"))
            .astimezone(timezone.utc)
        )

        pollutants = {
            key: value.get("v")
            for key, value in data.get("iaqi", {}).items()
        }

        normalized = {
            "city": self.city,
            "aqi": data.get("aqi"),
            "dominant_pollutant": data.get("dominentpol"),
            "pollutants": pollutants,
            "event_timestamp": event_time,
            "ingested_at": datetime.now(timezone.utc),
            "source": "aqicn"
        }

        logger.info(
            "Live AQI normalized",
            extra={
                "city": self.city,
                "aqi": normalized["aqi"],
                "event_timestamp": event_time.isoformat()
            }
        )

        return normalized

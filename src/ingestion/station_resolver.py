from typing import Dict, Any
import requests

from src.utils.logger import get_logger

logger = get_logger(__name__)


class AQICNStationResolver:
    """
    Resolves a city name to a primary AQICN station ID (idx).
    """

    BASE_URL = "https://api.waqi.info/feed"

    def __init__(self, token: str, city: str):
        self.token = token
        self.city = city.lower()

    def resolve(self) -> Dict[str, Any]:
        """
        Fetch live city feed and extract station metadata.
        """
        url = f"{self.BASE_URL}/{self.city}/"
        params = {"token": self.token}

        logger.info("Resolving AQICN station", extra={"city": self.city})

        response = requests.get(url, params=params, timeout=10)
        response.raise_for_status()
        payload = response.json()

        if payload.get("status") != "ok":
            raise RuntimeError("Failed to resolve AQICN station")

        data = payload["data"]

        station_info = {
            "city": self.city,
            "station_id": data["idx"],
            "station_name": data["city"]["name"],
            "geo": data["city"]["geo"],
            "source": "aqicn"
        }

        logger.info(
            "Station resolved",
            extra={"station_id": station_info["station_id"]}
        )

        return station_info

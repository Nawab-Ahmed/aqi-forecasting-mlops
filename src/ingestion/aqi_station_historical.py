from typing import List, Dict, Any
import requests
from datetime import datetime, timezone

from src.utils.logger import get_logger

logger = get_logger(__name__)


class AQICNStationHistoricalClient:
    """
    Fetches hourly historical AQI data for a specific AQICN station (idx).
    """

    BASE_URL = "https://api.waqi.info/api/feed/@{station_id}/history/"

    def __init__(self, token: str, station_id: int):
        self.token = token
        self.station_id = station_id

    def fetch(self) -> List[Dict[str, Any]]:
        """
        Fetch available hourly historical AQI data for the station.
        """
        url = self.BASE_URL.format(station_id=self.station_id)
        params = {"token": self.token}

        logger.info(
            "Fetching station historical AQI",
            extra={"station_id": self.station_id}
        )

        response = requests.get(url, params=params, timeout=15)
        response.raise_for_status()
        payload = response.json()

        if payload.get("status") != "ok":
            raise RuntimeError("Failed to fetch station historical AQI")

        history = payload["data"]

        records = []
        ingested_at = datetime.now(timezone.utc)

        for item in history:
            record = {
                "station_id": self.station_id,
                "aqi": item.get("aqi"),
                "dominant_pollutant": item.get("dominentpol"),
                "pollutants": item.get("iaqi", {}),
                "event_timestamp": datetime.fromtimestamp(
                    item["time"]["v"],
                    tz=timezone.utc
                ),
                "ingested_at": ingested_at,
                "source": "aqicn"
            }
            records.append(record)

        logger.info(
            "Historical AQI fetched",
            extra={"records": len(records)}
        )

        return records

# src/ingestion/aqi_client_historical.py

from typing import List, Dict, Any
from datetime import datetime, timezone, timedelta
import requests
import time
import logging

from src.utils.logger import get_logger

logger = get_logger(__name__)

class AQICNHistoricalClient:
    """
    Fetches hourly historical AQI data for a city from AQICN.
    """

    BASE_URL = "https://api.waqi.info/feed"

    def __init__(self, token: str, city: str):
        self.token = token
        self.city = city.lower()

    def fetch_range(
        self, 
        start_date: datetime, 
        end_date: datetime, 
        max_retries: int = 3
    ) -> List[Dict[str, Any]]:
        """
        Fetch hourly historical AQI from start_date to end_date
        """

        logger.info(
            "Starting historical AQI fetch",
            extra={"city": self.city, "start_date": start_date, "end_date": end_date}
        )

        results = []
        current_date = start_date

        # AQICN may limit data per request; fetch day by day to be safe
        while current_date <= end_date:
            success = False
            for attempt in range(1, max_retries + 1):
                try:
                    url = f"{self.BASE_URL}/{self.city}/history/"
                    params = {
                        "token": self.token,
                        "date": current_date.strftime("%Y-%m-%d")
                    }

                    logger.info(f"Fetching historical AQI", extra={"date": current_date.strftime("%Y-%m-%d")})

                    response = requests.get(url, params=params, timeout=15)
                    response.raise_for_status()
                    payload = response.json()

                    if payload.get("status") != "ok":
                        logger.warning("Non-ok response from AQICN", extra={"payload": payload})
                        raise ValueError("Invalid AQICN response")

                    day_records = self._normalize(payload.get("data", {}))
                    results.extend(day_records)

                    success = True
                    break

                except Exception as e:
                    logger.error(f"Failed to fetch data for {current_date.strftime('%Y-%m-%d')} on attempt {attempt}", exc_info=True)
                    time.sleep(2)  # backoff before retry

            if not success:
                logger.error(f"Skipping {current_date.strftime('%Y-%m-%d')} after {max_retries} attempts")

            current_date += timedelta(days=1)

        logger.info("Finished historical AQI fetch", extra={"total_records": len(results)})
        return results

    def _normalize(self, data: Dict[str, Any]) -> List[Dict[str, Any]]:
        """
        Normalize AQICN historical payload into canonical schema
        """
        normalized_records = []

        # Data usually contains 'forecast', 'history', or 'data' fields
        hourly_data = data.get("history", {}).get("hourly", []) or data.get("hourly", [])
        for entry in hourly_data:
            time_str = entry["time"]["iso"]  # ISO formatted timestamp
            event_time = datetime.fromisoformat(time_str.replace("Z", "+00:00")).astimezone(timezone.utc)

            pollutants = {k: v.get("v") for k, v in entry.get("iaqi", {}).items()}

            normalized_records.append({
                "city": self.city,
                "aqi": entry.get("aqi"),
                "dominant_pollutant": entry.get("dominentpol"),
                "pollutants": pollutants,
                "event_timestamp": event_time,
                "ingested_at": datetime.now(timezone.utc),
                "source": "aqicn"
            })

        return normalized_records

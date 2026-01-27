from datetime import datetime


def validate_hourly_record(record: dict):
    """
    Hard validation for raw hourly AQI data.
    Fails fast if schema is violated.
    """

    if not isinstance(record.get("timestamp"), datetime):
        raise TypeError("timestamp must be datetime")

    if not isinstance(record.get("aqi"), dict):
        raise TypeError("aqi must be a dict")

    if not isinstance(record.get("weather"), dict):
        raise TypeError("weather must be a dict")

    if "us_aqi" not in record["aqi"]:
        raise ValueError("aqi.us_aqi missing")

    if record["aqi"]["us_aqi"] is not None:
        if record["aqi"]["us_aqi"] < 0:
            raise ValueError("Invalid AQI value")

    return True

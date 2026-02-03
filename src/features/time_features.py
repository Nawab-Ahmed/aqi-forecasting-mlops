# src/features/time_features.py

import pandas as pd

def add_time_features(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()

    ts = pd.to_datetime(df["event_timestamp"], utc=True)

    df["hour"] = ts.dt.hour
    df["weekday"] = ts.dt.weekday
    df["day"] = ts.dt.day
    df["month"] = ts.dt.month
    df["is_weekend"] = (df["weekday"] >= 5).astype(int)

    return df

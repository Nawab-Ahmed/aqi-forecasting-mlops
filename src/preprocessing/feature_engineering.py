import pandas as pd
import numpy as np

def cap_outliers_iqr(series: pd.Series, factor: float = 1.5) -> pd.Series:
    q1 = series.quantile(0.25)
    q3 = series.quantile(0.75)
    iqr = q3 - q1
    lower, upper = q1 - factor * iqr, q3 + factor * iqr
    return series.clip(lower, upper)

def create_time_features(df: pd.DataFrame) -> pd.DataFrame:
    # Basic components
    hour = df["event_timestamp"].dt.hour
    month = df["event_timestamp"].dt.month
    
    # Cyclical Encoding: Captures the 23:00 to 00:00 transition
    df["hour_sin"] = np.sin(2 * np.pi * hour / 24)
    df["hour_cos"] = np.cos(2 * np.pi * hour / 24)
    df["month_sin"] = np.sin(2 * np.pi * month / 12)
    df["month_cos"] = np.cos(2 * np.pi * month / 12)
    
    df["is_weekend"] = df["event_timestamp"].dt.weekday.isin([5, 6]).astype(int)
    return df

def create_pollutant_change(df: pd.DataFrame, pollutants: list) -> pd.DataFrame:
    for f in pollutants:
        df[f"{f}_change"] = df[f].diff().fillna(0)
    return df

def create_lags(df: pd.DataFrame, features: list, lag_hours: list) -> pd.DataFrame:
    for f in features:
        for lag in lag_hours:
            df[f"{f}_lag_{lag}h"] = df[f].shift(lag)
    return df

def create_rolling_stats(df: pd.DataFrame, features: list, windows: list) -> pd.DataFrame:
    for f in features:
        for w in windows:
            rolling = df[f].rolling(window=w, min_periods=1)
            df[f"{f}_rolling_avg_{w}h"] = rolling.mean()
            df[f"{f}_rolling_std_{w}h"] = rolling.std().fillna(0)
    return df

def create_targets(df: pd.DataFrame, target_feature: str, horizons: list) -> pd.DataFrame:
    for h in horizons:
        df[f"{target_feature}_t_plus_{h}h"] = df[target_feature].shift(-h)
    return df
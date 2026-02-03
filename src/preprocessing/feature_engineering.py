import pandas as pd
from sklearn.preprocessing import RobustScaler, StandardScaler

def cap_outliers_iqr(series: pd.Series, factor: float = 1.5) -> pd.Series:
    q1 = series.quantile(0.25)
    q3 = series.quantile(0.75)
    iqr = q3 - q1
    lower, upper = q1 - factor * iqr, q3 + factor * iqr
    return series.clip(lower, upper)

def create_time_features(df: pd.DataFrame) -> pd.DataFrame:
    df["hour"] = df["event_timestamp"].dt.hour
    df["day"] = df["event_timestamp"].dt.day
    df["weekday"] = df["event_timestamp"].dt.weekday
    df["month"] = df["event_timestamp"].dt.month
    df["is_weekend"] = df["weekday"].isin([5,6]).astype(int)
    return df

def create_pollutant_change(df: pd.DataFrame, pollutants: list) -> pd.DataFrame:
    for f in pollutants:
        df[f"{f}_change"] = df[f].diff().fillna(0)
    return df

def create_lags(df: pd.DataFrame, features: list, lag_hours: list) -> pd.DataFrame:
    for f in features:
        for lag in lag_hours:
            df[f"{f}_lag_{lag}"] = df[f].shift(lag)
    return df

def create_rolling_stats(df: pd.DataFrame, features: list, windows: list) -> pd.DataFrame:
    for f in features:
        for w in windows:
            df[f"{f}_rolling_{w}h"] = df[f].rolling(window=w, min_periods=1).mean()
            df[f"{f}_std_{w}h"] = df[f].rolling(window=w, min_periods=1).std()
    return df

def create_targets(df: pd.DataFrame, target_feature: str, horizons: list) -> pd.DataFrame:
    for h in horizons:
        df[f"{target_feature}_t_plus_{h}h"] = df[target_feature].shift(-h)
    return df

def scale_features(df: pd.DataFrame, pollutant_features: list, weather_features: list) -> pd.DataFrame:
    df_scaled = df.copy()
    pollutant_scaler = RobustScaler()
    weather_scaler = StandardScaler()
    
    df_scaled[pollutant_features] = pollutant_scaler.fit_transform(df_scaled[pollutant_features])
    df_scaled[weather_features] = weather_scaler.fit_transform(df_scaled[weather_features])
    
    return df_scaled

from sklearn.preprocessing import RobustScaler, StandardScaler
import pandas as pd

def scale_features(
    df: pd.DataFrame,
    pollutant_features: list,
    weather_features: list
):
    df = df.copy()

    # Only scale columns that actually exist
    pollutant_features = [c for c in pollutant_features if c in df.columns]
    weather_features = [c for c in weather_features if c in df.columns]

    pollutant_scaler = RobustScaler()
    weather_scaler = StandardScaler()

    if pollutant_features:
        df[pollutant_features] = pollutant_scaler.fit_transform(
            df[pollutant_features]
        )

    if weather_features:
        df[weather_features] = weather_scaler.fit_transform(
            df[weather_features]
        )

    return df, pollutant_scaler, weather_scaler

from sklearn.preprocessing import RobustScaler, StandardScaler
import pandas as pd

def scale_features(df: pd.DataFrame, pollutant_features: list, weather_features: list) -> pd.DataFrame:
    df_scaled = df.copy()
    pollutant_scaler = RobustScaler()
    weather_scaler = StandardScaler()
    
    df_scaled[pollutant_features] = pollutant_scaler.fit_transform(df_scaled[pollutant_features])
    df_scaled[weather_features] = weather_scaler.fit_transform(df_scaled[weather_features])
    
    return df_scaled

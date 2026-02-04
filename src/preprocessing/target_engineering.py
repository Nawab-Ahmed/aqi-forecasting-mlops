# src/preprocessing/target_engineering.py

import pandas as pd

def create_future_targets(
    df: pd.DataFrame,
    target_col: str,
    horizons: list[int]
) -> pd.DataFrame:
    """
    Create future prediction targets from RAW target values.
    """
    df = df.copy()

    for h in horizons:
        df[f"{target_col}_t_plus_{h}h"] = df[target_col].shift(-h)

    return df

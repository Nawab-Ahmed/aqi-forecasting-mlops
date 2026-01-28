import pandas as pd
import logging

logger = logging.getLogger(__name__)

def handle_missing(df: pd.DataFrame, method="ffill") -> pd.DataFrame:
    """
    Handle missing values in a DataFrame.

    Args:
        df: Input DataFrame
        method: Method to fill missing values. Options: 'ffill', 'bfill', 'mean', 'interpolate'

    Returns:
        DataFrame with missing values handled
    """
    missing_before = df.isna().sum()
    logger.info(f"Missing values before handling:\n{missing_before[missing_before > 0]}")

    if method == "ffill":
        df = df.fillna(method="ffill").fillna(method="bfill")
    elif method == "bfill":
        df = df.fillna(method="bfill").fillna(method="ffill")
    elif method == "mean":
        df = df.fillna(df.mean())
    elif method == "interpolate":
        df = df.interpolate(method='time').fillna(method="bfill").fillna(method="ffill")
    else:
        raise ValueError(f"Unknown method: {method}")

    missing_after = df.isna().sum()
    logger.info(f"Missing values after handling:\n{missing_after[missing_after > 0]}")

    return df

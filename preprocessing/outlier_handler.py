import pandas as pd
import numpy as np
from src.logger import get_logger

logger = get_logger(__name__)

def detect_outliers_iqr(df: pd.DataFrame, column: str, multiplier: float = 1.5):
    """
    Detect outliers in a numeric column using the IQR method.

    Args:
        df: Input DataFrame
        column: Column to check for outliers
        multiplier: IQR multiplier for upper/lower bound

    Returns:
        df with a boolean column "{column}_outlier" marking True if outlier
    """
    Q1 = df[column].quantile(0.25)
    Q3 = df[column].quantile(0.75)
    IQR = Q3 - Q1
    lower_bound = Q1 - multiplier * IQR
    upper_bound = Q3 + multiplier * IQR

    outlier_col = f"{column}_outlier"
    df[outlier_col] = ~df[column].between(lower_bound, upper_bound)
    logger.info(f"Detected {df[outlier_col].sum()} outliers in '{column}'")
    return df

def handle_outliers(df: pd.DataFrame, column: str, method: str = "clip", multiplier: float = 1.5):
    """
    Handle outliers using one of the industry-standard methods:
        - 'clip': cap values at lower/upper bound
        - 'remove': drop outlier rows
        - 'flag': just add boolean flag (default)

    Args:
        df: Input DataFrame
        column: Column to handle
        method: One of ['clip', 'remove', 'flag']
        multiplier: IQR multiplier for defining outliers

    Returns:
        DataFrame after handling outliers
    """
    df = detect_outliers_iqr(df, column, multiplier)

    Q1 = df[column].quantile(0.25)
    Q3 = df[column].quantile(0.75)
    IQR = Q3 - Q1
    lower_bound = Q1 - multiplier * IQR
    upper_bound = Q3 + multiplier * IQR

    if method == "clip":
        df[column] = df[column].clip(lower=lower_bound, upper=upper_bound)
        logger.info(f"Clipped '{column}' to IQR bounds [{lower_bound}, {upper_bound}]")
    elif method == "remove":
        original_len = len(df)
        df = df[df[column].between(lower_bound, upper_bound)]
        logger.info(f"Removed {original_len - len(df)} outlier rows from '{column}'")
    elif method == "flag":
        logger.info(f"Flag column '{column}_outlier' added for inspection")
    else:
        raise ValueError("method must be one of ['clip', 'remove', 'flag']")

    return df

import pandas as pd
import numpy as np
from .trend import calculate_sma

def calculate_bollinger_bands(data: pd.Series, window: int = 20, std_dev: int = 2) -> pd.DataFrame:
    """
    Calculates Bollinger Bands.
    
    Returns a DataFrame with 'bb_upper', 'bb_middle' (SMA), and 'bb_lower' columns.
    """
    if data.empty or len(data) < window:
        return pd.DataFrame(index=data.index, columns=['bb_upper', 'bb_middle', 'bb_lower'], dtype=float)
        
    sma = calculate_sma(data, window=window)
    rolling_std = data.rolling(window=window, min_periods=window).std()
    
    upper_band = sma + (rolling_std * std_dev)
    lower_band = sma - (rolling_std * std_dev)
    
    return pd.DataFrame({
        'bb_upper': upper_band,
        'bb_middle': sma,
        'bb_lower': lower_band
    }, index=data.index)
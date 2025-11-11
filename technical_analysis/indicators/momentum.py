import pandas as pd
import numpy as np

def calculate_rsi(data: pd.Series, window: int = 14) -> pd.Series:
    """Calculates the Relative Strength Index (RSI)"""
    if data.empty or len(data) < window:
        return pd.Series(index=data.index, dtype=float)

    delta = data.diff(1)
    
    gain = delta.where(delta > 0, 0)
    loss = -delta.where(delta < 0, 0)

    # Use ewm for a smoother, more standard RSI calculation
    avg_gain = gain.ewm(com=window - 1, min_periods=window, adjust=False).mean()
    avg_loss = loss.ewm(com=window - 1, min_periods=window, adjust=False).mean()

    rs = avg_gain / avg_loss
    rsi = 100.0 - (100.0 / (1.0 + rs))
    
    # Fill initial NaNs (from the window) with 50 (neutral) or np.nan
    # rsi[:window] = np.nan 
    return rsi
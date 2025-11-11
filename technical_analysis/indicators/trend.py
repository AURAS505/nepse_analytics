import pandas as pd
import numpy as np

def calculate_sma(data: pd.Series, window: int) -> pd.Series:
    """Calculates the Simple Moving Average (SMA)"""
    if data.empty or len(data) < window:
        return pd.Series(index=data.index, dtype=float)
    return data.rolling(window=window, min_periods=window).mean()

def calculate_ema(data: pd.Series, window: int) -> pd.Series:
    """Calculates the Exponential Moving Average (EMA)"""
    if data.empty or len(data) < window:
        return pd.Series(index=data.index, dtype=float)
    return data.ewm(span=window, adjust=False, min_periods=window).mean()

def calculate_macd(data: pd.Series, short_window: int = 12, long_window: int = 26, signal_window: int = 9) -> pd.DataFrame:
    """
    Calculates the Moving Average Convergence Divergence (MACD).
    
    Returns a DataFrame with 'macd', 'signal', and 'histogram' columns.
    """
    if data.empty or len(data) < long_window:
        return pd.DataFrame(index=data.index, columns=['macd', 'signal', 'histogram'], dtype=float)
        
    ema_short = calculate_ema(data, window=short_window)
    ema_long = calculate_ema(data, window=long_window)
    
    macd_line = ema_short - ema_long
    signal_line = calculate_ema(macd_line, window=signal_window)
    histogram = macd_line - signal_line
    
    return pd.DataFrame({
        'macd': macd_line,
        'signal': signal_line,
        'histogram': histogram
    }, index=data.index)
import pandas as pd
import numpy as np

def calculate_obv(close: pd.Series, volume: pd.Series) -> pd.Series:
    """
    Calculates the On-Balance Volume (OBV).
    Requires both close price and volume data.
    """
    if close.empty or volume.empty or len(close) != len(volume):
        return pd.Series(index=close.index, dtype=float)

    obv = pd.Series(index=close.index, dtype=float)
    obv.iloc[0] = volume.iloc[0]
    
    price_diff = close.diff(1)
    
    # Vectorized approach
    direction = np.sign(price_diff.fillna(0)) # 0 for no change, 1 for up, -1 for down
    volume_change = volume * direction
    
    obv = volume_change.cumsum()
    
    # Set the first value
    obv.iloc[0] = volume.iloc[0] 
    
    return obv
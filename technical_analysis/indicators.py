# technical_analysis/indicators.py

import pandas as pd
import numpy as np
from nepse_data.models import StockPrices  # To get volume
from adjustments_stock_price.models import StockPricesAdj # To get adjusted prices

def get_historical_data(symbol: str, days: int = 365) -> pd.DataFrame:
    """
    Fetches adjusted price and volume data for a given symbol.
    """
    # 1. Fetch Adjusted Prices
    # We fetch more data than requested (days + 100) to ensure indicators
    # (like 35-day SMA) have enough "warm-up" data.
    price_data = StockPricesAdj.objects.filter(symbol=symbol)\
                                     .order_by('-business_date')[:days + 100]
    
    if not price_data.exists():
        return pd.DataFrame() # Return empty if no data

    price_df = pd.DataFrame.from_records(price_data.values(
        'business_date', 'open_price_adj', 'high_price_adj', 'low_price_adj', 'close_price_adj'
    ))
    price_df.rename(columns={
        'business_date': 'date',
        'open_price_adj': 'open',
        'high_price_adj': 'high',
        'low_price_adj': 'low',
        'close_price_adj': 'close',
    }, inplace=True)
    price_df['date'] = pd.to_datetime(price_df['date'])
    price_df.set_index('date', inplace=True)

    # 2. Fetch Volume Data
    volume_data = StockPrices.objects.filter(symbol=symbol)\
                                     .order_by('-business_date')[:days + 100]
    
    volume_df = pd.DataFrame.from_records(volume_data.values(
        'business_date', 'total_traded_quantity'
    ))
    volume_df.rename(columns={
        'business_date': 'date',
        'total_traded_quantity': 'volume',
    }, inplace=True)
    volume_df['date'] = pd.to_datetime(volume_df['date'])
    volume_df.set_index('date', inplace=True)
    
    # 3. Combine price and volume
    df = price_df.join(volume_df['volume'], how='inner')
    df.sort_index(ascending=True, inplace=True) # Sort from oldest to newest
    df[['open', 'high', 'low', 'close', 'volume']] = df[['open', 'high', 'low', 'close', 'volume']].apply(pd.to_numeric)
    
    return df


# ---------------------------------
#  TREND INDICATORS
# ---------------------------------

def get_sma_signal(df: pd.DataFrame, short_window: int, long_window: int) -> str:
    """
    Generates a signal based on a two-SMA crossover.
    """
    if df.empty or len(df) < long_window:
        return "Wait"
        
    df[f'sma_short'] = df['close'].rolling(window=short_window).mean()
    df[f'sma_long'] = df['close'].rolling(window=long_window).mean()
    
    # Get the last two rows to check for a crossover
    last_rows = df.iloc[-2:]
    
    # Buy Signal: Short crosses above Long
    if last_rows.iloc[0]['sma_short'] <= last_rows.iloc[0]['sma_long'] and \
       last_rows.iloc[1]['sma_short'] > last_rows.iloc[1]['sma_long']:
        return "Buy"
        
    # Sell Signal: Short crosses below Long
    if last_rows.iloc[0]['sma_short'] >= last_rows.iloc[0]['sma_long'] and \
       last_rows.iloc[1]['sma_short'] < last_rows.iloc[1]['sma_long']:
        return "Sell"
        
    return "Wait"

def get_macd_signal(df: pd.DataFrame) -> str:
    """
    Generates a signal based on MACD line and Signal line crossover.
    """
    if df.empty or len(df) < 35: # MACD uses 12, 26, 9 periods
        return "Wait"

    exp12 = df['close'].ewm(span=12, adjust=False).mean()
    exp26 = df['close'].ewm(span=26, adjust=False).mean()
    df['macd'] = exp12 - exp26
    df['signal_line'] = df['macd'].ewm(span=9, adjust=False).mean()
    
    last_rows = df.iloc[-2:]
    
    # Buy Signal: MACD crosses above Signal
    if last_rows.iloc[0]['macd'] <= last_rows.iloc[0]['signal_line'] and \
       last_rows.iloc[1]['macd'] > last_rows.iloc[1]['signal_line']:
        return "Buy"
        
    # Sell Signal: MACD crosses below Signal
    if last_rows.iloc[0]['macd'] >= last_rows.iloc[0]['signal_line'] and \
       last_rows.iloc[1]['macd'] < last_rows.iloc[1]['signal_line']:
        return "Sell"
        
    return "Wait"

def get_cci_signal(df: pd.DataFrame, window: int = 20) -> str:
    """
    Generates a signal based on CCI overbought/oversold levels.
    """
    if df.empty or len(df) < window:
        return "Wait"
        
    tp = (df['high'] + df['low'] + df['close']) / 3
    sma_tp = tp.rolling(window=window).mean()
    mean_dev = tp.rolling(window=window).apply(lambda x: np.mean(np.abs(x - x.mean())))
    
    df['cci'] = (tp - sma_tp) / (0.015 * mean_dev)
    
    last_rows = df.iloc[-2:]

    # Buy Signal: Crosses back above -100
    if last_rows.iloc[0]['cci'] <= -100 and last_rows.iloc[1]['cci'] > -100:
        return "Buy"
    
    # Sell Signal: Crosses back below +100
    if last_rows.iloc[0]['cci'] >= 100 and last_rows.iloc[1]['cci'] < 100:
        return "Sell"
        
    return "Wait"

def get_adx_signal(df: pd.DataFrame, window: int = 14) -> str:
    """
    Generates a signal based on ADX trend strength and DI crossover.
    """
    if df.empty or len(df) < window * 2:
        return "Wait"

    df['tr'] = np.maximum(df['high'] - df['low'], 
                         np.maximum(np.abs(df['high'] - df['close'].shift(1)), 
                                    np.abs(df['low'] - df['close'].shift(1))))
    
    df['plus_dm'] = np.where((df['high'] - df['high'].shift(1)) > (df['low'].shift(1) - df['low']), 
                            np.maximum(df['high'] - df['high'].shift(1), 0), 0)
    
    df['minus_dm'] = np.where((df['low'].shift(1) - df['low']) > (df['high'] - df['high'].shift(1)), 
                             np.maximum(df['low'].shift(1) - df['low'], 0), 0)

    df['atr'] = df['tr'].ewm(alpha=1/window, adjust=False).mean()
    df['plus_di'] = 100 * (df['plus_dm'].ewm(alpha=1/window, adjust=False).mean() / df['atr'])
    df['minus_di'] = 100 * (df['minus_dm'].ewm(alpha=1/window, adjust=False).mean() / df['atr'])
    
    df['dx'] = 100 * (np.abs(df['plus_di'] - df['minus_di']) / (df['plus_di'] + df['minus_di']))
    df['adx'] = df['dx'].ewm(alpha=1/window, adjust=False).mean()

    last_row = df.iloc[-1]
    
    # If ADX is weak, no trend, so wait.
    if last_row['adx'] < 25:
        return "Wait"
        
    # Strong trend, check direction
    if last_row['plus_di'] > last_row['minus_di']:
        return "Buy" # Strong Uptrend
    else:
        return "Sell" # Strong Downtrend

# ---------------------------------
#  MOMENTUM INDICATOR
# ---------------------------------

def get_rsi_signal(df: pd.DataFrame, window: int = 14) -> str:
    """
    Generates a signal based on RSI overbought/oversold levels.
    """
    if df.empty or len(df) < window:
        return "Wait"
        
    delta = df['close'].diff(1)
    gain = delta.where(delta > 0, 0)
    loss = -delta.where(delta < 0, 0)

    avg_gain = gain.rolling(window=window, min_periods=1).mean()
    avg_loss = loss.rolling(window=window, min_periods=1).mean()

    rs = avg_gain / avg_loss
    df['rsi'] = 100 - (100 / (1 + rs))
    
    last_rows = df.iloc[-2:]
    
    # Buy Signal: Crosses back above 30
    if last_rows.iloc[0]['rsi'] <= 30 and last_rows.iloc[1]['rsi'] > 30:
        return "Buy"
        
    # Sell Signal: Crosses back below 70
    if last_rows.iloc[0]['rsi'] >= 70 and last_rows.iloc[1]['rsi'] < 70:
        return "Sell"
        
    return "Wait"

# ---------------------------------
#  VOLATILITY INDICATOR
# ---------------------------------

def get_bb_signal(df: pd.DataFrame, window: int = 20, std_dev: int = 2) -> str:
    """
    Generates a signal based on price touching Bollinger Bands.
    """
    if df.empty or len(df) < window:
        return "Wait"
        
    df['sma'] = df['close'].rolling(window=window).mean()
    df['std'] = df['close'].rolling(window=window).std()
    df['upper_band'] = df['sma'] + (df['std'] * std_dev)
    df['lower_band'] = df['sma'] - (df['std'] * std_dev)
    
    last_row = df.iloc[-1]
    
    # Buy Signal: Touches or crosses below lower band
    if last_row['close'] <= last_row['lower_band']:
        return "Buy"
        
    # Sell Signal: Touches or crosses above upper band
    if last_row['close'] >= last_row['upper_band']:
        return "Sell"
        
    return "Wait"
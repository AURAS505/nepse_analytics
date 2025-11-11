import pandas as pd
from datetime import datetime, timedelta

from .data_service import MarketDataService
from technical_analysis.models import IndicatorType, IndicatorValue
from technical_analysis import indicators as ta

class IndicatorService:
    """
    Service layer for calculating and retrieving technical indicators.
    Acts as a bridge between data, calculation, and the database.
    """
    
    @staticmethod
    def get_indicator_data(symbol: str, indicator_name: str, params: dict, start_date=None, end_date=None) -> pd.Series | pd.DataFrame:
        """
        Main method to get data for a specific indicator.
        Fetches data and calls the appropriate calculation function.
        """
        
        # 1. Determine required data length
        # We need extra data for "warm-up" (e.g., a 200-day SMA needs 200 days of data)
        # Find the largest 'window' or 'period' in params
        max_period = max(v for k, v in params.items() if k in ['window', 'period', 'long_window'] + [0])
        
        # Fetch at least 2x the max period, or a default (e.g., 250 days),
        # to ensure indicators are stable.
        warm_up_days = max(max_period * 2, 250)
        
        if start_date:
            calc_start_date = start_date - timedelta(days=warm_up_days)
        else:
            calc_start_date = None # Fetch all available
            
        # 2. Get OHLCV data
        # Note: Volume indicators will need a modified MarketDataService
        ohlcv_df = MarketDataService.get_ohlcv(
            symbol=symbol,
            start_date=calc_start_date,
            end_date=end_date
        )
        
        if ohlcv_df.empty:
            return pd.DataFrame() # Return empty if no price data

        close_prices = ohlcv_df['close']
        
        # 3. Call the correct calculation function
        # This mapping is the core of the service
        
        # --- THIS IS THE FIX ---
        # Make the name matching more flexible
        name_lower = indicator_name.lower()
        
        if 'sma' in name_lower: # Catches 'sma', 'SMA_50', 'SMA_200'
            result = ta.calculate_sma(close_prices, window=params.get('window', 20))
            
        elif 'ema' in name_lower: # Catches 'ema'
            result = ta.calculate_ema(close_prices, window=params.get('window', 20))
            
        elif 'rsi' in name_lower: # Catches 'rsi'
            result = ta.calculate_rsi(close_prices, window=params.get('window', 14))
            
        elif 'macd' in name_lower: # Catches 'macd'
            result = ta.calculate_macd(
                close_prices,
                short_window=params.get('short_window', 12),
                long_window=params.get('long_window', 26),
                signal_window=params.get('signal_window', 9)
            )
            
        elif 'bollinger' in name_lower: # Catches 'bollinger_bands'
            result = ta.calculate_bollinger_bands(
                close_prices,
                window=params.get('window', 20),
                std_dev=params.get('std_dev', 2)
            )
            
        # --- THIS IS THE UPDATE ---
        # Add OBV (requires volume)
        elif 'obv' in name_lower:
            # Assumes get_ohlcv can also return volume
            if 'volume' not in ohlcv_df.columns:
                raise ValueError("Volume data is required for OBV")
            result = ta.calculate_obv(close_prices, ohlcv_df['volume'])
            
        else:
            raise NotImplementedError(f"Indicator '{indicator_name}' is not implemented.")
        
        # 4. Trim warm-up data
        if start_date:
            result = result.loc[start_date:]
            
        return result

    @staticmethod
    def calculate_and_store(symbol: str, indicator_type: IndicatorType, end_date: datetime.date):
        """
        Calculates a specific indicator for a symbol up to end_date
        and saves the latest value to the IndicatorValue model.
        
        This is ideal for a daily batch job.
        """
        params = indicator_type.default_parameters
        
        try:
            # Get full data series
            indicator_data = IndicatorService.get_indicator_data(
                symbol=symbol,
                indicator_name=indicator_type.name,
                params=params,
                end_date=end_date
            )
            
            if indicator_data.empty:
                print(f"No data for {symbol}, skipping {indicator_type.name}")
                return

            # Get the very last calculated value(s)
            latest_data = indicator_data.iloc[-1]
            latest_date = indicator_data.index[-1].date()

            value_to_store = None
            json_to_store = None

            if isinstance(latest_data, pd.DataFrame):
                # For multi-value indicators like MACD
                json_to_store = latest_data.to_dict()
            elif isinstance(latest_data, pd.Series):
                 # For multi-value indicators like Bollinger Bands
                json_to_store = latest_data.to_dict()
            else:
                # For single-value indicators like RSI, SMA
                value_to_store = latest_data
            
            if pd.isna(value_to_store) and json_to_store is None:
                 print(f"Calculation resulted in NaN for {symbol} on {latest_date}")
                 return

            # Save to database
            obj, created = IndicatorValue.objects.update_or_create(
                symbol=symbol,
                indicator_type=indicator_type,
                business_date=latest_date,
                defaults={
                    'value': value_to_store,
                    'value_json': json_to_store,
                    'parameters': params
                }
            )
            
            if created:
                print(f"CREATED: {obj}")
            else:
                print(f"UPDATED: {obj}")

        except Exception as e:
            print(f"Error calculating {indicator_type.name} for {symbol}: {e}")
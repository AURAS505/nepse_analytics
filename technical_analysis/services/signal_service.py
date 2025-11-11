from datetime import datetime
import pandas as pd

from .indicator_service import IndicatorService
from .data_service import MarketDataService
from technical_analysis.models import Signal, TradingStrategy

class SignalService:
    """
    Service for generating trading signals based on indicator data.
    """
    
    @staticmethod
    def generate_rsi_signals(symbol: str, strategy: TradingStrategy):
        """
        Generates signals for an RSI Overbought/Oversold strategy.
        """
        params = strategy.config.get('rsi_params', {'window': 14})
        overbought = strategy.config.get('overbought', 70)
        oversold = strategy.config.get('oversold', 30)
        
        # 1. Get Indicator Data
        rsi_series = IndicatorService.get_indicator_data(
            symbol=symbol,
            indicator_name='rsi',
            params=params
        )
        
        if rsi_series.empty or len(rsi_series) < 2:
            return # Not enough data
            
        # 2. Get latest price for the signal
        latest_price_data = MarketDataService.get_latest_price(symbol)
        if not latest_price_data:
            return
            
        latest_price = latest_price_data['close']
        latest_date = latest_price_data['date']
        
        # 3. Get the last two data points to check for a crossover
        today_rsi = rsi_series.iloc[-1]
        yesterday_rsi = rsi_series.iloc[-2]
        
        new_signal = None
        
        # 4. Check for crossover signals
        
        # Bearish Crossover (Sell Signal)
        if yesterday_rsi >= overbought and today_rsi < overbought:
            new_signal = Signal(
                symbol=symbol,
                strategy=strategy,
                signal_type='SELL',
                strength=8, # High strength
                confidence=80.0,
                business_date=latest_date,
                price_at_signal=latest_price,
                reason=f"RSI crossed below Overbought level ({overbought}) from {yesterday_rsi:.2f} to {today_rsi:.2f}.",
                technical_summary={'rsi': today_rsi, 'prev_rsi': yesterday_rsi}
            )
            
        # Bullish Crossover (Buy Signal)
        elif yesterday_rsi <= oversold and today_rsi > oversold:
            new_signal = Signal(
                symbol=symbol,
                strategy=strategy,
                signal_type='BUY',
                strength=8, # High strength
                confidence=80.0,
                business_date=latest_date,
                price_at_signal=latest_price,
                reason=f"RSI crossed above Oversold level ({oversold}) from {yesterday_rsi:.2f} to {today_rsi:.2f}.",
                technical_summary={'rsi': today_rsi, 'prev_rsi': yesterday_rsi}
            )
            
        if new_signal:
            # Deactivate old signals for this symbol/strategy
            Signal.objects.filter(
                symbol=symbol,
                strategy=strategy,
                is_active=True
            ).update(is_active=False)
            
            # Save new signal
            new_signal.save()
            print(f"CREATED Signal: {new_signal}")

    @staticmethod
    def generate_ma_crossover_signals(symbol: str, strategy: TradingStrategy):
        """
        Generates signals for a Moving Average Crossover strategy.
        """
        short_window = strategy.config.get('short_window', 50)
        long_window = strategy.config.get('long_window', 200)
        
        # 1. Get Indicator Data
        sma_short = IndicatorService.get_indicator_data(
            symbol=symbol,
            indicator_name='sma',
            params={'window': short_window}
        )
        sma_long = IndicatorService.get_indicator_data(
            symbol=symbol,
            indicator_name='sma',
            params={'window': long_window}
        )
        
        if sma_short.empty or sma_long.empty or len(sma_short) < 2 or len(sma_long) < 2:
            return

        # 2. Get latest price
        latest_price_data = MarketDataService.get_latest_price(symbol)
        if not latest_price_data:
            return
            
        latest_price = latest_price_data['close']
        latest_date = latest_price_data['date']

        # 3. Check for crossover
        # We need to align the series in case of NaNs at the beginning
        signals_df = pd.DataFrame({'short': sma_short, 'long': sma_long}).dropna()
        if len(signals_df) < 2:
            return

        today = signals_df.iloc[-1]
        yesterday = signals_df.iloc[-2]
        
        new_signal = None
        
        # 4. Check for signals
        
        # Golden Cross (Buy Signal)
        if yesterday['short'] <= yesterday['long'] and today['short'] > today['long']:
            new_signal = Signal(
                symbol=symbol,
                strategy=strategy,
                signal_type='BUY',
                strength=7,
                confidence=75.0,
                business_date=latest_date,
                price_at_signal=latest_price,
                reason=f"Golden Cross: SMA({short_window}) crossed above SMA({long_window}).",
                technical_summary={'short_ma': today['short'], 'long_ma': today['long']}
            )
            
        # Death Cross (Sell Signal)
        elif yesterday['short'] >= yesterday['long'] and today['short'] < today['long']:
            new_signal = Signal(
                symbol=symbol,
                strategy=strategy,
                signal_type='SELL',
                strength=7,
                confidence=75.0,
                business_date=latest_date,
                price_at_signal=latest_price,
                reason=f"Death Cross: SMA({short_window}) crossed below SMA({long_window}).",
                technical_summary={'short_ma': today['short'], 'long_ma': today['long']}
            )
            
        if new_signal:
            Signal.objects.filter(
                symbol=symbol,
                strategy=strategy,
                is_active=True
            ).update(is_active=False)
            new_signal.save()
            print(f"CREATED Signal: {new_signal}")
            
    @staticmethod
    def run_all_strategies_for_symbol(symbol: str):
        """
        Runs all active strategies for a given symbol.
        """
        active_strategies = TradingStrategy.objects.filter(is_active=True)
        
        for strategy in active_strategies:
            try:
                if strategy.strategy_type == 'RSI_OVERSOLD':
                    SignalService.generate_rsi_signals(symbol, strategy)
                elif strategy.strategy_type == 'MA_CROSSOVER':
                    SignalService.generate_ma_crossover_signals(symbol, strategy)
                # Add more strategy types here
                
            except Exception as e:
                print(f"Error running strategy {strategy.name} for {symbol}: {e}")
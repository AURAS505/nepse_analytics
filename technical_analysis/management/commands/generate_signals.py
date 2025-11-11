import time
from django.core.management.base import BaseCommand
from technical_analysis.models import TradingStrategy
from technical_analysis.services import MarketDataService, SignalService

class Command(BaseCommand):
    help = "Runs all active trading strategies to generate new signals."

    def add_arguments(self, parser):
        parser.add_argument(
            '--symbol',
            type=str,
            help='Generate signals for a single symbol.',
        )
        parser.add_argument(
            '--strategy',
            type=str,
            help='Run a single strategy by its `strategy_type` (e.g., MA_CROSSOVER).',
        )

    def handle(self, *args, **options):
        start_time = time.time()
        self.stdout.write("Starting signal generation task...")
        
        symbol_option = options['symbol']
        strategy_option = options['strategy']

        # Get Symbols
        if symbol_option:
            symbols = [symbol_option.upper()]
            self.stdout.write(self.style.WARNING(f"Running for single symbol: {symbol_option}"))
        else:
            symbols = MarketDataService.get_active_symbols()
            self.stdout.write(f"Processing {len(symbols)} active symbols.")

        # Get Strategies
        strategies_query = TradingStrategy.objects.filter(is_active=True)
        if strategy_option:
            strategies_query = strategies_query.filter(strategy_type=strategy_option)
            self.stdout.write(self.style.WARNING(f"Running for single strategy: {strategy_option}"))
            
        active_strategies = list(strategies_query)
        if not active_strategies:
            self.stdout.write(self.style.ERROR("No active TradingStrategy found. Please populate the database."))
            return
            
        self.stdout.write(f"Using {len(active_strategies)} active strategies.")

        total_symbols = len(symbols)
        
        for i, symbol in enumerate(symbols):
            self.stdout.write(f"--- Scanning {symbol} ({i+1}/{total_symbols}) ---")
            for strategy in active_strategies:
                try:
                    # The SignalService methods will find/create signals
                    if strategy.strategy_type == 'RSI_OVERSOLD':
                        SignalService.generate_rsi_signals(symbol, strategy)
                    elif strategy.strategy_type == 'MA_CROSSOVER':
                        SignalService.generate_ma_crossover_signals(symbol, strategy)
                    # Add more strategy types here as services are built
                    
                    self.stdout.write(self.style.SUCCESS(f"  Ran strategy '{strategy.name}' for {symbol}"))
                except Exception as e:
                    self.stdout.write(self.style.ERROR(f"  Failed to run {strategy.name} for {symbol}: {e}"))

        end_time = time.time()
        self.stdout.write(self.style.SUCCESS(f"--- Task Complete ---"))
        self.stdout.write(f"Total time taken: {end_time - start_time:.2f} seconds")
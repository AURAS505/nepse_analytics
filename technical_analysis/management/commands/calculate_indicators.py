import time
from datetime import datetime
from django.core.management.base import BaseCommand
from technical_analysis.models import IndicatorType
from technical_analysis.services import MarketDataService, IndicatorService

class Command(BaseCommand):
    help = "Calculates and stores technical indicator values for all active symbols."

    def add_arguments(self, parser):
        parser.add_argument(
            '--symbol',
            type=str,
            help='Calculate indicators for a single symbol.',
        )

    def handle(self, *args, **options):
        start_time = time.time()
        today = datetime.now().date()
        
        symbol_option = options['symbol']
        
        if symbol_option:
            symbols = [symbol_option.upper()]
            self.stdout.write(self.style.WARNING(f"Calculating indicators for single symbol: {symbol_option}"))
        else:
            self.stdout.write("Fetching all active symbols...")
            symbols = MarketDataService.get_active_symbols()
            self.stdout.write(f"Found {len(symbols)} active symbols.")

        indicator_types = list(IndicatorType.objects.filter(is_active=True))
        if not indicator_types:
            self.stdout.write(self.style.ERROR("No active IndicatorType found in database. Please populate IndicatorType model."))
            return

        self.stdout.write(f"Calculating {len(indicator_types)} active indicators...")
        
        total_symbols = len(symbols)
        
        for i, symbol in enumerate(symbols):
            self.stdout.write(f"--- Processing {symbol} ({i+1}/{total_symbols}) ---")
            for ind_type in indicator_types:
                try:
                    IndicatorService.calculate_and_store(
                        symbol=symbol,
                        indicator_type=ind_type,
                        end_date=today
                    )
                    self.stdout.write(self.style.SUCCESS(f"  Successfully calculated {ind_type.name} for {symbol}"))
                except Exception as e:
                    self.stdout.write(self.style.ERROR(f"  Failed to calculate {ind_type.name} for {symbol}: {e}"))

        end_time = time.time()
        self.stdout.write(self.style.SUCCESS(f"--- Task Complete ---"))
        self.stdout.write(f"Total time taken: {end_time - start_time:.2f} seconds")
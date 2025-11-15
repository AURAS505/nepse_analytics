"""
Management command to populate fiscal years
Usage: python manage.py populate_fiscal_years
"""
from django.core.management.base import BaseCommand
from django.db import transaction
from django.utils import timezone
from nepali_datetime.models import FiscalYear
from nepali_datetime.utils import bs_to_ad, NEPALI_CALENDAR_DATA


class Command(BaseCommand):
    help = 'Populate Nepal fiscal year data'

    def add_arguments(self, parser):
        parser.add_argument(
            '--start-year',
            type=int,
            default=2070,
            help='Starting BS year (default: 2070)'
        )
        parser.add_argument(
            '--end-year',
            type=int,
            default=2090,
            help='Ending BS year (default: 2090)'
        )
        parser.add_argument(
            '--clear',
            action='store_true',
            help='Clear existing fiscal year data before populating'
        )

    def handle(self, *args, **options):
        start_year = options['start_year']
        end_year = options['end_year']
        clear_existing = options['clear']

        if clear_existing:
            self.stdout.write(self.style.WARNING('Clearing existing fiscal year data...'))
            FiscalYear.objects.all().delete()

        self.stdout.write(f'Populating fiscal years for BS {start_year}-{end_year}...')

        created_count = 0
        updated_count = 0
        today = timezone.now().date()
        
        with transaction.atomic():
            for year in range(start_year, end_year + 1):
                if year not in NEPALI_CALENDAR_DATA or (year + 1) not in NEPALI_CALENDAR_DATA:
                    self.stdout.write(
                        self.style.WARNING(
                            f'Skipping FY {year}/{str(year+1)[-2:]} - incomplete data'
                        )
                    )
                    continue

                # Fiscal year format: 2080/81
                fiscal_year_str = f"{year}/{str(year + 1)[-2:]}"
                
                try:
                    # Start: Shrawan 1 of start year
                    ad_start = bs_to_ad(year, 4, 1)  # Month 4 = Shrawan
                    
                    # End: Last day of Ashadh of end year
                    ashadh_days = NEPALI_CALENDAR_DATA[year + 1][2]  # Month 3 = Ashadh (index 2)
                    ad_end = bs_to_ad(year + 1, 3, ashadh_days)
                    
                    # Calculate total days
                    total_days = (ad_end.date() - ad_start.date()).days + 1
                    
                    # Calculate English fiscal year
                    ad_start_year = ad_start.year
                    ad_end_year = ad_end.year
                    if ad_start_year == ad_end_year:
                        fiscal_year_english = str(ad_start_year)
                    else:
                        fiscal_year_english = f"{ad_start_year}/{str(ad_end_year)[-2:]}"
                    
                    # Check if this is current fiscal year
                    is_current = ad_start.date() <= today <= ad_end.date()
                    
                    # Create or update fiscal year
                    obj, created = FiscalYear.objects.update_or_create(
                        fiscal_year=fiscal_year_str,
                        defaults={
                            'bs_start_year': year,
                            'bs_start_month': 4,
                            'bs_start_day': 1,
                            'bs_end_year': year + 1,
                            'bs_end_month': 3,
                            'bs_end_day': ashadh_days,
                            'ad_start_date': ad_start.date(),
                            'ad_end_date': ad_end.date(),
                            'ad_start_year': ad_start_year,
                            'ad_end_year': ad_end_year,
                            'fiscal_year_english': fiscal_year_english,
                            'is_current': is_current,
                            'total_days': total_days,
                        }
                    )
                    
                    if created:
                        created_count += 1
                        status = self.style.SUCCESS('✓ Created')
                    else:
                        updated_count += 1
                        status = self.style.WARNING('↻ Updated')
                    
                    current_marker = ' [CURRENT]' if is_current else ''
                    self.stdout.write(
                        f'{status} FY {fiscal_year_str} (AD: {fiscal_year_english}): '
                        f'{ad_start.date()} to {ad_end.date()} '
                        f'({total_days} days){current_marker}'
                    )
                    
                except Exception as e:
                    self.stdout.write(
                        self.style.ERROR(
                            f'Error creating FY {fiscal_year_str}: {e}'
                        )
                    )
                    continue

        self.stdout.write('\n' + '='*60)
        self.stdout.write(
            self.style.SUCCESS(
                f'Successfully populated fiscal years!\n'
                f'Created: {created_count}\n'
                f'Updated: {updated_count}\n'
                f'Total: {FiscalYear.objects.count()}'
            )
        )
        
        # Show current fiscal year
        current_fy = FiscalYear.get_current_fiscal_year()
        if current_fy:
            self.stdout.write(
                self.style.SUCCESS(
                    f'\nCurrent Fiscal Year: {current_fy.fiscal_year}\n'
                    f'Period: {current_fy.ad_start_date} to {current_fy.ad_end_date}'
                )
            )
        else:
            self.stdout.write(
                self.style.WARNING('\nNo current fiscal year found!')
            )
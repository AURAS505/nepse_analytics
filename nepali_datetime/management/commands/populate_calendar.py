"""
Management command to populate Nepali calendar data
Usage: python manage.py populate_calendar
"""
from django.core.management.base import BaseCommand
from django.db import transaction
from django.db import models
from nepali_datetime.models import NepaliCalendar
from nepali_datetime.utils import NEPALI_CALENDAR_DATA, bs_to_ad


class Command(BaseCommand):
    help = 'Populate Nepali calendar data for BS years'

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
            help='Clear existing calendar data before populating'
        )

    def handle(self, *args, **options):
        start_year = options['start_year']
        end_year = options['end_year']
        clear_existing = options['clear']

        if clear_existing:
            self.stdout.write(self.style.WARNING('Clearing existing calendar data...'))
            NepaliCalendar.objects.all().delete()

        self.stdout.write(f'Populating calendar data for BS years {start_year}-{end_year}...')

        created_count = 0
        updated_count = 0
        
        with transaction.atomic():
            for year in range(start_year, end_year + 1):
                if year not in NEPALI_CALENDAR_DATA:
                    self.stdout.write(
                        self.style.WARNING(f'Skipping year {year} - no data available')
                    )
                    continue

                for month in range(1, 13):
                    days_in_month = NEPALI_CALENDAR_DATA[year][month - 1]
                    
                    # Calculate AD start date for this BS month
                    try:
                        ad_start = bs_to_ad(year, month, 1)
                    except Exception as e:
                        self.stdout.write(
                            self.style.ERROR(f'Error calculating date for {year}/{month}: {e}')
                        )
                        continue

                    # Create or update calendar entry
                    obj, created = NepaliCalendar.objects.update_or_create(
                        bs_year=year,
                        month=month,
                        defaults={
                            'days_in_month': days_in_month,
                            'ad_start_date': ad_start.date(),
                        }
                    )

                    if created:
                        created_count += 1
                    else:
                        updated_count += 1

        self.stdout.write(
            self.style.SUCCESS(
                f'Successfully populated calendar data!\n'
                f'Created: {created_count} entries\n'
                f'Updated: {updated_count} entries'
            )
        )
        
        # Display summary
        total_entries = NepaliCalendar.objects.count()
        year_range = NepaliCalendar.objects.aggregate(
            min_year=models.Min('bs_year'),
            max_year=models.Max('bs_year')
        )
        
        self.stdout.write(
            self.style.SUCCESS(
                f'\nCalendar Summary:\n'
                f'Total entries: {total_entries}\n'
                f'Year range: {year_range["min_year"]} - {year_range["max_year"]}'
            )
        )
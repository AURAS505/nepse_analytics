from django.db import models
from django.core.exceptions import ValidationError
from django.utils import timezone
from datetime import datetime, timedelta


class NepaliCalendar(models.Model):
    """Store Nepali calendar data for BS to AD conversion"""
    
    MONTH_CHOICES = [
        (1, 'Baisakh'),
        (2, 'Jestha'),
        (3, 'Ashadh'),
        (4, 'Shrawan'),
        (5, 'Bhadra'),
        (6, 'Ashwin'),
        (7, 'Kartik'),
        (8, 'Mangsir'),
        (9, 'Poush'),
        (10, 'Magh'),
        (11, 'Falgun'),
        (12, 'Chaitra'),
    ]
    
    bs_year = models.IntegerField(db_index=True, help_text="Bikram Sambat Year")
    month = models.IntegerField(choices=MONTH_CHOICES, db_index=True)
    days_in_month = models.IntegerField(help_text="Number of days in this month")
    ad_start_date = models.DateField(
        help_text="Gregorian date when this BS month starts",
        null=True, 
        blank=True
    )
    
    class Meta:
        ordering = ['bs_year', 'month']
        unique_together = [['bs_year', 'month']]
        verbose_name = "Nepali Calendar Entry"
        verbose_name_plural = "Nepali Calendar"
        indexes = [
            models.Index(fields=['bs_year', 'month']),
        ]
    
    def __str__(self):
        return f"{self.get_month_display()} {self.bs_year} ({self.days_in_month} days)"
    
    def clean(self):
        if self.month < 1 or self.month > 12:
            raise ValidationError("Month must be between 1 and 12")
        if self.days_in_month < 28 or self.days_in_month > 32:
            raise ValidationError("Days in month must be between 28 and 32")
    
    @property
    def month_name(self):
        return self.get_month_display()


class FiscalYear(models.Model):
    """Nepal Fiscal Year runs from Shrawan 1 to Ashad end (July-July)"""
    
    fiscal_year = models.CharField(
        max_length=10, 
        unique=True, 
        db_index=True,
        help_text="Nepali Fiscal Year Format: 2080/81"
    )
    
    fiscal_year_english = models.CharField(
        max_length=10,
        db_index=True,
        help_text="English Fiscal Year Format: 2023/24",
        blank=True,
        null=True
    )
    
    # BS Dates
    bs_start_year = models.IntegerField()
    bs_start_month = models.IntegerField(default=4)  # Shrawan
    bs_start_day = models.IntegerField(default=1)
    
    bs_end_year = models.IntegerField()
    bs_end_month = models.IntegerField(default=3)  # Ashadh
    bs_end_day = models.IntegerField()  # Usually 31 or 32
    
    # AD Dates (converted)
    ad_start_date = models.DateField(db_index=True)
    ad_end_date = models.DateField(db_index=True)
    ad_start_year = models.IntegerField(help_text="Starting AD year", null=True, blank=True)
    ad_end_year = models.IntegerField(help_text="Ending AD year", null=True, blank=True)
    
    # Status
    is_current = models.BooleanField(default=False, db_index=True)
    
    # Metadata
    total_days = models.IntegerField(help_text="Total days in fiscal year")
    notes = models.TextField(blank=True, null=True)
    
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)
    
    class Meta:
        ordering = ['-bs_start_year']
        verbose_name = "Fiscal Year"
        verbose_name_plural = "Fiscal Years"
        indexes = [
            models.Index(fields=['-bs_start_year']),
            models.Index(fields=['ad_start_date', 'ad_end_date']),
        ]
    
    def __str__(self):
        return f"FY {self.fiscal_year}"
    
    def clean(self):
        if self.bs_start_month != 4:
            raise ValidationError("Fiscal year must start in Shrawan (month 4)")
        if self.bs_end_month != 3:
            raise ValidationError("Fiscal year must end in Ashadh (month 3)")
        if self.ad_end_date <= self.ad_start_date:
            raise ValidationError("End date must be after start date")
    
    def save(self, *args, **kwargs):
        # Auto-calculate English fiscal year from AD dates
        if self.ad_start_date and self.ad_end_date:
            self.ad_start_year = self.ad_start_date.year
            self.ad_end_year = self.ad_end_date.year
            
            # Generate English fiscal year string
            # Format: 2024/25 (short form) or keep full years
            if self.ad_start_year == self.ad_end_year:
                self.fiscal_year_english = str(self.ad_start_year)
            else:
                self.fiscal_year_english = f"{self.ad_start_year}/{str(self.ad_end_year)[-2:]}"
        
        # Auto-set is_current based on today's date
        today = timezone.now().date()
        if self.ad_start_date <= today <= self.ad_end_date:
            # Unset other current fiscal years
            FiscalYear.objects.filter(is_current=True).update(is_current=False)
            self.is_current = True
        
        super().save(*args, **kwargs)
    
    @classmethod
    def get_current_fiscal_year(cls):
        """Get the current fiscal year"""
        return cls.objects.filter(is_current=True).first()
    
    @classmethod
    def get_fiscal_year_for_date(cls, date):
        """Get fiscal year for a given date"""
        return cls.objects.filter(
            ad_start_date__lte=date,
            ad_end_date__gte=date
        ).first()
    
    def get_quarter(self, date):
        """Get quarter (1-4) for a date within this fiscal year"""
        if not (self.ad_start_date <= date <= self.ad_end_date):
            return None
        
        days_elapsed = (date - self.ad_start_date).days
        quarter = (days_elapsed // 91) + 1
        return min(quarter, 4)
    
    @property
    def display_name(self):
        return f"FY {self.fiscal_year} (AD: {self.fiscal_year_english})"
    
    @property
    def bs_display(self):
        return f"{self.bs_start_year}/{self.bs_start_month}/{self.bs_start_day} - {self.bs_end_year}/{self.bs_end_month}/{self.bs_end_day}"
    
    @property
    def ad_display(self):
        return f"{self.ad_start_date.strftime('%Y-%m-%d')} to {self.ad_end_date.strftime('%Y-%m-%d')}"
    
    @property
    def full_display(self):
        return f"BS: {self.fiscal_year} | AD: {self.fiscal_year_english} ({self.ad_start_date.strftime('%b %Y')} - {self.ad_end_date.strftime('%b %Y')})"


class DateConversion(models.Model):
    """Cache for date conversions to improve performance"""
    
    bs_year = models.IntegerField(db_index=True)
    bs_month = models.IntegerField(db_index=True)
    bs_day = models.IntegerField()
    
    ad_date = models.DateField(unique=True, db_index=True)
    
    fiscal_year = models.ForeignKey(
        FiscalYear,
        on_delete=models.SET_NULL,
        null=True,
        blank=True,
        related_name='date_conversions'
    )
    
    day_of_week = models.IntegerField(
        help_text="0=Monday, 6=Sunday"
    )
    
    is_weekend = models.BooleanField(default=False)
    
    created_at = models.DateTimeField(auto_now_add=True)
    
    class Meta:
        ordering = ['ad_date']
        verbose_name = "Date Conversion"
        verbose_name_plural = "Date Conversions"
        indexes = [
            models.Index(fields=['bs_year', 'bs_month', 'bs_day']),
            models.Index(fields=['ad_date']),
        ]
        unique_together = [['bs_year', 'bs_month', 'bs_day']]
    
    def __str__(self):
        return f"BS {self.bs_year}/{self.bs_month}/{self.bs_day} = AD {self.ad_date}"
    
    def save(self, *args, **kwargs):
        if self.ad_date:
            self.day_of_week = self.ad_date.weekday()
            self.is_weekend = self.day_of_week == 5  # Saturday is weekend in Nepal
            
            # Auto-assign fiscal year
            if not self.fiscal_year:
                self.fiscal_year = FiscalYear.get_fiscal_year_for_date(self.ad_date)
        
        super().save(*args, **kwargs)
    
    @property
    def bs_display(self):
        calendar = NepaliCalendar.objects.filter(
            bs_year=self.bs_year,
            month=self.bs_month
        ).first()
        month_name = calendar.month_name if calendar else str(self.bs_month)
        return f"{month_name} {self.bs_day}, {self.bs_year}"
    
    @property
    def ad_display(self):
        return self.ad_date.strftime("%B %d, %Y")


class PublicHoliday(models.Model):
    """Nepal public holidays"""
    
    HOLIDAY_TYPE_CHOICES = [
        ('national', 'National Holiday'),
        ('festival', 'Festival'),
        ('bank', 'Bank Holiday'),
        ('nepse', 'NEPSE Closed'),
        ('other', 'Other'),
    ]
    
    name = models.CharField(max_length=200)
    name_nepali = models.CharField(max_length=200, blank=True, null=True)
    
    bs_year = models.IntegerField()
    bs_month = models.IntegerField()
    bs_day = models.IntegerField()
    
    ad_date = models.DateField(db_index=True)
    
    holiday_type = models.CharField(
        max_length=20,
        choices=HOLIDAY_TYPE_CHOICES,
        default='national'
    )
    
    is_nepse_trading_day = models.BooleanField(
        default=False,
        help_text="Is NEPSE open for trading on this holiday?"
    )
    
    description = models.TextField(blank=True, null=True)
    
    class Meta:
        ordering = ['ad_date']
        verbose_name = "Public Holiday"
        verbose_name_plural = "Public Holidays"
        indexes = [
            models.Index(fields=['ad_date']),
            models.Index(fields=['bs_year']),
        ]
    
    def __str__(self):
        return f"{self.name} ({self.ad_date})"
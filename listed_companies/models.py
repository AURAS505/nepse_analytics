# listed_companies/models.py
from django.db import models
from django.core.validators import MinValueValidator, MaxValueValidator
from django.core.exceptions import ValidationError

# ========================================
# EXISTING MODELS - Keep as is
# ========================================

class Companies(models.Model):
    """Your existing Companies model - unchanged"""
    nepse_code = models.CharField(primary_key=True, max_length=50)
    script_ticker = models.CharField(unique=True, max_length=20)
    company_name = models.CharField(max_length=255)
    sector = models.CharField(max_length=100, blank=True, null=True)
    type = models.CharField(max_length=50, blank=True, null=True)
    status = models.CharField(max_length=50, blank=True, null=True)
    instrument = models.CharField(max_length=50, blank=True, null=True)
    par_value = models.DecimalField(max_digits=10, decimal_places=2, default=100.00)

    class Meta:
        db_table = 'companies'

    def __str__(self):
        return self.company_name


class FloorsheetRaw(models.Model):
    """Your existing FloorsheetRaw model - unchanged"""
    id = models.BigIntegerField(primary_key=True)
    contract_no = models.CharField(max_length=255, blank=True, null=True)
    stock_symbol = models.CharField(max_length=255)
    buyer = models.IntegerField(blank=True, null=True)
    seller = models.IntegerField(blank=True, null=True)
    quantity = models.IntegerField(blank=True, null=True)
    rate = models.DecimalField(max_digits=10, decimal_places=2, blank=True, null=True)
    amount = models.DecimalField(max_digits=15, decimal_places=2, blank=True, null=True)
    calculation_date = models.DateField(blank=True, null=True)
    sector = models.CharField(max_length=255, blank=True, null=True)

    class Meta:
        managed = False
        db_table = 'floorsheet_raw'
        verbose_name = 'Floorsheet (Raw)'
        verbose_name_plural = 'Floorsheet (Raw)'

    def __str__(self):
        return f"{self.stock_symbol} ({self.contract_no})"


# ========================================
# NEW MODELS - For Shareholding Tracking
# These are completely independent and won't affect existing data
# ========================================

class ShareholdingPattern(models.Model):
    """
    NEW: Tracks shareholding pattern changes over time
    Links to Companies via script_ticker (no ForeignKey to avoid constraints)
    """
    company_symbol = models.CharField(
        max_length=20, 
        db_index=True,
        help_text="References Companies.script_ticker"
    )
    as_of_date = models.DateField(
        db_index=True,
        help_text="Date of this shareholding snapshot"
    )
    
    # Promoter Holdings
    promoter_shares = models.BigIntegerField(
        null=True, 
        blank=True,
        validators=[MinValueValidator(0)],
        help_text="Number of promoter shares"
    )
    promoter_percentage = models.DecimalField(
        max_digits=5, 
        decimal_places=2,
        null=True, 
        blank=True,
        validators=[MinValueValidator(0), MaxValueValidator(100)],
        help_text="Promoter shareholding percentage"
    )
    
    # Public Holdings
    public_shares = models.BigIntegerField(
        null=True, 
        blank=True,
        validators=[MinValueValidator(0)],
        help_text="Number of public shares"
    )
    public_percentage = models.DecimalField(
        max_digits=5, 
        decimal_places=2,
        null=True, 
        blank=True,
        validators=[MinValueValidator(0), MaxValueValidator(100)],
        help_text="Public shareholding percentage"
    )
    
    # Institutional Holdings (Mutual Funds, Insurance, etc.)
    institutional_shares = models.BigIntegerField(
        null=True, 
        blank=True,
        validators=[MinValueValidator(0)],
        help_text="Number of institutional shares"
    )
    institutional_percentage = models.DecimalField(
        max_digits=5, 
        decimal_places=2,
        null=True, 
        blank=True,
        validators=[MinValueValidator(0), MaxValueValidator(100)],
        help_text="Institutional shareholding percentage"
    )
    
    # Other Holdings
    other_shares = models.BigIntegerField(
        null=True, 
        blank=True,
        validators=[MinValueValidator(0)]
    )
    other_percentage = models.DecimalField(
        max_digits=5, 
        decimal_places=2,
        null=True, 
        blank=True,
        validators=[MinValueValidator(0), MaxValueValidator(100)]
    )
    
    # Free Float
    free_float_shares = models.BigIntegerField(
        null=True, 
        blank=True,
        help_text="Shares available for public trading"
    )
    free_float_percentage = models.DecimalField(
        max_digits=5, 
        decimal_places=2,
        null=True, 
        blank=True,
        help_text="Free float percentage"
    )
    
    # Total
    total_shares = models.BigIntegerField(
        null=True, 
        blank=True,
        help_text="Total outstanding shares"
    )
    
    # Metadata
    source = models.CharField(
        max_length=100, 
        blank=True, 
        null=True,
        help_text="Data source (e.g., NEPSE, AGM Report)"
    )
    remarks = models.TextField(blank=True, null=True)
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    class Meta:
        db_table = 'shareholding_patterns'
        verbose_name = 'Shareholding Pattern'
        verbose_name_plural = 'Shareholding Patterns'
        unique_together = ('company_symbol', 'as_of_date')
        ordering = ['-as_of_date', 'company_symbol']
        indexes = [
            models.Index(fields=['company_symbol', '-as_of_date']),
            models.Index(fields=['as_of_date']),
        ]

    def __str__(self):
        return f"{self.company_symbol} - {self.as_of_date}"
    
    @property
    def company(self):
        """Helper to get related company object"""
        try:
            return Companies.objects.get(script_ticker=self.company_symbol)
        except Companies.DoesNotExist:
            return None
    
    def clean(self):
        """Validate that percentages add up to approximately 100%"""
        percentages = [
            self.promoter_percentage or 0,
            self.public_percentage or 0,
            self.institutional_percentage or 0,
            self.other_percentage or 0
        ]
        total = sum(percentages)
        if total > 0 and abs(total - 100) > 0.5:  # Allow 0.5% tolerance
            raise ValidationError(
                f'Shareholding percentages must add up to 100%. Current total: {total}%'
            )


class LockInPeriod(models.Model):
    """
    NEW: Track lock-in periods for promoter and institutional shares
    """
    LOCK_IN_TYPE_CHOICES = [
        ('PROMOTER', 'Promoter Lock-in'),
        ('MF_OTHERS', 'Mutual Fund & Others Lock-in'),
        ('IPO', 'IPO Lock-in'),
        ('FPO', 'FPO Lock-in'),
        ('RIGHT', 'Right Share Lock-in'),
        ('MERGER', 'Merger/Acquisition Lock-in'),
        ('OTHER', 'Other'),
    ]
    
    company_symbol = models.CharField(
        max_length=20, 
        db_index=True,
        help_text="References Companies.script_ticker"
    )
    lock_in_type = models.CharField(
        max_length=20, 
        choices=LOCK_IN_TYPE_CHOICES,
        db_index=True
    )
    
    # Lock-in details
    locked_shares = models.BigIntegerField(
        validators=[MinValueValidator(0)],
        help_text="Number of shares under lock-in"
    )
    lock_in_start_date = models.DateField()
    lock_in_end_date = models.DateField(
        db_index=True,
        help_text="Date when lock-in period ends"
    )
    
    # Additional information
    shareholder_name = models.CharField(
        max_length=255, 
        blank=True, 
        null=True,
        help_text="Name of entity whose shares are locked"
    )
    description = models.TextField(
        blank=True, 
        null=True,
        help_text="Reason or details of lock-in"
    )
    
    # Status tracking
    is_active = models.BooleanField(
        default=True,
        help_text="Whether lock-in is currently active"
    )
    
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    class Meta:
        db_table = 'lock_in_periods'
        verbose_name = 'Lock-in Period'
        verbose_name_plural = 'Lock-in Periods'
        ordering = ['lock_in_end_date', 'company_symbol']
        indexes = [
            models.Index(fields=['company_symbol', 'lock_in_type']),
            models.Index(fields=['lock_in_end_date', 'is_active']),
        ]

    def __str__(self):
        return f"{self.company_symbol} - {self.get_lock_in_type_display()} till {self.lock_in_end_date}"
    
    @property
    def company(self):
        """Helper to get related company object"""
        try:
            return Companies.objects.get(script_ticker=self.company_symbol)
        except Companies.DoesNotExist:
            return None

    @property
    def is_expired(self):
        """Check if lock-in period has expired"""
        from django.utils import timezone
        return timezone.now().date() > self.lock_in_end_date

    @property
    def days_remaining(self):
        """Calculate days remaining in lock-in period"""
        from django.utils import timezone
        if self.is_expired:
            return 0
        today = timezone.now().date()
        return (self.lock_in_end_date - today).days


class CorporateAction(models.Model):
    """
    NEW: Track corporate actions affecting shareholding
    """
    ACTION_TYPE_CHOICES = [
        ('BONUS', 'Bonus Share'),
        ('RIGHT', 'Right Share'),
        ('DIVIDEND', 'Cash Dividend'),
        ('SPLIT', 'Stock Split'),
        ('MERGE', 'Stock Merger'),
        ('BUYBACK', 'Share Buyback'),
        ('IPO', 'Initial Public Offering'),
        ('FPO', 'Follow-on Public Offering'),
        ('DELISTING', 'Delisting'),
        ('AMALGAMATION', 'Amalgamation/Merger'),
        ('ACQUISITION', 'Acquisition'),
        ('CAPITAL_REDUCTION', 'Capital Reduction'),
        ('OTHER', 'Other'),
    ]
    
    company_symbol = models.CharField(
        max_length=20, 
        db_index=True,
        help_text="References Companies.script_ticker"
    )
    action_type = models.CharField(
        max_length=20, 
        choices=ACTION_TYPE_CHOICES,
        db_index=True
    )
    
    # Important Dates
    announcement_date = models.DateField(
        null=True, 
        blank=True,
        help_text="Date of announcement"
    )
    record_date = models.DateField(
        null=True, 
        blank=True,
        help_text="Record date for eligibility"
    )
    book_closure_date = models.DateField(
        null=True, 
        blank=True,
        help_text="Book closure date"
    )
    effective_date = models.DateField(
        null=True, 
        blank=True,
        help_text="Date when action takes effect"
    )
    
    # Action details
    details = models.JSONField(
        blank=True, 
        null=True,
        help_text="Structured data: e.g., {'ratio': '1:1', 'price': 100}"
    )
    description = models.TextField(
        blank=True, 
        null=True,
        help_text="Description of the corporate action"
    )
    
    # Impact on shareholding
    affects_promoter_holding = models.BooleanField(
        default=False,
        help_text="Does this action affect promoter holdings?"
    )
    affects_public_holding = models.BooleanField(
        default=False,
        help_text="Does this action affect public holdings?"
    )
    
    # Additional info
    regulatory_approval_date = models.DateField(null=True, blank=True)
    source_document = models.CharField(
        max_length=255, 
        blank=True, 
        null=True,
        help_text="Reference to source document"
    )
    
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    class Meta:
        db_table = 'corporate_actions'
        verbose_name = 'Corporate Action'
        verbose_name_plural = 'Corporate Actions'
        ordering = ['-announcement_date', 'company_symbol']
        indexes = [
            models.Index(fields=['company_symbol', '-announcement_date']),
            models.Index(fields=['action_type', '-announcement_date']),
        ]

    def __str__(self):
        return f"{self.company_symbol} - {self.get_action_type_display()} on {self.announcement_date}"
    
    @property
    def company(self):
        """Helper to get related company object"""
        try:
            return Companies.objects.get(script_ticker=self.company_symbol)
        except Companies.DoesNotExist:
            return None


# ========================================
# Optional: Add fields to existing Companies model
# You can add these later via migration if needed
# ========================================

"""
If you want to add listing date and other fields to Companies model,
create a migration like this:

python manage.py makemigrations listed_companies --empty

Then edit the migration:

operations = [
    migrations.AddField(
        model_name='companies',
        name='listed_date',
        field=models.DateField(null=True, blank=True),
    ),
    migrations.AddField(
        model_name='companies',
        name='listed_shares',
        field=models.BigIntegerField(null=True, blank=True),
    ),
    migrations.AddField(
        model_name='companies',
        name='paid_up_capital',
        field=models.DecimalField(max_digits=20, decimal_places=2, null=True, blank=True),
    ),
]
"""
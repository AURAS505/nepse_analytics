# my_portfolio/models.py
from django.db import models
from django.utils import timezone
from decimal import Decimal, ROUND_HALF_UP
import uuid

# Import the models from your other apps
# We link to 'Companies' from listed_companies
from listed_companies.models import Companies 
# We'll need StockPrices later for the dashboard view
from nepse_data.models import StockPrices 


def generate_unique_id():
    """Generates a unique ID in the format YYYYMMDD-XXXXXX"""
    date_prefix = timezone.now().strftime('%Y%m%d')
    random_part = str(uuid.uuid4())[:6].upper()
    return f"{date_prefix}-{random_part}"


class Transaction(models.Model):
    class TransactionType(models.TextChoices):
        BALANCE_BD = 'Balance b/d', 'Balance b/d'
        BUY = 'BUY', 'BUY'
        SALE = 'SALE', 'SALE'
        BONUS = 'BONUS', 'BONUS'
        IPO = 'IPO', 'IPO'
        RIGHT = 'RIGHT', 'RIGHT'
        CONVERSION_IN = 'CONVERSION(+)', 'CONVERSION(+)'
        CONVERSION_OUT = 'CONVERSION(-)', 'CONVERSION(-)'
        # --- NEWLY ADDED ---
        SUSPENSE_IN = 'SUSPENSE(+)', 'SUSPENSE(+)'
        SUSPENSE_OUT = 'SUSPENSE(-)', 'SUSPENSE(-)'
        # --- END NEW ---

    unique_id = models.CharField(
        max_length=50, 
        primary_key=True, 
        default=generate_unique_id, 
        editable=False
    )
    date = models.DateField()
    
    # Links to the 'Companies' model in 'listed_companies' app
    # This matches your Flask app's FOREIGN KEY
    symbol = models.ForeignKey(
        Companies, 
        on_delete=models.PROTECT, 
        to_field='script_ticker',
        db_column='symbol'
    )
    
    # These fields will be auto-populated from the Company model
    script = models.CharField(max_length=255, editable=False, blank=True)
    sector = models.CharField(max_length=100, editable=False, blank=True)
    
    transaction_type = models.CharField(
        max_length=20, 
        choices=TransactionType.choices
    )
    kitta = models.PositiveIntegerField()
    billed_amount = models.DecimalField(
        max_digits=15, 
        decimal_places=2, 
        null=True, 
        blank=True
    )
    rate = models.DecimalField(
        max_digits=10, 
        decimal_places=2, 
        null=True, 
        blank=True, 
        editable=False
    )
    broker = models.CharField(max_length=50, null=True, blank=True)
    created_at = models.DateTimeField(auto_now_add=True)

    class Meta:
        ordering = ['-date', '-created_at']
        verbose_name = 'Portfolio Transaction'
        verbose_name_plural = 'Portfolio Transactions'

    def __str__(self):
        return f"{self.date} | {self.symbol} | {self.transaction_type} | {self.kitta}"

    def save(self, *args, **kwargs):
        # Auto-populate script and sector from the linked Company
        if self.symbol:
            self.script = self.symbol.company_name
            self.sector = self.symbol.sector

        # Auto-calculate rate, matching your Flask logic
        if self.billed_amount is not None and self.kitta > 0:
            self.rate = (self.billed_amount / Decimal(self.kitta)).quantize(
                Decimal('0.01'), rounding=ROUND_HALF_UP
            )
        else:
            # Set rate to None if it can't be calculated
            self.rate = None
            
        super().save(*args, **kwargs)
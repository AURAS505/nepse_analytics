# my_portfolio/models.py
from django.db import models
from django.utils import timezone
from listed_companies.models import Companies
from nepse_data.models import Brokers
from decimal import Decimal
import uuid
from django.utils import timezone
from datetime import datetime # <-- ADD THIS IMPORT

def generate_unique_id():
    return str(uuid.uuid4())


class Transaction(models.Model):
    
    class TransactionType(models.TextChoices):
        BUY = 'BUY', 'BUY'
        SALE = 'SALE', 'SALE'
        BONUS = 'BONUS', 'BONUS'
        RIGHT = 'RIGHT', 'RIGHT'
        IPO = 'IPO', 'IPO'
        CASH = 'CASH', 'CASH' # For Cash Dividends
        CONVERSION_P = 'CONVERSION(+)', 'CONVERSION(+)'
        CONVERSION_M = 'CONVERSION(-)', 'CONVERSION(-)'
        SUSPENSE_P = 'SUSPENSE(+)', 'SUSPENSE(+)'
        SUSPENSE_M = 'SUSPENSE(-)', 'SUSPENSE(-)'
        BALANCE_BD = 'Balance b/d', 'Balance b/d'

    # --- Core Fields ---
    unique_id = models.CharField(max_length=100, unique=True, blank=True)
    date = models.DateField(default=timezone.now)
    symbol = models.ForeignKey(Companies, on_delete=models.CASCADE, to_field='script_ticker')
    transaction_type = models.CharField(max_length=20, choices=TransactionType.choices)
    
    # Kitta is nullable (for CASH type)
    kitta = models.IntegerField(null=True, blank=True) 
    
    broker = models.CharField(max_length=10, null=True, blank=True)
    
    # --- Renamed Financial Fields (as requested) ---
    rate = models.DecimalField(
        "Rate", max_digits=20, decimal_places=2, null=True, blank=True
    ) 
    gross_amount = models.DecimalField(
        max_digits=20, decimal_places=2, null=True, blank=True
    ) 
    commission_rate = models.DecimalField(
        "Comm. rate (%)", max_digits=10, decimal_places=4, null=True, blank=True
    ) 
    commission_amount = models.DecimalField(
        "Comm. Amt.", max_digits=20, decimal_places=2, null=True, blank=True
    )
    nepse_commission = models.DecimalField(
        max_digits=20, decimal_places=2, null=True, blank=True
    ) 
    sebon_regularity_fee = models.DecimalField(
        max_digits=20, decimal_places=2, null=True, blank=True
    ) 
    broker_commission = models.DecimalField(
        max_digits=20, decimal_places=2, null=True, blank=True
    ) 
    sebo_commission = models.DecimalField(
        max_digits=20, decimal_places=2, null=True, blank=True
    ) 
    cgt = models.DecimalField(
        "CGT", max_digits=20, decimal_places=2, null=True, blank=True
    ) 
    dp_fee = models.DecimalField(
        "DP Fee", max_digits=20, decimal_places=2, null=True, blank=True
    ) 
    billed_amount = models.DecimalField(max_digits=20, decimal_places=2, null=True, blank=True)

    # --- Auto-Generated/Readonly Fields ---
    script = models.CharField(max_length=255, blank=True, help_text="Auto-filled from Symbol")
    sector = models.CharField(max_length=100, blank=True, help_text="Auto-filled from Symbol")
    eff_rate = models.DecimalField( 
        "Effective Rate", max_digits=20, decimal_places=2, null=True, blank=True,
        help_text="Auto-calculated (Billed Amount / Kitta)"
    )
    created_at = models.DateTimeField(auto_now_add=True)
    
    
    class Meta:
        ordering = ['date', 'created_at']
        
    def __str__(self):
        return f"{self.symbol.script_ticker} ({self.transaction_type}) - {self.kitta} @ {self.rate}"

    # ### MODIFIED SAVE METHOD ###
    def save(self, *args, **kwargs):
        if self.symbol:
            self.script = self.symbol.company_name
            self.sector = self.symbol.sector
            
        # --- FIX for CASH transactions ---
        # Only run calculations if it's NOT a cash dividend
        if self.transaction_type != 'CASH':
            # --- Auto-calculate Gross Amount if missing ---
            if self.rate is not None and self.kitta is not None and self.kitta > 0 and self.gross_amount is None:
                self.gross_amount = self.rate * Decimal(self.kitta)

            # --- Auto-calculate Effective Rate (eff_rate) ---
            if self.billed_amount is not None and self.kitta is not None and self.kitta > 0:
                self.eff_rate = self.billed_amount / Decimal(self.kitta)
            elif self.transaction_type == 'BONUS':
                self.eff_rate = Decimal('0.00')
        else:
            # This is a CASH transaction, so force kitta and eff_rate to None
            self.kitta = None
            self.eff_rate = None
            
        # --- Auto-generate Unique ID on creation ---
        if not self.unique_id:
            
            # --- FIX for 'str' object has no attribute 'strftime' ---
            current_date_obj = self.date
            if isinstance(current_date_obj, str):
                try:
                    # Assumes YYYY-MM-DD format from the form
                    current_date_obj = datetime.strptime(current_date_obj, '%Y-%m-%d').date()
                except ValueError:
                    current_date_obj = timezone.now().date() # Fallback
            
            # Use the date object for strftime
            date_str = current_date_obj.strftime('%Y%m%d')
            
            # --- FIX for New Unique ID Format ---
            self.unique_id = f"{date_str}-{str(uuid.uuid4()).split('-')[0]}"

        super().save(*args, **kwargs)


# --- BROKER TRANSACTION MODEL (UNCHANGED AND CORRECT) ---
class BrokerTransaction(models.Model):
    
    class ActionType(models.TextChoices):
        PAYMENT = 'Payment', 'Payment'
        RECEIPT = 'Receipt', 'Receipt'
        CHQ_ISSUE = 'Chq Issue', 'Cheque Issue'
        PLEDGE_CHARGE = 'Pledge Charge', 'Pledge Charge'
        MISC_PLUS = 'Misc(+)', 'Misc (+)'
        MISC_MINUS = 'Misc(-)', 'Misc (-)'
        BALANCE_BD = 'Balance b/d', 'Balance b/d'

    unique_id = models.CharField(max_length=100, unique=True, blank=True)
    broker = models.ForeignKey(Brokers, on_delete=models.CASCADE, to_field='broker_no')
    date = models.DateField(default=timezone.now)
    action = models.CharField(max_length=20, choices=ActionType.choices)
    amount = models.DecimalField(max_digits=20, decimal_places=2)
    remarks = models.TextField(blank=True, null=True)
    created_at = models.DateTimeField(auto_now_add=True)

    class Meta:
        ordering = ['-date', '-created_at']

    def __str__(self):
        return f"{self.broker.broker_no} - {self.action} - {self.amount}"

    def save(self, *args, **kwargs):
        if not self.unique_id:
            date_str = self.date.strftime('%Y%m%d')
            self.unique_id = f"B{self.broker.broker_no}-{date_str}-{str(uuid.uuid4()).split('-')[0]}"
        super().save(*args, **kwargs)
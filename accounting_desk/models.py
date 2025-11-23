from django.db import models
from django.utils import timezone
from datetime import date
# accounting_desk/models.py
from django.core.exceptions import ValidationError
from decimal import Decimal

from my_portfolio.models import DematAccount, MeroShareHolding
from nepse_data.models import StockPrices

# --- 1. CORE ACCOUNTING MODELS ---

class AccountCategory(models.TextChoices):
    ASSET = 'ASSET', 'Asset (Cash/Bank)'
    LIABILITY = 'LIABILITY', 'Liability (Loans/Payables)'
    EQUITY = 'EQUITY', 'Equity'
    INCOME = 'INCOME', 'Income (Dividends/Profit)'
    EXPENSE = 'EXPENSE', 'Expense (Fees/Charges)'

class AccountHead(models.Model):
    name = models.CharField(max_length=100)
    category = models.CharField(max_length=20, choices=AccountCategory.choices)
    broker_code = models.CharField(max_length=10, blank=True, null=True) 

    def __str__(self):
        return f"{self.name} ({self.get_category_display()})"

class LedgerEntry(models.Model):
    ENTRY_TYPES = [('DR', 'Debit'), ('CR', 'Credit')]
    date = models.DateField(default=timezone.now)
    account = models.ForeignKey(AccountHead, on_delete=models.CASCADE, related_name='entries')
    description = models.CharField(max_length=255)
    ref_id = models.CharField(max_length=50, blank=True)
    entry_type = models.CharField(max_length=2, choices=ENTRY_TYPES)
    amount = models.DecimalField(max_digits=15, decimal_places=2)
    is_settled = models.BooleanField(default=True)
    due_date = models.DateField(null=True, blank=True)
    created_at = models.DateTimeField(auto_now_add=True)

    class Meta:
        ordering = ['-date', '-created_at']
        verbose_name_plural = "Ledger Entries"

# --- 2. BANK LOAN MODELS ---

class LoanFacility(models.Model):
    bank_name = models.CharField(max_length=100)
    account_number = models.CharField(max_length=50, blank=True)
    sanctioned_limit = models.DecimalField(max_digits=15, decimal_places=2)
    
    # Lifecycle Dates
    start_date = models.DateField(default=timezone.now, help_text="Loan Sanction Date")
    expiry_date = models.DateField(null=True, blank=True, help_text="Renewal/Expiry Date")
    
    # Current Usage (Manual Update)
    current_used_amount = models.DecimalField(max_digits=15, decimal_places=2, default=0)

    class Meta:
        verbose_name_plural = "Loan Facilities"

    def __str__(self):
        return f"{self.bank_name} - {self.account_number}"

    @property
    def get_active_rate(self):
        """Finds the applicable effective rate for today."""
        today = date.today()
        # 1. Check strict range
        active = self.interest_history.filter(effective_date__lte=today, end_date__gte=today).first()
        if active: return active.rate
        # 2. Check ongoing (no end date)
        ongoing = self.interest_history.filter(effective_date__lte=today, end_date__isnull=True).first()
        if ongoing: return ongoing.rate
        # 3. Fallback to latest added
        latest = self.interest_history.first()
        return latest.rate if latest else 0.0

class LoanInterestHistory(models.Model):
    loan_facility = models.ForeignKey(LoanFacility, on_delete=models.CASCADE, related_name='interest_history')
    
    # Rate Components
    base_rate = models.FloatField(help_text="Bank's Base Rate %", default=0.0)
    premium = models.FloatField(help_text="Agreed Premium %", default=0.0)
    
    # Effective Rate = Base + Premium (Auto-calculated)
    rate = models.FloatField(help_text="Total Effective Rate %", editable=False)
    
    effective_date = models.DateField(verbose_name="From Date")
    end_date = models.DateField(verbose_name="To Date", null=True, blank=True)
    remarks = models.CharField(max_length=200, blank=True)

    class Meta:
        ordering = ['-effective_date']

    def save(self, *args, **kwargs):
        # Auto-calculate total rate
        self.rate = float(self.base_rate) + float(self.premium)
        super().save(*args, **kwargs)

    def __str__(self):
        return f"{self.rate}% ({self.base_rate}+{self.premium})"


class PledgedScrip(models.Model):
    loan_facility = models.ForeignKey('LoanFacility', on_delete=models.CASCADE, related_name='pledged_scrips')
    
    # --- Link to Source ---
    demat_account = models.ForeignKey(DematAccount, on_delete=models.CASCADE, null=True, blank=True)
    
    symbol = models.CharField(max_length=20) 
    quantity = models.PositiveIntegerField()
    pledged_date = models.DateField(default=timezone.now)
    
    # --- Valuation Parameters ---
    average_price = models.DecimalField(max_digits=10, decimal_places=2, null=True, blank=True, help_text="Input: Average Closing Price (e.g. 180 days)")
    average_price_days = models.IntegerField(default=180, help_text="e.g. 120 or 180 days")
    
    # We store this to know what the price was when we pledged
    closing_price = models.DecimalField(max_digits=10, decimal_places=2, null=True, blank=True, help_text="Latest Closing Price from StockPrices")
    
    # This is the "Margin" (e.g., 50%)
    valuation_percent = models.FloatField(default=50, help_text="Bank's Allowable Margin %")
    
    # The final calculated limit
    allowable_drawing_power = models.DecimalField(max_digits=15, decimal_places=2, default=0, editable=False)

    def save(self, *args, **kwargs):
        # 1. Determine Base Price: Min(Average Price, Closing Price)
        base_price = min(Decimal(self.average_price), Decimal(self.closing_price))
        
        # 2. Calculate Drawing Power: Base Price * Qty * (Valuation% / 100)
        self.allowable_drawing_power = base_price * Decimal(self.quantity) * (Decimal(self.valuation_percent) / 100)
        
        super().save(*args, **kwargs)

    def __str__(self):
        return f"{self.symbol} ({self.quantity}) - {self.loan_facility.bank_name}"
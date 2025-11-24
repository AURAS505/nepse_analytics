from django.db import models
from django.utils import timezone
from datetime import date
from decimal import Decimal
from django.core.exceptions import ValidationError
from django.db.models import Sum, F

# Adjust these imports if your app name is different for portfolio
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
    start_date = models.DateField(default=timezone.now, help_text="Loan Sanction Date")
    expiry_date = models.DateField(null=True, blank=True, help_text="Renewal/Expiry Date")
    
    # This field is auto-calculated from PledgeEntry
    current_used_amount = models.DecimalField(max_digits=15, decimal_places=2, default=0, editable=False)

    class Meta:
        verbose_name_plural = "Loan Facilities"

    def __str__(self):
        return f"{self.bank_name} - {self.account_number}"

    def recalculate_usage(self):
        """Auto-calculate usage from Entry Sheet"""
        entries = self.pledge_entries.all()
        total = Decimal(0)
        for entry in entries:
            if entry.action in ['BALANCE', 'PLEDGE']:
                total += entry.utilized_loan
            elif entry.action == 'UNPLEDGE':
                total -= entry.utilized_loan
        
        self.current_used_amount = max(total, Decimal(0))
        self.save()

    @property
    def get_active_rate(self):
        today = date.today()
        active = self.interest_history.filter(effective_date__lte=today, end_date__gte=today).first()
        if active: return active.rate
        ongoing = self.interest_history.filter(effective_date__lte=today, end_date__isnull=True).first()
        if ongoing: return ongoing.rate
        latest = self.interest_history.first()
        return latest.rate if latest else 0.0


class LoanInterestHistory(models.Model):
    loan_facility = models.ForeignKey(LoanFacility, on_delete=models.CASCADE, related_name='interest_history')
    base_rate = models.FloatField(help_text="Bank's Base Rate %", default=0.0)
    premium = models.FloatField(help_text="Agreed Premium %", default=0.0)
    rate = models.FloatField(help_text="Total Effective Rate %", editable=False)
    effective_date = models.DateField(verbose_name="From Date")
    end_date = models.DateField(verbose_name="To Date", null=True, blank=True)
    remarks = models.CharField(max_length=200, blank=True)

    class Meta:
        ordering = ['-effective_date']

    def save(self, *args, **kwargs):
        self.rate = float(self.base_rate) + float(self.premium)
        super().save(*args, **kwargs)


# --- 3. MARGIN MANAGEMENT (NEW) ---

class StockMargin(models.Model):
    date = models.DateField(default=timezone.now)
    loan_facility = models.ForeignKey(LoanFacility, on_delete=models.CASCADE, verbose_name="Pledged Institution")
    script = models.CharField(max_length=20, verbose_name="Script")
    margin = models.FloatField(help_text="Margin % (e.g., 50)")
    remarks = models.CharField(max_length=200, blank=True)

    class Meta:
        ordering = ['-date']
        unique_together = ('loan_facility', 'script', 'date')

    def save(self, *args, **kwargs):
        self.script = self.script.upper()
        super().save(*args, **kwargs)

    def __str__(self):
        return f"{self.script} - {self.loan_facility.bank_name} ({self.margin}%)"


# --- 4. PLEDGE ENTRY SHEET & INVENTORY ---

class PledgeEntry(models.Model):
    ACTION_CHOICES = [
        ('BALANCE', 'Balance b/d'),
        ('PLEDGE', 'Pledge (Add)'),
        ('UNPLEDGE', 'Unpledged (Release)'),
    ]

    date = models.DateField(default=timezone.now)
    loan_facility = models.ForeignKey(LoanFacility, on_delete=models.CASCADE, related_name='pledge_entries', verbose_name="Pledged Institution")
    demat_account = models.ForeignKey(DematAccount, on_delete=models.CASCADE)
    symbol = models.CharField(max_length=20, verbose_name="Script")
    
    action = models.CharField(max_length=10, choices=ACTION_CHOICES)
    
    margin = models.FloatField(default=50, verbose_name="Margin %")
    kitta = models.PositiveIntegerField(verbose_name="Kitta (Qty)")
    
    average_closing_price = models.DecimalField(max_digits=10, decimal_places=2, default=0)
    closing_price = models.DecimalField(max_digits=10, decimal_places=2, default=0)
    
    utilized_loan = models.DecimalField(max_digits=15, decimal_places=2, default=0, help_text="Amount Utilized or Repaid")

    created_at = models.DateTimeField(auto_now_add=True)

    class Meta:
        ordering = ['-date', '-created_at']
        verbose_name = "Entry Sheet Row"

    @property
    def low_price(self):
        """Low (ACP or CP)"""
        return min(Decimal(self.average_closing_price), Decimal(self.closing_price))

    @property
    def drawing_power(self):
        """(Low Price * Kitta * Margin) / 100"""
        return (self.low_price * Decimal(self.kitta) * Decimal(self.margin)) / 100

    def save(self, *args, **kwargs):
        super().save(*args, **kwargs)
        # Trigger usage update on loan
        self.loan_facility.recalculate_usage()


class PledgedScrip(models.Model):
    """Inventory Snapshot"""
    loan_facility = models.ForeignKey(LoanFacility, on_delete=models.CASCADE, related_name='pledged_scrips')
    demat_account = models.ForeignKey(DematAccount, on_delete=models.CASCADE, null=True, blank=True)
    symbol = models.CharField(max_length=20) 
    quantity = models.PositiveIntegerField(default=0)
    
    # Valuation snapshot
    average_price = models.DecimalField(max_digits=10, decimal_places=2, default=0)
    closing_price = models.DecimalField(max_digits=10, decimal_places=2, default=0)
    valuation_percent = models.FloatField(default=50)
    allowable_drawing_power = models.DecimalField(max_digits=15, decimal_places=2, default=0)
    
    # Tracked from Entries
    utilized_amount = models.DecimalField(max_digits=15, decimal_places=2, default=0)

    def save(self, *args, **kwargs):
        base_price = min(Decimal(self.average_price), Decimal(self.closing_price))
        self.allowable_drawing_power = base_price * Decimal(self.quantity) * (Decimal(self.valuation_percent) / 100)
        super().save(*args, **kwargs)

    def __str__(self):
        return f"{self.symbol} ({self.quantity})"
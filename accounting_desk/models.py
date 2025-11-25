from django.db import models
from django.utils import timezone
from decimal import Decimal
import uuid

# --- External Imports ---
from listed_companies.models import Companies
from my_portfolio.models import DematAccount

# ==============================================================================
# 1. BANK FACILITY ENTRY SHEET
# ==============================================================================
class LoanFacility(models.Model):
    """
    Entry Sheet 1: Bank Facility Details
    Stores static details about the loan agreement.
    """
    bank_name = models.CharField(max_length=100, verbose_name="Bank Name")
    account_number = models.CharField(max_length=50, blank=True, verbose_name="Loan A/C No")
    sanctioned_limit = models.DecimalField(max_digits=15, decimal_places=2, default=0, verbose_name="Sanctioned Limit")
    
    start_date = models.DateField(default=timezone.now, verbose_name="Sanction Date")
    expiry_date = models.DateField(null=True, blank=True, verbose_name="Expiry Date")
    
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    class Meta:
        verbose_name = "1. Bank Facility Entry"
        verbose_name_plural = "1. Bank Facility Entries"
        ordering = ['bank_name']

    def __str__(self):
        return f"{self.bank_name} ({self.account_number})"


# ==============================================================================
# 2. INTEREST RATE ENTRY SHEET
# ==============================================================================
class LoanInterestHistory(models.Model):
    """
    Entry Sheet 2: Interest Rates
    Tracks historical changes in rates (Base Rate + Premium).
    """
    loan_facility = models.ForeignKey(LoanFacility, on_delete=models.CASCADE, related_name='interest_entries', verbose_name="Bank")
    effective_date = models.DateField(default=timezone.now, verbose_name="Effective Date")
    
    base_rate = models.DecimalField(max_digits=5, decimal_places=2, default=0, verbose_name="Base Rate %")
    premium = models.DecimalField(max_digits=5, decimal_places=2, default=0, verbose_name="Premium %")
    
    remarks = models.CharField(max_length=200, blank=True)
    created_at = models.DateTimeField(auto_now_add=True)

    class Meta:
        verbose_name = "2. Interest Rate Entry"
        verbose_name_plural = "2. Interest Rate Entries"
        ordering = ['-effective_date']

    @property
    def total_rate(self):
        return self.base_rate + self.premium

    def __str__(self):
        return f"{self.loan_facility} - {self.total_rate}%"


# ==============================================================================
# 3. MARGIN ENTRY SHEET
# ==============================================================================
class MarginRule(models.Model):
    """
    Entry Sheet 3: Margin Rules
    Defines the lending margin (e.g., 50%) for a specific script at a specific bank.
    """
    loan_facility = models.ForeignKey(LoanFacility, on_delete=models.CASCADE, related_name='margin_entries', verbose_name="Bank")
    symbol = models.ForeignKey(Companies, on_delete=models.CASCADE, to_field='script_ticker', verbose_name="Script")
    
    margin_percent = models.DecimalField(max_digits=5, decimal_places=2, default=50.00, verbose_name="Margin %")
    remarks = models.CharField(max_length=200, blank=True)
    
    updated_at = models.DateTimeField(auto_now=True)

    class Meta:
        verbose_name = "3. Margin Entry"
        verbose_name_plural = "3. Margin Entries"
        unique_together = ('loan_facility', 'symbol')
        ordering = ['symbol']

    def __str__(self):
        return f"{self.symbol} @ {self.loan_facility}: {self.margin_percent}%"


# ==============================================================================
# 4. PLEDGE ENTRY SHEET (Main Transaction Log)
# ==============================================================================
class PledgeEntrySheet(models.Model):
    """
    Entry Sheet 4: The Master Transaction Log.
    User enters data, system auto-calculates Drawing Power (DP).
    """
    
    ACTION_CHOICES = [
        ('BALANCE_BD', 'Balance b/d (Opening)'),
        ('PLEDGE', 'Pledge (Deposit)'),
        ('UNPLEDGE', 'Unpledge (Release)'),
        ('VALUATION', 'Valuation Update'), # Updates price without changing Kitta
    ]

    # --- A. User Inputs (Typed on Sheet) ---
    unique_id = models.CharField(max_length=50, unique=True, blank=True, verbose_name="Unique ID", help_text="Leave blank to auto-generate")
    date = models.DateField(default=timezone.now, verbose_name="Date")
    loan_facility = models.ForeignKey(LoanFacility, on_delete=models.CASCADE, related_name='pledge_entries', verbose_name="Bank")
    demat_account = models.ForeignKey(DematAccount, on_delete=models.SET_NULL, null=True, blank=True, verbose_name="DP / Demat")
    symbol = models.ForeignKey(Companies, on_delete=models.CASCADE, to_field='script_ticker', verbose_name="Script")
    action = models.CharField(max_length=20, choices=ACTION_CHOICES, verbose_name="Action")
    
    kitta = models.IntegerField(default=0, verbose_name="Kitta", help_text="Qty for this transaction")
    
    tx_utilized = models.DecimalField(max_digits=15, decimal_places=2, default=0, verbose_name="Utilized Amt", help_text="Loan amount Taken or Repaid")

    # --- B. Rendered Data (Fetched from System) ---
    # These are saved so historical records don't change if today's price changes
    tx_180_avg = models.DecimalField(max_digits=10, decimal_places=2, default=0, verbose_name="180 Days Avg")
    tx_closing_price = models.DecimalField(max_digits=10, decimal_places=2, default=0, verbose_name="Closing Price (CP)")
    tx_margin = models.DecimalField(max_digits=5, decimal_places=2, default=0, verbose_name="Margin %")

    # --- C. System Calculated (Auto-Filled on Save) ---
    tx_min_price = models.DecimalField(max_digits=10, decimal_places=2, default=0, verbose_name="Min Price") # Lower of 180 or CP
    tx_drawing_power = models.DecimalField(max_digits=15, decimal_places=2, default=0, verbose_name="Drawing Amt") # Calculated DP
    
    # --- D. Hidden Ledger State (Running Balance) ---
    # Stores the balance AFTER this transaction
    cl_kitta = models.IntegerField(default=0, editable=False)
    cl_drawing_power = models.DecimalField(max_digits=15, decimal_places=2, default=0, editable=False)
    cl_utilized = models.DecimalField(max_digits=15, decimal_places=2, default=0, editable=False)

    remarks = models.TextField(blank=True, null=True)
    created_at = models.DateTimeField(auto_now_add=True)

    class Meta:
        verbose_name = "4. Pledge Entry Sheet"
        verbose_name_plural = "4. Pledge Entry Sheets"
        ordering = ['-date', '-created_at']

    def save(self, *args, **kwargs):
        # 1. Auto-Generate Unique ID if missing
        if not self.unique_id:
            self.unique_id = f"PE-{self.date.strftime('%Y%m%d')}-{str(uuid.uuid4())[:6].upper()}"

        # 2. Calculate Min Price (Logic: Min of 180 Avg and CP)
        # Avoid zero division or errors if data is missing
        p1 = self.tx_180_avg if self.tx_180_avg else Decimal(0)
        p2 = self.tx_closing_price if self.tx_closing_price else Decimal(0)
        
        if p1 > 0 and p2 > 0:
            self.tx_min_price = min(p1, p2)
        else:
            self.tx_min_price = max(p1, p2) # If one is missing, take the other

        # 3. Calculate Drawing Amount (Transaction Level)
        # Formula: Kitta * MinPrice * Margin%
        if self.kitta > 0 and self.tx_min_price > 0:
            valuation = Decimal(self.kitta) * self.tx_min_price
            self.tx_drawing_power = valuation * (self.tx_margin / Decimal(100))
        else:
            self.tx_drawing_power = Decimal(0)

        # 4. Calculate Running Balances (Closing State)
        # We need the PREVIOUS closing balance to calculate THIS closing balance.
        # Note: This simple logic assumes entries are added sequentially. 
        prev = self.get_previous_entry()
        op_kitta = prev.cl_kitta if prev else 0
        op_dp = prev.cl_drawing_power if prev else Decimal(0)
        op_util = prev.cl_utilized if prev else Decimal(0)

        if self.action == 'BALANCE_BD':
            self.cl_kitta = self.kitta
            self.cl_drawing_power = self.tx_drawing_power
            self.cl_utilized = self.tx_utilized

        elif self.action == 'PLEDGE':
            self.cl_kitta = op_kitta + self.kitta
            self.cl_drawing_power = op_dp + self.tx_drawing_power
            self.cl_utilized = op_util + self.tx_utilized

        elif self.action == 'UNPLEDGE':
            self.cl_kitta = max(0, op_kitta - self.kitta)
            self.cl_drawing_power = max(Decimal(0), op_dp - self.tx_drawing_power)
            self.cl_utilized = max(Decimal(0), op_util - self.tx_utilized)

        elif self.action == 'VALUATION':
            self.cl_kitta = op_kitta
            # For valuation, we recalculate DP for the WHOLE inventory based on NEW prices
            total_valuation = Decimal(self.cl_kitta) * self.tx_min_price
            self.cl_drawing_power = total_valuation * (self.tx_margin / Decimal(100))
            self.cl_utilized = op_util # Usage doesn't usually change on valuation
            
            # For display purposes, tx_drawing_power in valuation shows the Difference (Gain/Loss)
            self.tx_drawing_power = self.cl_drawing_power - op_dp

        super().save(*args, **kwargs)

    def get_previous_entry(self):
        """Finds the most recent entry before this one for the same Bank & Script."""
        return PledgeEntrySheet.objects.filter(
            loan_facility=self.loan_facility,
            symbol=self.symbol,
            created_at__lt=self.created_at if self.pk else timezone.now()
        ).order_by('-date', '-created_at').first()
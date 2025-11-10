# nepse_data/models.py
from django.db import models

class StockPrices(models.Model):
    # Django will add an 'id' field automatically as a primary key
    business_date = models.DateField()
    security_id = models.CharField(max_length=20)
    symbol = models.CharField(max_length=20)
    security_name = models.CharField(max_length=255)
    open_price = models.DecimalField(max_digits=10, decimal_places=2, blank=True, null=True)
    high_price = models.DecimalField(max_digits=10, decimal_places=2, blank=True, null=True)
    low_price = models.DecimalField(max_digits=10, decimal_places=2, blank=True, null=True)
    close_price = models.DecimalField(max_digits=10, decimal_places=2, blank=True, null=True)
    total_traded_quantity = models.BigIntegerField(blank=True, null=True)
    total_traded_value = models.DecimalField(max_digits=15, decimal_places=2, blank=True, null=True)
    previous_close = models.DecimalField(max_digits=10, decimal_places=2, blank=True, null=True)
    fifty_two_week_high = models.DecimalField(max_digits=10, decimal_places=2, blank=True, null=True)
    fifty_two_week_low = models.DecimalField(max_digits=10, decimal_places=2, blank=True, null=True)
    last_updated_time = models.CharField(max_length=50, blank=True, null=True)
    last_updated_price = models.DecimalField(max_digits=10, decimal_places=2, blank=True, null=True)
    total_trades = models.IntegerField(blank=True, null=True)
    average_traded_price = models.DecimalField(max_digits=10, decimal_places=2, blank=True, null=True)
    market_capitalization = models.DecimalField(max_digits=20, decimal_places=2, blank=True, null=True)

    class Meta:
        db_table = 'stock_prices'
        unique_together = (('business_date', 'security_id'),)
        verbose_name_plural = 'Stock Prices'

    def __str__(self):
        return f"{self.symbol} on {self.business_date}"

# --- THIS IS THE FIXED MODEL ---
class Indices(models.Model):
    id = models.AutoField(primary_key=True) 
    sn = models.BigIntegerField(blank=True, null=True)
    date = models.DateField(blank=True, null=True)
    
    # --- THESE FIELDS ARE NOW FIXED (DecimalField) ---
    open = models.DecimalField(max_digits=14, decimal_places=2, blank=True, null=True)
    high = models.DecimalField(max_digits=14, decimal_places=2, blank=True, null=True)
    low = models.DecimalField(max_digits=14, decimal_places=2, blank=True, null=True)
    close = models.DecimalField(max_digits=14, decimal_places=2, blank=True, null=True)
    absolute_change = models.DecimalField(max_digits=14, decimal_places=2, blank=True, null=True)
    # --- END OF FIX ---

    percentage_change = models.TextField(blank=True, null=True)
    
    # --- THESE FIELDS ARE ALSO FIXED (DecimalField) ---
    number_52_weeks_high = models.DecimalField(max_digits=14, decimal_places=2, db_column='52_weeks_high', blank=True, null=True)
    number_52_weeks_low = models.DecimalField(max_digits=14, decimal_places=2, db_column='52_weeks_low', blank=True, null=True)
    turnover_values = models.DecimalField(max_digits=20, decimal_places=2, blank=True, null=True)
    # --- END OF FIX ---
    
    turnover_volume = models.BigIntegerField(blank=True, null=True)
    total_transaction = models.BigIntegerField(blank=True, null=True)
    sector = models.TextField(blank=True, null=True)
    created_at = models.DateTimeField(auto_now_add=True, null=True) 

    class Meta:
        db_table = 'indices'
        verbose_name_plural = 'Indices'

    def __str__(self):
        return f"{self.sector} on {self.date}"
# --- END OF FIX ---

class Marcap(models.Model):
    id = models.AutoField(primary_key=True)
    sn = models.IntegerField(blank=True, null=True)
    business_date = models.DateField(unique=True)
    market_capitalization = models.DecimalField(max_digits=20, decimal_places=2, blank=True, null=True)
    sensitive_market_capitalization = models.DecimalField(max_digits=20, decimal_places=2, blank=True, null=True)
    float_market_capitalization = models.DecimalField(max_digits=20, decimal_places=2, blank=True, null=True)
    sensitive_float_market_capitalization = models.DecimalField(max_digits=20, decimal_places=2, blank=True, null=True)

    # --- NEW COLUMNS ADDED ---
    total_turnover = models.DecimalField(max_digits=20, decimal_places=2, blank=True, null=True)
    total_traded_shares = models.BigIntegerField(blank=True, null=True)
    total_transactions = models.BigIntegerField(blank=True, null=True)
    total_scrips_traded = models.BigIntegerField(blank=True, null=True)
    # --- END OF NEW COLUMNS ---

    created_at = models.DateTimeField(auto_now_add=True, null=True)

    class Meta:
        db_table = 'marcap'
        verbose_name = 'Market Cap'
        verbose_name_plural = 'Market Caps'

    def __str__(self):
        return f"Market Cap on {self.business_date}"
    

class FloorsheetRaw(models.Model):
    # We need a primary key for Django, but the DB uses contract_no
    # We will use the 'id' from your generated_models.py
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
        managed = False  # <-- Tells Django to *not* create or change this table
        db_table = 'floorsheet_raw'
        verbose_name = 'Floorsheet (Raw)'
        verbose_name_plural = 'Floorsheet (Raw)'

    def __str__(self):
        return f"{self.stock_symbol} ({self.contract_no})"
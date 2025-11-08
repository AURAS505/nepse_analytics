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

class Indices(models.Model):
    # Django will add an 'id' field automatically
    sn = models.BigIntegerField(blank=True, null=True)
    date = models.DateTimeField(blank=True, null=True)
    open = models.FloatField(blank=True, null=True)
    high = models.FloatField(blank=True, null=True)
    low = models.FloatField(blank=True, null=True)
    close = models.FloatField(blank=True, null=True)
    absolute_change = models.FloatField(blank=True, null=True)
    percentage_change = models.TextField(blank=True, null=True)
    number_52_weeks_high = models.FloatField(db_column='52_weeks_high', blank=True, null=True)
    number_52_weeks_low = models.FloatField(db_column='52_weeks_low', blank=True, null=True)
    turnover_values = models.FloatField(blank=True, null=True)
    turnover_volume = models.BigIntegerField(blank=True, null=True)
    total_transaction = models.BigIntegerField(blank=True, null=True)
    sector = models.TextField(blank=True, null=True)

    class Meta:
        db_table = 'indices'
        verbose_name_plural = 'Indices'

    def __str__(self):
        return f"{self.sector} on {self.date}"

class Marcap(models.Model):
    # Django will add an 'id' field automatically
    sn = models.IntegerField(blank=True, null=True)
    business_date = models.DateField(unique=True)
    market_capitalization = models.DecimalField(max_digits=20, decimal_places=2, blank=True, null=True)
    sensitive_market_capitalization = models.DecimalField(max_digits=20, decimal_places=2, blank=True, null=True)
    float_market_capitalization = models.DecimalField(max_digits=20, decimal_places=2, blank=True, null=True)
    sensitive_float_market_capitalization = models.DecimalField(max_digits=20, decimal_places=2, blank=True, null=True)
    created_at = models.DateTimeField(blank=True, null=True)

    class Meta:
        db_table = 'marcap'
        verbose_name = 'Market Cap'
        verbose_name_plural = 'Market Caps'

    def __str__(self):
        return f"Market Cap on {self.business_date}"
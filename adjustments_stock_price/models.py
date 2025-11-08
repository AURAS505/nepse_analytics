# adjustments_stock_price/models.py
from django.db import models
# Import the Companies model from your other app
from listed_companies.models import Companies

class PriceAdjustments(models.Model):
    # Django will add an 'id' field automatically
    adjustment_date = models.DateTimeField(blank=True, null=True)
    
    # This is better than just a CharField
    symbol = models.ForeignKey(
        Companies, 
        on_delete=models.DO_NOTHING, 
        db_column='symbol', 
        to_field='script_ticker'
    )
    
    book_close_date = models.DateField()
    adjustment_type = models.CharField(max_length=10)
    adjustment_percent = models.DecimalField(max_digits=10, decimal_places=4)
    par_value = models.DecimalField(max_digits=10, decimal_places=2)
    records_adjusted = models.IntegerField(blank=True, null=True)
    adjustment_factor = models.DecimalField(max_digits=18, decimal_places=10, blank=True, null=True)

    class Meta:
        db_table = 'price_adjustments'
        verbose_name = 'Price Adjustment'
        verbose_name_plural = 'Price Adjustments'

    def __str__(self):
        return f"{self.symbol} - {self.adjustment_type} on {self.book_close_date}"

class StockPricesAdj(models.Model):
    id = models.IntegerField(primary_key=True)
    business_date = models.DateField()
    security_id = models.IntegerField(blank=True, null=True)
    symbol = models.CharField(max_length=20)
    security_name = models.CharField(max_length=255, blank=True, null=True)
    open_price = models.DecimalField(max_digits=14, decimal_places=2, blank=True, null=True)
    high_price = models.DecimalField(max_digits=14, decimal_places=2, blank=True, null=True)
    low_price = models.DecimalField(max_digits=14, decimal_places=2, blank=True, null=True)
    close_price = models.DecimalField(max_digits=14, decimal_places=2, blank=True, null=True)
    open_price_adj = models.DecimalField(max_digits=14, decimal_places=2, blank=True, null=True)
    high_price_adj = models.DecimalField(max_digits=14, decimal_places=2, blank=True, null=True)
    low_price_adj = models.DecimalField(max_digits=14, decimal_places=2, blank=True, null=True)
    close_price_adj = models.DecimalField(max_digits=14, decimal_places=2, blank=True, null=True)
    adjustment_factor = models.DecimalField(max_digits=18, decimal_places=10, blank=True, null=True)
    average_traded_price_adj = models.DecimalField(max_digits=14, decimal_places=2, blank=True, null=True)
    fifty_two_week_high_adj = models.DecimalField(max_digits=14, decimal_places=2, blank=True, null=True)
    fifty_two_week_low_adj = models.DecimalField(max_digits=14, decimal_places=2, blank=True, null=True)

    class Meta:
        db_table = 'stock_prices_adj'
        unique_together = (('symbol', 'business_date'),)
        verbose_name = 'Adjusted Stock Price'
        verbose_name_plural = 'Adjusted Stock Prices'

    def __str__(self):
        return f"{self.symbol} (Adj) on {self.business_date}"
from django.db import models
from adjustments_stock_price.models import StockPricesAdj
from listed_companies.models import Companies

class IndicatorType(models.Model):
    """Metadata for different technical indicators"""
    CATEGORY_CHOICES = [
        ('TREND', 'Trend'),
        ('MOMENTUM', 'Momentum'),
        ('VOLATILITY', 'Volatility'),
        ('VOLUME', 'Volume'),
    ]
    
    name = models.CharField(max_length=50, unique=True)  # RSI, MACD, SMA_20
    display_name = models.CharField(max_length=100)
    category = models.CharField(max_length=20, choices=CATEGORY_CHOICES)
    description = models.TextField()
    default_parameters = models.JSONField()  # {"period": 14, "overbought": 70}
    is_active = models.BooleanField(default=True)
    
    class Meta:
        db_table = 'indicator_types'
        
    def __str__(self):
        return self.display_name


class IndicatorValue(models.Model):
    """Stores calculated indicator values"""
    symbol = models.CharField(max_length=20, db_index=True)  # Match with StockPricesAdj
    indicator_type = models.ForeignKey(IndicatorType, on_delete=models.CASCADE)
    business_date = models.DateField(db_index=True)
    
    # Flexible storage for different indicators
    value = models.DecimalField(max_digits=18, decimal_places=6, null=True)
    value_json = models.JSONField(null=True)  # For multi-value indicators like MACD
    
    parameters = models.JSONField()  # Parameters used for this calculation
    
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)
    
    class Meta:
        db_table = 'indicator_values'
        unique_together = [['symbol', 'indicator_type', 'business_date']]
        indexes = [
            models.Index(fields=['symbol', 'business_date']),
            models.Index(fields=['indicator_type', 'business_date']),
        ]
        ordering = ['-business_date']
        
    def __str__(self):
        return f"{self.symbol} - {self.indicator_type.name} on {self.business_date}"


class IndicatorCache(models.Model):
    """Cache frequently accessed indicator data"""
    symbol = models.CharField(max_length=20)
    indicator_type = models.ForeignKey(IndicatorType, on_delete=models.CASCADE)
    timeframe = models.CharField(max_length=20)  # daily, weekly, monthly
    data = models.JSONField()  # Cached calculations
    last_updated = models.DateTimeField(auto_now=True)
    
    class Meta:
        db_table = 'indicator_cache'
        unique_together = [['symbol', 'indicator_type', 'timeframe']]
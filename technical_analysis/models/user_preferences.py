from django.contrib.auth.models import User
from django.db import models
from adjustments_stock_price.models import StockPricesAdj
from listed_companies.models import Companies

class Watchlist(models.Model):
    """User watchlists"""
    user = models.ForeignKey(User, on_delete=models.CASCADE, related_name='watchlists')
    name = models.CharField(max_length=100)
    description = models.TextField(blank=True)
    
    symbols = models.JSONField(default=list)  # List of symbols
    
    is_default = models.BooleanField(default=False)
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)
    
    class Meta:
        db_table = 'user_watchlists'
        unique_together = [['user', 'name']]


class PriceAlert(models.Model):
    """Price alerts for users"""
    ALERT_TYPE_CHOICES = [
        ('PRICE_ABOVE', 'Price Above'),
        ('PRICE_BELOW', 'Price Below'),
        ('CHANGE_PERCENT', 'Change Percent'),
        ('VOLUME_SPIKE', 'Volume Spike'),
        ('RSI_OVERBOUGHT', 'RSI Overbought'),
        ('RSI_OVERSOLD', 'RSI Oversold'),
        ('MACD_CROSSOVER', 'MACD Crossover'),
        ('MA_CROSSOVER', 'MA Crossover'),
    ]
    
    user = models.ForeignKey(User, on_delete=models.CASCADE, related_name='alerts')
    symbol = models.CharField(max_length=20, db_index=True)
    
    alert_type = models.CharField(max_length=30, choices=ALERT_TYPE_CHOICES)
    condition_value = models.DecimalField(max_digits=14, decimal_places=2)
    
    is_active = models.BooleanField(default=True)
    is_triggered = models.BooleanField(default=False)
    
    triggered_at = models.DateTimeField(null=True)
    triggered_price = models.DecimalField(max_digits=14, decimal_places=2, null=True)
    
    notification_sent = models.BooleanField(default=False)
    
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)
    
    class Meta:
        db_table = 'price_alerts'
        indexes = [
            models.Index(fields=['user', 'is_active']),
            models.Index(fields=['symbol', 'is_active']),
        ]


class TechnicalScan(models.Model):
    """Saved technical scans/screeners"""
    user = models.ForeignKey(User, on_delete=models.CASCADE, related_name='technical_scans')
    name = models.CharField(max_length=100)
    
    # Scan criteria
    criteria = models.JSONField()  # Store filter conditions
    
    # Scan results (cached)
    last_results = models.JSONField(null=True)
    last_run = models.DateTimeField(null=True)
    
    is_active = models.BooleanField(default=True)
    created_at = models.DateTimeField(auto_now_add=True)
    
    class Meta:
        db_table = 'technical_scans'
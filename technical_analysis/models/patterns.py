from django.db import models
from adjustments_stock_price.models import StockPricesAdj
from listed_companies.models import Companies

class ChartPattern(models.Model):
    """Detected chart patterns"""
    PATTERN_CHOICES = [
        # Candlestick Patterns
        ('DOJI', 'Doji'),
        ('HAMMER', 'Hammer'),
        ('SHOOTING_STAR', 'Shooting Star'),
        ('ENGULFING_BULL', 'Bullish Engulfing'),
        ('ENGULFING_BEAR', 'Bearish Engulfing'),
        ('MORNING_STAR', 'Morning Star'),
        ('EVENING_STAR', 'Evening Star'),
        
        # Chart Patterns
        ('HEAD_SHOULDERS', 'Head and Shoulders'),
        ('INVERSE_HEAD_SHOULDERS', 'Inverse Head and Shoulders'),
        ('DOUBLE_TOP', 'Double Top'),
        ('DOUBLE_BOTTOM', 'Double Bottom'),
        ('TRIANGLE_ASCENDING', 'Ascending Triangle'),
        ('TRIANGLE_DESCENDING', 'Descending Triangle'),
        ('WEDGE_RISING', 'Rising Wedge'),
        ('WEDGE_FALLING', 'Falling Wedge'),
        ('FLAG_BULL', 'Bullish Flag'),
        ('FLAG_BEAR', 'Bearish Flag'),
    ]
    
    SENTIMENT_CHOICES = [
        ('BULLISH', 'Bullish'),
        ('BEARISH', 'Bearish'),
        ('NEUTRAL', 'Neutral'),
    ]
    
    symbol = models.CharField(max_length=20, db_index=True)
    pattern_type = models.CharField(max_length=50, choices=PATTERN_CHOICES)
    sentiment = models.CharField(max_length=20, choices=SENTIMENT_CHOICES)
    
    detected_date = models.DateField(db_index=True)
    start_date = models.DateField()
    end_date = models.DateField(null=True)
    
    confidence = models.DecimalField(max_digits=5, decimal_places=2)  # 0-100%
    
    # Pattern specific data
    pattern_data = models.JSONField()  # Store pattern coordinates, breakout levels
    
    is_completed = models.BooleanField(default=False)
    breakout_confirmed = models.BooleanField(default=False)
    breakout_date = models.DateField(null=True)
    
    created_at = models.DateTimeField(auto_now_add=True)
    
    class Meta:
        db_table = 'chart_patterns'
        indexes = [
            models.Index(fields=['symbol', '-detected_date']),
        ]


class SupportResistanceLevel(models.Model):
    """Support and Resistance levels"""
    LEVEL_TYPE_CHOICES = [
        ('SUPPORT', 'Support'),
        ('RESISTANCE', 'Resistance'),
    ]
    
    symbol = models.CharField(max_length=20, db_index=True)
    level_type = models.CharField(max_length=20, choices=LEVEL_TYPE_CHOICES)
    
    price_level = models.DecimalField(max_digits=14, decimal_places=2)
    strength = models.IntegerField()  # 1-10, based on touch points
    
    first_touched = models.DateField()
    last_touched = models.DateField()
    touch_count = models.IntegerField(default=1)
    
    is_active = models.BooleanField(default=True)
    broken_date = models.DateField(null=True)
    
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)
    
    class Meta:
        db_table = 'support_resistance_levels'
        indexes = [
            models.Index(fields=['symbol', 'is_active']),
        ]
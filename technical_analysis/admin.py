from django.contrib import admin
from .models import (
    IndicatorType, IndicatorValue, 
    Signal, TradingStrategy,
    ChartPattern, SupportResistanceLevel,
    Watchlist, PriceAlert
)

@admin.register(IndicatorType)
class IndicatorTypeAdmin(admin.ModelAdmin):
    list_display = ['name', 'category', 'is_active']
    list_filter = ['category', 'is_active']

@admin.register(IndicatorValue)
class IndicatorValueAdmin(admin.ModelAdmin):
    list_display = ['symbol', 'indicator_type', 'business_date', 'value']
    list_filter = ['indicator_type', 'business_date']
    search_fields = ['symbol']
    date_hierarchy = 'business_date'

@admin.register(Signal)
class SignalAdmin(admin.ModelAdmin):
    list_display = ['symbol', 'signal_type', 'strategy', 'strength', 'business_date']
    list_filter = ['signal_type', 'strategy', 'business_date']
    search_fields = ['symbol']

@admin.register(TradingStrategy)
class TradingStrategyAdmin(admin.ModelAdmin):
    list_display = ['name', 'strategy_type', 'is_active', 'risk_level']
    list_filter = ['is_active', 'risk_level']

@admin.register(ChartPattern)
class ChartPatternAdmin(admin.ModelAdmin):
    list_display = ['symbol', 'pattern_type', 'sentiment', 'detected_date', 'confidence']
    list_filter = ['pattern_type', 'sentiment', 'detected_date']
    search_fields = ['symbol']

@admin.register(SupportResistanceLevel)
class SupportResistanceLevelAdmin(admin.ModelAdmin):
    list_display = ['symbol', 'level_type', 'price_level', 'strength', 'touch_count']
    list_filter = ['level_type', 'is_active']
    search_fields = ['symbol']

@admin.register(Watchlist)
class WatchlistAdmin(admin.ModelAdmin):
    list_display = ['user', 'name', 'is_default', 'created_at']
    list_filter = ['is_default']
    search_fields = ['user__username', 'name']

@admin.register(PriceAlert)
class PriceAlertAdmin(admin.ModelAdmin):
    list_display = ['user', 'symbol', 'alert_type', 'is_active', 'is_triggered']
    list_filter = ['alert_type', 'is_active', 'is_triggered']
    search_fields = ['user__username', 'symbol']
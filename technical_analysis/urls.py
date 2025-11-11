from django.urls import path
from . import views

app_name = 'technical_analysis'

urlpatterns = [
    # Main Dashboard
    path('', views.dashboard, name='dashboard'),
    
    # Analysis Tools
    path('charts/', views.chart_viewer, name='chart_viewer'),
    path('charts/<str:symbol>/', views.stock_chart, name='stock_chart'),
    path('screener/', views.stock_screener, name='stock_screener'),
    path('signals/', views.trading_signals, name='signals'),
    path('signals/<str:symbol>/', views.stock_signals, name='stock_signals'),
    path('patterns/', views.pattern_recognition, name='patterns'),
    path('patterns/<str:symbol>/', views.stock_patterns, name='stock_patterns'),
    path('support-resistance/', views.support_resistance, name='support_resistance'),
    path('support-resistance/<str:symbol>/', views.sr_levels, name='sr_levels'),
    path('backtest/', views.backtest_strategy, name='backtest'),
    
    # Technical Indicators - Individual Pages
    path('indicators/<str:indicator_name>/', views.indicator_detail, name='indicator_detail'),
    path('indicators/<str:indicator_name>/<str:symbol>/', views.indicator_symbol, name='indicator_symbol'),
    
    # Advanced Features
    path('scanner/', views.market_scanner, name='market_scanner'),
    path('watchlist/', views.watchlist_manager, name='watchlist'),
    path('alerts/', views.alert_center, name='alerts'),
    path('sector-analysis/', views.sector_analysis, name='sector_analysis'),
    path('strategy-builder/', views.strategy_builder, name='strategy_builder'),
    path('performance/', views.performance_tracker, name='performance'),
    path('tutorial/', views.tutorial, name='tutorial'),
    
    # API Endpoints (for AJAX/React)
    path('api/calculate-indicator/', views.calculate_indicator_api, name='api_calculate_indicator'),
    path('api/get-ohlcv/<str:symbol>/', views.get_ohlcv_api, name='api_get_ohlcv'),
    path('api/screener-results/', views.screener_results_api, name='api_screener_results'),
    path('api/signals-data/', views.signals_data_api, name='api_signals_data'),
]
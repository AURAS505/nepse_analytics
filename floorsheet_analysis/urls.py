# In: floorsheet_analysis/urls.py

from django.urls import path
from . import views

app_name = 'floorsheet_analysis'  

urlpatterns = [
    path('', views.settlement_report, name='settlement_report'),
    path('company_trades/', views.company_trades_report, name='company_trades_report'),
    
    # --- THIS IS THE CORRECTED LINE ---
    path('broker_trades/', views.broker_trades_report, name='broker_trades_report'),
    
    path('daily_holdings/', views.stock_holding_history_report, name='stock_holding_history_report'),
    
    # API URLs
    path('api/stock_summary/', views.api_stock_summary_report, name='api_stock_summary'),
    path('api/broker_sector_details/', views.broker_sector_details, name='broker_sector_details'),
    path('api/broker_script_details/', views.broker_script_details, name='broker_script_details'),
    path('api/get_floorsheet_details/', views.get_floorsheet_details, name='get_floorsheet_details'),
]
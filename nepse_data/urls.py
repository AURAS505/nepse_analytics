from django.urls import path
from . import views

app_name = 'nepse_data' 

urlpatterns = [
    path('todays-price/', views.todays_price_view, name='todays_price'),
    path('download-stock-prices/', views.download_stock_prices_view, name='download_stock_prices'),
    
    path('data-entry/', views.data_entry_view, name='data_entry'),
    path('data-entry/delete/', views.delete_price_data_view, name='delete_price_data'),

    path('indices/', views.indices_view, name='indices'),
    path('download-indices/', views.download_indices_view, name='download_indices'),
    
    # --- THESE ARE THE MISSING LINES ---


    
    path('market-cap/', views.market_cap_view, name='market_cap'),
    path('download-marcap/', views.download_marcap_view, name='download_marcap'),
    path('floorsheet/', views.floorsheet_view, name='floorsheet'),
    path('download-floorsheet/', views.download_floorsheet_view, name='download_floorsheet'),
    path('data-entry/delete-floorsheet/', views.delete_floorsheet_data_view, name='delete_floorsheet_data'),
    path('download-dividend-history/', views.download_dividend_history_view, name='download_dividend_history'),
    path('dividend-history/', views.dividend_history_view, name='dividend_history'),
    path('download-dividend-sample/', views.download_dividend_sample_view, name='download_dividend_sample'),
    path('dividend/edit/<int:pk>/', views.edit_dividend_view, name='edit_dividend'),
    path('dividend/delete/<int:pk>/', views.delete_dividend_view, name='delete_dividend'),
    path('dividend/delete-all/', views.delete_all_dividends_view, name='delete_all_dividends'),
    path('dividend/add/', views.add_dividend_view, name='add_dividend'),
    path('company-lookup/', views.company_lookup_json_view, name='company_lookup_json'),
    path('dividend/search/', views.search_dividends_json_view, name='search_dividends_json'),
    
    
    
    
    # --- END OF FIX ---

]
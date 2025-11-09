# nepse_data/urls.py
from django.urls import path
from . import views

# This sets the namespace for the app, so we can use {% url 'nepse_data:todays_price' %}
app_name = 'nepse_data' 

urlpatterns = [
    # This will be the page for /data/todays-price/
    path('todays-price/', views.todays_price_view, name='todays_price'),
        # Add this new line for the download
    path('download-stock-prices/', views.download_stock_prices_view, name='download_stock_prices'),

    # --- ADD THESE TWO LINES ---
    path('data-entry/', views.data_entry_view, name='data_entry'),
    path('data-entry/delete/', views.delete_price_data_view, name='delete_price_data'),

    # We will add these soon
    # path('indices/', views.indices_view, name='indices'),
    # path('market-cap/', views.market_cap_view, name='market_cap'),
    # path('data-entry/', views.data_entry_view, name='data_entry'),
]
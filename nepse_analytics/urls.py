# nepse_analytics/nepse_analytics/urls.py
from django.contrib import admin
from django.urls import path, include

urlpatterns = [
    path('admin/', admin.site.urls),
    
    # These are working because we created their urls.py files
    path('', include('core.urls')), 
    path('data/', include('nepse_data.urls')), 

    # --- We will build these later ---
    # Comment these out for now, because their urls.py files don't exist yet
    path('companies/', include('listed_companies.urls')),
    path('adjustments/', include('adjustments_stock_price.urls')),
    # path('analysis/', include('statistical_analysis.urls')),
    path('floorsheet/', include('floorsheet_analysis.urls')),
]
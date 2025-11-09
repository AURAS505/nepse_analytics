# adjustments_stock_price/urls.py
from django.urls import path
from . import views

app_name = 'adjustments_stock_price'

urlpatterns = [
    # This is the link from the 'todays_price' page
    path('', views.adjustment_tool_view, name='index'),

    # These are for the recalculation progress bar
    path('start-recalc/', views.start_recalc_view, name='start_recalc'),
    path('recalc-status/<str:job_id>/', views.recalc_status_view, name='recalc_status'),
    path('clear-job/<str:job_id>/', views.clear_job_view, name='clear_job'),
]
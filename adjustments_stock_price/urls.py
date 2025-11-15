# adjustments_stock_price/urls.py
from django.urls import path
from . import views

app_name = 'adjustments_stock_price'

urlpatterns = [
    # Main page (List and Add)
    path('', views.adjustment_tool_view, name='index'),

    # Edit/Delete/View
    path('edit/<int:adj_id>/', views.edit_adjustment_view, name='edit'),
    path('delete/<int:adj_id>/', views.delete_adjustment_view, name='delete'),
    path('view/<str:symbol>/', views.view_adjustments_view, name='view'),

    # Bulk Upload (This is the missing line)
    path('bulk-upload/', views.bulk_upload_adjustments_view, name='bulk_upload'),
    path('download-sample/', views.download_adjustment_sample_csv_view, name='download_sample_csv'),

    # API for form
    path('api/company/<str:symbol>/', views.get_company_name_view, name='get_company_name'),

    # API for background task
    path('start-recalc/', views.start_recalc_view, name='start_recalc'),
    path('recalc-status/<str:job_id>/', views.recalc_status_view, name='recalc_status'),
    path('clear-job/<str:job_id>/', views.clear_job_view, name='clear_job'),
    path('adjustments/clear-job/<str:job_id>/', views.clear_job_view, name='clear_job'),
]
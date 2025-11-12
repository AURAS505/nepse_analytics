# my_portfolio/urls.py
from django.urls import path
from . import views

app_name = 'my_portfolio'

urlpatterns = [
    # /portfolio/
    path('', views.portfolio_home, name='portfolio_home'),
    
    # /portfolio/transactions/
    path('transactions/', views.transaction_list_and_add, name='transactions'),
    
    # /portfolio/transactions/edit/YYYYMMDD-XXXXXX/
    path('transactions/edit/<str:unique_id>/', views.transaction_edit, name='transaction_edit'),
    
    # /portfolio/transactions/delete/YYYYMMDD-XXXXXX/
    path('transactions/delete/<str:unique_id>/', views.transaction_delete, name='transaction_delete'),
    
    # /portfolio/transactions/delete_all/
    path('transactions/delete_all/', views.transaction_delete_all, name='transaction_delete_all'),
    
    # /portfolio/transactions/upload/
    path('transactions/upload/', views.transaction_upload, name='transaction_upload'),
    
    # /portfolio/transactions/download_template/csv/
    path('transactions/download_template/<str:file_type>/', 
         views.download_transaction_template, 
         name='download_transaction_template'),
    
    # /portfolio/report/
     path('company_dashboard/', views.company_dashboard, name='company_dashboard'),

    # --- API Endpoints ---
    
    # /portfolio/api/company_details/HBL/
    path('api/company_details/<str:symbol>/', views.api_company_details, name='api_company_details'),
]
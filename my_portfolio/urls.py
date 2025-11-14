# my_portfolio/urls.py
from django.urls import path
from . import views

app_name = 'my_portfolio'

urlpatterns = [
    # /portfolio/
    path('', views.portfolio_home, name='portfolio_home'),
    
    # --- Stock Transactions ---
    path('transactions/', views.transaction_list_and_add, name='transactions'),
    path('transactions/edit/<str:unique_id>/', views.transaction_edit, name='transaction_edit'),
    path('transactions/delete/<str:unique_id>/', views.transaction_delete, name='transaction_delete'),
    path('transactions/delete_all/', views.transaction_delete_all, name='transaction_delete_all'),
    path('transactions/upload/', views.transaction_upload, name='transaction_upload'),
    path('transactions/download_template/<str:file_type>/', 
         views.download_transaction_template, 
         name='download_transaction_template'),
    
    # --- ADD THESE NEW BROKER TRANSACTION URLS ---
    # --- Broker Transaction URLs ---
    path('broker_transactions/', 
         views.broker_transaction_list_and_add, 
         name='broker_transactions'),
    
    path('broker_transactions/edit/<str:unique_id>/', 
         views.broker_transaction_edit, 
         name='broker_transaction_edit'),
    
    path('broker_transactions/delete/<str:unique_id>/', 
         views.broker_transaction_delete, 
         name='broker_transaction_delete'),
    
    # --- ADD THIS NEW URL FOR UPLOAD ---
    path('broker_transactions/upload/', 
         views.broker_transaction_upload, 
         name='broker_transaction_upload'),
         
    # --- ADD THIS NEW URL FOR TEMPLATE DOWNLOAD ---
    path('broker_transactions/download_template/csv/', 
         views.download_broker_template, 
         name='download_broker_template'),
    # --- END ADD ---

    # --- Reports ---
    path('company_dashboard/', views.company_dashboard, name='company_dashboard'),
    path('report/valuation/', views.valuation_report, name='valuation_report'),
    path('report/valuation/download/', views.download_valuation_report, name='download_valuation_report'),

    # --- API Endpoints ---
    path('api/company_details/<str:symbol>/', views.api_company_details, name='api_company_details'),
    path('report/broker_ledger/', views.broker_ledger_report, name='broker_ledger_report'),
    
]
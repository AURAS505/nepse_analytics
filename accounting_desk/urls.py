from django.urls import path
from . import views

app_name = 'accounting_desk'

urlpatterns = [
    path('', views.accounting_dashboard, name='dashboard'),
    
    # --- ADD THIS LINE ---
    path('loans/', views.bank_loan_report, name='bank_loan_report'),
    path('api/get-scrip-info/', views.get_scrip_info, name='get_scrip_info'),
]
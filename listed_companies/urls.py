# listed_companies/urls.py
from django.urls import path
from . import views

app_name = 'listed_companies'

urlpatterns = [
    # ========================================
    # EXISTING URLs - Keep unchanged
    # ========================================
    
    # /companies/
    path('', views.listed_company_view, name='list'),

    # /companies/add/
    path('add/', views.add_company_view, name='add'),

    # /companies/edit/NABIL/
    path('edit/<str:nepse_code>/', views.edit_company_view, name='edit'),

    # /companies/delete/NABIL/
    path('delete/<str:nepse_code>/', views.delete_company_view, name='delete'),

    # /companies/delete-all/
    path('delete-all/', views.delete_all_companies_view, name='delete_all'),

    # /companies/upload/
    path('upload/', views.upload_companies_view, name='upload'),

    # /companies/download/
    path('download/', views.download_companies_view, name='download'),

    # /companies/download-sample-csv/
    path('download-sample-csv/', views.download_sample_csv_view, name='download_sample_csv'),

    # /companies/download-sample-xlsx/
    path('download-sample-xlsx/', views.download_sample_xlsx_view, name='download_sample_xlsx'),

    # API route - check missing companies
    # /companies/api/check-missing/
    path('api/check-missing/', views.check_missing_companies_view, name='check_missing'),
    
    
    # ========================================
    # NEW URLs - Shareholding Management
    # ========================================
    
    # Shareholding views
    # /companies/shareholding/NABIL/
    path('shareholding/<str:script_ticker>/', views.company_shareholding_view, name='shareholding'),
    
    # /companies/shareholding/dashboard/
    path('shareholding-dashboard/', views.shareholding_dashboard_view, name='shareholding_dashboard'),
    
    # Lock-in dashboard
    # /companies/lock-ins/
    path('lock-ins/', views.lock_in_dashboard_view, name='lock_in_dashboard'),
    
    # CSV uploads
    # /companies/upload-shareholding/
    path('upload-shareholding/', views.upload_shareholding_csv, name='upload_shareholding'),
    
    # /companies/upload-lockin/
    path('upload-lockin/', views.upload_lockin_csv, name='upload_lockin'),
    
    # Download sample CSVs
    # /companies/download-shareholding-sample/
    path('download-shareholding-sample/', views.download_shareholding_sample_csv, name='download_shareholding_sample'),
    
    # /companies/download-lockin-sample/
    path('download-lockin-sample/', views.download_lockin_sample_csv, name='download_lockin_sample'),
    
    # API endpoints
    # /companies/api/shareholding/NABIL/
    path('api/shareholding/<str:script_ticker>/', views.get_company_shareholding_json, name='get_shareholding_json'),
    
    # /companies/api/mark-lockin-expired/123/
    path('api/mark-lockin-expired/<int:lock_in_id>/', views.mark_lockin_expired, name='mark_lockin_expired'),
]
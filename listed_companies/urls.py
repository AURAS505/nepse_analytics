# listed_companies/urls.py
from django.urls import path
from . import views

app_name = 'listed_companies'

urlpatterns = [
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

    # API route
    # /companies/api/check-missing/
    path('api/check-missing/', views.check_missing_companies_view, name='check_missing'),
]
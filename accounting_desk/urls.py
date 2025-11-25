from django.urls import path
from . import views

app_name = 'accounting_desk'

urlpatterns = [
    # 1. Main Dashboard
    path('', views.accounting_dashboard, name='dashboard'),
    
    # 2. Bank Loan & Rates
    path('loans/', views.bank_loan_report, name='bank_loan_report'),
    
    # 3. Pledge Entry Sheet (The Main Transaction Log)
    path('loans/entry-sheet/', views.pledge_entry_sheet, name='pledge_entry_sheet'),
    path('loans/entry-sheet/delete/<int:pk>/', views.delete_pledge_entry, name='delete_pledge_entry'),

    # 4. Margin Rules
    path('loans/margins/', views.manage_margins, name='manage_margins'),
    path('loans/margins/delete/<int:pk>/', views.delete_margin, name='delete_margin'),

    # 5. API
    path('api/get-scrip-info/', views.get_scrip_info, name='get_scrip_info'),

    # 6. Placeholders / Helpers
    path('loans/sync-valuations/', views.sync_loan_valuations, name='sync_loan_valuations'),
    path('loans/update-usage/', views.update_loan_usage, name='update_loan_usage'),
    path('loans/entry-sheet/edit/<int:pk>/', views.edit_pledge_entry, name='edit_pledge_entry'),
    path('loans/entry-sheet/delete-all/', views.delete_all_pledges, name='delete_all_pledges'),
    path('loans/entry-sheet/upload/', views.upload_pledge_entries, name='upload_pledge_entries'),
    path('loans/entry-sheet/sample/', views.download_pledge_sample, name='download_pledge_sample'),
    path('loans/margins/edit/<int:pk>/', views.edit_margin, name='edit_margin'),
    path('loans/margins/delete-all/', views.delete_all_margins, name='delete_all_margins'),
    path('loans/margins/upload/', views.upload_margins, name='upload_margins'),
    path('loans/margins/sample/', views.download_margin_sample, name='download_margin_sample'),
]
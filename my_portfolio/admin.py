# my_portfolio/admin.py
from django.contrib import admin
from .models import Transaction

@admin.register(Transaction)
class TransactionAdmin(admin.ModelAdmin):
    # This controls the columns shown in the transaction list
    list_display = (
        'date', 
        'symbol', 
        'transaction_type', 
        'kitta', 
        'billed_amount', 
        'rate', 
        'broker', 
        'unique_id'
    )
    
    # This adds a filter sidebar
    list_filter = ('transaction_type', 'symbol', 'date', 'broker')
    
    # This adds a search bar
    search_fields = ('symbol__script_ticker', 'script', 'unique_id')
    
    # These fields are calculated automatically and shouldn't be edited by hand
    readonly_fields = ('unique_id', 'created_at', 'script', 'sector', 'rate')
    
    # This organizes the "Edit Transaction" page
    fieldsets = (
        (None, {
            'fields': ('date', 'symbol', 'transaction_type', 'kitta')
        }),
        ('Financials', {
            'fields': ('billed_amount', 'broker')
        }),
        ('Auto-Generated Fields', {
            'fields': ('script', 'sector', 'rate', 'unique_id', 'created_at'),
            'classes': ('collapse',)  # Hides this section by default
        }),
    )

    # We add this to make the 'symbol' field searchable
    # instead of just a massive dropdown list
    autocomplete_fields = ['symbol']
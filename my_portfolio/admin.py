# my_portfolio/admin.py
from django.contrib import admin
from .models import Transaction, BrokerTransaction
# from .models import Brokers  <-- REMOVED THIS LINE

@admin.register(Transaction)
class TransactionAdmin(admin.ModelAdmin):
    # This controls the columns shown in the transaction list
    list_display = (
        'date', 
        'symbol', 
        'transaction_type', 
        'kitta',
        'rate',             # Your 'Rate'
        'gross_amount',
        'commission_amount',
        'cgt',
        'billed_amount',
        'eff_rate',         # Your 'Eff. Rate'
        'broker', 
        'unique_id'
    )
    
    # This adds a filter sidebar
    list_filter = ('transaction_type', 'symbol', 'date', 'broker')
    
    # This adds a search bar
    search_fields = ('symbol__script_ticker', 'script', 'unique_id')
    
    # These fields are calculated automatically and shouldn't be edited by hand
    readonly_fields = ('unique_id', 'created_at', 'script', 'sector', 'eff_rate', 'gross_amount')
    
    # This organizes the "Edit Transaction" page in the admin
    fieldsets = (
        (None, {
            'fields': (
                'date', 
                'symbol', 
                'transaction_type', 
                'kitta', 
                'broker'
            )
        }),
        ('Financials (Manual Entry)', {
            'fields': (
                'rate',                 # Your 'Rate'
                'commission_rate',
                'commission_amount',
                'nepse_commission',
                'sebon_regularity_fee',
                'broker_commission',
                'sebo_commission',
                'cgt',                  # Your 'CGH'
                'dp_fee',               # Your 'dp'
                'billed_amount',
            )
        }),
        ('Auto-Generated Fields', {
            'fields': (
                'script', 
                'sector', 
                'eff_rate',             # Your 'Eff. Rate'
                'gross_amount',
                'unique_id', 
                'created_at'
            ),
            'classes': ('collapse',)  # Hides this section by default
        }),
    )

    # We add this to make the 'symbol' field searchable
    # instead of just a massive dropdown list
    autocomplete_fields = ['symbol']

# Also register the BrokerTransaction model (if not already done)
@admin.register(BrokerTransaction)
class BrokerTransactionAdmin(admin.ModelAdmin):
    list_display = ('date', 'broker', 'action', 'amount', 'remarks', 'unique_id')
    list_filter = ('action', 'broker', 'date')
    search_fields = ('broker__name', 'broker__broker_no', 'remarks', 'unique_id')
    readonly_fields = ('unique_id', 'created_at')
    # autocomplete_fields = ['broker']  # Removed - requires Brokers admin to be registered

# --- REMOVED THE BrokersAdmin CLASS FROM THIS FILE ---
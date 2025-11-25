from django.contrib import admin
from .models import LoanFacility, LoanInterestHistory, MarginRule, PledgeEntrySheet

# 1. Bank Facility Admin
@admin.register(LoanFacility)
class LoanFacilityAdmin(admin.ModelAdmin):
    list_display = ('bank_name', 'account_number', 'sanctioned_limit', 'expiry_date')
    search_fields = ('bank_name',)

# 2. Interest Rate Admin
@admin.register(LoanInterestHistory)
class LoanInterestHistoryAdmin(admin.ModelAdmin):
    list_display = ('effective_date', 'loan_facility', 'base_rate', 'premium', 'total_rate')
    list_filter = ('loan_facility',)

# 3. Margin Rule Admin
@admin.register(MarginRule)
class MarginRuleAdmin(admin.ModelAdmin):
    list_display = ('symbol', 'loan_facility', 'margin_percent')
    list_filter = ('loan_facility', 'symbol')

# 4. Pledge Entry Sheet Admin
@admin.register(PledgeEntrySheet)
class PledgeEntrySheetAdmin(admin.ModelAdmin):
    list_display = ('unique_id', 'date', 'loan_facility', 'symbol', 'action', 'kitta', 'tx_drawing_power', 'tx_utilized')
    list_filter = ('loan_facility', 'action', 'symbol')
    search_fields = ('unique_id', 'symbol__script_ticker')
    
    fieldsets = (
        ('1. Entry Sheet Inputs', {
            'fields': (
                'unique_id', 'date', 'loan_facility', 'demat_account', 
                'symbol', 'action', 'kitta', 'tx_utilized', 'remarks'
            )
        }),
        ('2. Rendered Price Data (From System)', {
            'fields': ('tx_180_avg', 'tx_closing_price', 'tx_margin', 'tx_min_price')
        }),
        ('3. Calculated Results', {
            'fields': ('tx_drawing_power',)
        }),
        ('4. Ledger State (Running Balance)', {
            'classes': ('collapse',),
            'fields': ('cl_kitta', 'cl_drawing_power', 'cl_utilized')
        }),
    )
    
    # Prevent manual editing of calculated fields in Admin
    readonly_fields = ('tx_min_price', 'tx_drawing_power', 'cl_kitta', 'cl_drawing_power', 'cl_utilized')
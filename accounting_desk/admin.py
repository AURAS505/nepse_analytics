from django.contrib import admin
from .models import (
    AccountHead, 
    LedgerEntry, 
    LoanFacility, 
    PledgedScrip, 
    LoanInterestHistory
)

# --- 1. CORE ACCOUNTING ADMIN ---

@admin.register(AccountHead)
class AccountHeadAdmin(admin.ModelAdmin):
    list_display = ('name', 'category', 'broker_code')
    list_filter = ('category',)
    search_fields = ('name',)

@admin.register(LedgerEntry)
class LedgerEntryAdmin(admin.ModelAdmin):
    list_display = ('date', 'account', 'entry_type', 'amount', 'is_settled', 'description')
    list_filter = ('entry_type', 'is_settled', 'account__category', 'date')
    search_fields = ('description', 'ref_id', 'account__name')
    date_hierarchy = 'date'


# --- 2. BANK LOAN INLINES ---

class PledgedScripInline(admin.TabularInline):
    model = PledgedScrip
    extra = 1
    classes = ('collapse',) # Optional: makes it collapsible to save space

class InterestHistoryInline(admin.TabularInline):
    model = LoanInterestHistory
    extra = 1
    ordering = ('-effective_date',)


# --- 3. BANK LOAN ADMIN ---

@admin.register(LoanFacility)
class LoanFacilityAdmin(admin.ModelAdmin):
    list_display = ('bank_name', 'sanctioned_limit', 'current_used_amount', 'get_current_rate')
    search_fields = ('bank_name', 'account_number')
    
    # Add both Pledges and Interest History inside the main Loan page
    inlines = [PledgedScripInline, InterestHistoryInline]

    # Helper to show the latest effective rate in the list view
    def get_current_rate(self, obj):
        latest = obj.interest_history.first()
        if latest:
            return f"{latest.rate}% (Eff: {latest.effective_date})"
        return f"{obj.interest_rate}% (Base)"
    get_current_rate.short_description = "Current Rate"

@admin.register(PledgedScrip)
class PledgedScripAdmin(admin.ModelAdmin):
    # Standalone view for Pledged Scrips if needed
    list_display = ('symbol', 'quantity', 'loan_facility', 'valuation_percent', 'pledged_date')
    list_filter = ('loan_facility', 'symbol')
    search_fields = ('symbol', 'loan_facility__bank_name')
from django.contrib import admin
from django.db.models import Sum
from .models import (
    AccountHead, 
    LedgerEntry, 
    LoanFacility, 
    PledgedScrip, 
    LoanInterestHistory,
    PledgeEntry
)

# --- 1. CORE ACCOUNTING ADMIN ---

@admin.register(AccountHead)
class AccountHeadAdmin(admin.ModelAdmin):
    list_display = ('name', 'category', 'broker_code')
    list_filter = ('category',)
    search_fields = ('name',)
    ordering = ('category', 'name')

@admin.register(LedgerEntry)
class LedgerEntryAdmin(admin.ModelAdmin):
    list_display = ('date', 'account', 'entry_type', 'amount', 'is_settled', 'description')
    list_filter = ('entry_type', 'is_settled', 'account__category', 'date')
    search_fields = ('description', 'ref_id', 'account__name')
    date_hierarchy = 'date'
    ordering = ('-date', '-created_at')


# --- 2. BANK LOAN INLINES ---

class PledgedScripInline(admin.TabularInline):
    """Shows the CURRENT Inventory (Snapshot) inside Loan Facility"""
    model = PledgedScrip
    extra = 0
    readonly_fields = ('allowable_drawing_power',)
    fields = ('symbol', 'quantity', 'average_price', 'closing_price', 'valuation_percent', 'allowable_drawing_power')
    can_delete = False
    show_change_link = True

class InterestHistoryInline(admin.TabularInline):
    model = LoanInterestHistory
    extra = 1
    ordering = ('-effective_date',)


# --- 3. BANK LOAN & ENTRY SHEET ADMIN ---

@admin.register(PledgeEntry)
class PledgeEntryAdmin(admin.ModelAdmin):
    list_display = ('date', 'action_colored', 'loan_facility', 'symbol', 'kitta', 'margin', 'drawing_power_display', 'utilized_loan')
    list_filter = ('action', 'loan_facility', 'symbol', 'date')
    search_fields = ('symbol', 'loan_facility__bank_name', 'demat_account__name')
    date_hierarchy = 'date'
    ordering = ('-date', '-created_at')
    
    def action_colored(self, obj):
        from django.utils.html import format_html
        colors = {
            'PLEDGE': 'green',
            'UNPLEDGE': 'red',
            'BALANCE': 'blue',
        }
        color = colors.get(obj.action, 'black')
        return format_html('<span style="color: {}; font-weight: bold;">{}</span>', color, obj.get_action_display())
    action_colored.short_description = 'Action'

    def drawing_power_display(self, obj):
        return f"{obj.drawing_power:,.2f}"
    drawing_power_display.short_description = 'Drawing Power'


@admin.register(LoanFacility)
class LoanFacilityAdmin(admin.ModelAdmin):
    list_display = ('bank_name', 'account_number', 'sanctioned_limit', 'current_used_amount', 'utilization_status', 'get_current_rate')
    list_filter = ('bank_name',)
    search_fields = ('bank_name', 'account_number')
    readonly_fields = ('current_used_amount',) # Auto-calculated
    
    inlines = [PledgedScripInline, InterestHistoryInline]
    
    actions = ['recalculate_usage']

    def get_current_rate(self, obj):
        return f"{obj.get_active_rate}%"
    get_current_rate.short_description = "Current Rate"

    def utilization_status(self, obj):
        if obj.sanctioned_limit > 0:
            percent = (obj.current_used_amount / obj.sanctioned_limit) * 100
            return f"{percent:.1f}%"
        return "0%"
    utilization_status.short_description = "Utilization %"

    @admin.action(description='Recalculate Usage from Entry Sheet')
    def recalculate_usage(self, request, queryset):
        count = 0
        for loan in queryset:
            loan.recalculate_usage()
            count += 1
        self.message_user(request, f"Recalculated usage for {count} loan facilities.")


@admin.register(PledgedScrip)
class PledgedScripAdmin(admin.ModelAdmin):
    # Standalone view for Inventory Snapshot
    list_display = ('symbol', 'loan_facility', 'quantity', 'average_price', 'closing_price', 'allowable_drawing_power')
    list_filter = ('loan_facility', 'symbol')
    search_fields = ('symbol', 'loan_facility__bank_name')
    readonly_fields = ('allowable_drawing_power',)
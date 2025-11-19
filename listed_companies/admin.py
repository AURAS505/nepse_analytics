# listed_companies/admin.py
from django.contrib import admin
from .models import Companies, ShareholdingPattern, LockInPeriod, CorporateAction

# ========================================
# Register your existing Companies model
# ========================================

@admin.register(Companies)
class CompaniesAdmin(admin.ModelAdmin):
    list_display = ['script_ticker', 'company_name', 'sector', 'status', 'par_value']
    list_filter = ['sector', 'status', 'type']
    search_fields = ['script_ticker', 'company_name', 'nepse_code']
    ordering = ['script_ticker']


# ========================================
# NEW: Register Shareholding Pattern
# ========================================

@admin.register(ShareholdingPattern)
class ShareholdingPatternAdmin(admin.ModelAdmin):
    list_display = [
        'company_symbol', 'as_of_date', 
        'promoter_percentage', 'public_percentage', 
        'institutional_percentage', 'total_shares'
    ]
    list_filter = ['as_of_date', 'source']
    search_fields = ['company_symbol']
    date_hierarchy = 'as_of_date'
    ordering = ['-as_of_date', 'company_symbol']
    
    fieldsets = (
        ('Company & Date', {
            'fields': ('company_symbol', 'as_of_date')
        }),
        ('Promoter Holdings', {
            'fields': ('promoter_shares', 'promoter_percentage'),
            'classes': ('wide',)
        }),
        ('Public Holdings', {
            'fields': ('public_shares', 'public_percentage'),
            'classes': ('wide',)
        }),
        ('Institutional Holdings', {
            'fields': ('institutional_shares', 'institutional_percentage'),
            'classes': ('collapse',)
        }),
        ('Other Holdings', {
            'fields': ('other_shares', 'other_percentage'),
            'classes': ('collapse',)
        }),
        ('Free Float', {
            'fields': ('free_float_shares', 'free_float_percentage'),
        }),
        ('Total', {
            'fields': ('total_shares',),
        }),
        ('Metadata', {
            'fields': ('source', 'remarks'),
            'classes': ('collapse',)
        }),
    )


# ========================================
# NEW: Register Lock-in Period
# ========================================

@admin.register(LockInPeriod)
class LockInPeriodAdmin(admin.ModelAdmin):
    list_display = [
        'company_symbol', 'lock_in_type', 'locked_shares',
        'lock_in_start_date', 'lock_in_end_date', 
        'days_remaining_display', 'is_active'
    ]
    list_filter = ['lock_in_type', 'is_active', 'lock_in_end_date']
    search_fields = ['company_symbol', 'shareholder_name']
    date_hierarchy = 'lock_in_end_date'
    ordering = ['lock_in_end_date']
    
    fieldsets = (
        ('Company & Type', {
            'fields': ('company_symbol', 'lock_in_type', 'shareholder_name')
        }),
        ('Lock-in Details', {
            'fields': (
                'locked_shares', 
                'lock_in_start_date', 
                'lock_in_end_date',
                'description'
            )
        }),
        ('Status', {
            'fields': ('is_active',)
        }),
    )
    
    def days_remaining_display(self, obj):
        """Display days remaining with color coding"""
        from django.utils.html import format_html
        days = obj.days_remaining
        
        if days == 0:
            return format_html('<span style="color: red; font-weight: bold;">Expired</span>')
        elif days <= 30:
            return format_html('<span style="color: orange; font-weight: bold;">{} days</span>', days)
        elif days <= 90:
            return format_html('<span style="color: blue;">{} days</span>', days)
        else:
            return format_html('{} days', days)
    
    days_remaining_display.short_description = 'Days Remaining'
    
    actions = ['mark_as_inactive']
    
    def mark_as_inactive(self, request, queryset):
        """Mark selected lock-ins as inactive"""
        updated = queryset.update(is_active=False)
        self.message_user(request, f'{updated} lock-in period(s) marked as inactive.')
    
    mark_as_inactive.short_description = 'Mark selected as inactive'


# ========================================
# NEW: Register Corporate Action
# ========================================

@admin.register(CorporateAction)
class CorporateActionAdmin(admin.ModelAdmin):
    list_display = [
        'company_symbol', 'action_type', 'announcement_date',
        'book_closure_date', 'effective_date',
        'affects_promoter_holding', 'affects_public_holding'
    ]
    list_filter = [
        'action_type', 'announcement_date',
        'affects_promoter_holding', 'affects_public_holding'
    ]
    search_fields = ['company_symbol', 'description']
    date_hierarchy = 'announcement_date'
    ordering = ['-announcement_date']
    
    fieldsets = (
        ('Company & Action', {
            'fields': ('company_symbol', 'action_type', 'description')
        }),
        ('Important Dates', {
            'fields': (
                'announcement_date',
                'record_date',
                'book_closure_date',
                'effective_date',
                'regulatory_approval_date'
            )
        }),
        ('Action Details', {
            'fields': ('details',),
            'description': 'Store structured data as JSON, e.g., {"ratio": "1:1", "price": 100}'
        }),
        ('Impact', {
            'fields': ('affects_promoter_holding', 'affects_public_holding')
        }),
        ('Reference', {
            'fields': ('source_document',),
            'classes': ('collapse',)
        }),
    )
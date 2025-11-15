from django.contrib import admin
from django.utils.html import format_html
from .models import NepaliCalendar, FiscalYear, DateConversion, PublicHoliday


@admin.register(NepaliCalendar)
class NepaliCalendarAdmin(admin.ModelAdmin):
    list_display = ['bs_year', 'month_display', 'days_in_month', 'ad_start_date']
    list_filter = ['bs_year', 'month']
    search_fields = ['bs_year']
    ordering = ['-bs_year', 'month']
    list_per_page = 50
    
    def month_display(self, obj):
        return obj.get_month_display()
    month_display.short_description = 'Month'
    month_display.admin_order_field = 'month'
    
    fieldsets = (
        ('Nepali Date', {
            'fields': ('bs_year', 'month', 'days_in_month')
        }),
        ('Gregorian Reference', {
            'fields': ('ad_start_date',)
        }),
    )


@admin.register(FiscalYear)
class FiscalYearAdmin(admin.ModelAdmin):
    list_display = [
        'fiscal_year',
        'fiscal_year_english', 
        'status_badge',
        'ad_date_range', 
        'total_days',
        'created_at'
    ]
    list_filter = ['is_current', 'bs_start_year', 'ad_start_year']
    search_fields = ['fiscal_year', 'fiscal_year_english']
    ordering = ['-bs_start_year']
    readonly_fields = ['created_at', 'updated_at', 'bs_display', 'ad_display', 'full_display']
    
    def status_badge(self, obj):
        if obj.is_current:
            return format_html(
                '<span style="background-color: #28a745; color: white; padding: 3px 10px; '
                'border-radius: 3px; font-weight: bold;">CURRENT</span>'
            )
        return format_html(
            '<span style="color: #6c757d;">Inactive</span>'
        )
    status_badge.short_description = 'Status'
    
    def ad_date_range(self, obj):
        return f"{obj.ad_start_date} to {obj.ad_end_date}"
    ad_date_range.short_description = 'AD Period'
    
    fieldsets = (
        ('Fiscal Year', {
            'fields': ('fiscal_year', 'fiscal_year_english', 'is_current', 'full_display')
        }),
        ('Nepali Date (BS)', {
            'fields': (
                ('bs_start_year', 'bs_start_month', 'bs_start_day'),
                ('bs_end_year', 'bs_end_month', 'bs_end_day'),
                'bs_display'
            )
        }),
        ('Gregorian Date (AD)', {
            'fields': (
                ('ad_start_date', 'ad_start_year'),
                ('ad_end_date', 'ad_end_year'),
                'ad_display'
            )
        }),
        ('Metadata', {
            'fields': ('total_days', 'notes', 'created_at', 'updated_at'),
            'classes': ('collapse',)
        }),
    )
    
    actions = ['set_as_current']
    
    def set_as_current(self, request, queryset):
        if queryset.count() > 1:
            self.message_user(
                request,
                "Please select only one fiscal year to set as current.",
                level='error'
            )
            return
        
        FiscalYear.objects.update(is_current=False)
        queryset.update(is_current=True)
        self.message_user(
            request,
            f"Set {queryset.first().fiscal_year} as current fiscal year."
        )
    set_as_current.short_description = "Set selected as current fiscal year"


@admin.register(DateConversion)
class DateConversionAdmin(admin.ModelAdmin):
    list_display = [
        'bs_date_display',
        'ad_date',
        'fiscal_year',
        'day_name',
        'weekend_badge'
    ]
    list_filter = ['fiscal_year', 'is_weekend', 'bs_year']
    search_fields = ['ad_date', 'bs_year']
    date_hierarchy = 'ad_date'
    ordering = ['-ad_date']
    readonly_fields = ['day_of_week', 'is_weekend', 'created_at']
    list_per_page = 100
    
    def bs_date_display(self, obj):
        return f"{obj.bs_year}/{obj.bs_month:02d}/{obj.bs_day:02d}"
    bs_date_display.short_description = 'BS Date'
    
    def day_name(self, obj):
        days = ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday']
        return days[obj.day_of_week]
    day_name.short_description = 'Day'
    
    def weekend_badge(self, obj):
        if obj.is_weekend:
            return format_html(
                '<span style="background-color: #dc3545; color: white; padding: 2px 8px; '
                'border-radius: 3px; font-size: 11px;">WEEKEND</span>'
            )
        return ''
    weekend_badge.short_description = 'Weekend'
    
    fieldsets = (
        ('Nepali Date (BS)', {
            'fields': (('bs_year', 'bs_month', 'bs_day'),)
        }),
        ('Gregorian Date (AD)', {
            'fields': ('ad_date', 'day_of_week', 'is_weekend')
        }),
        ('Reference', {
            'fields': ('fiscal_year', 'created_at')
        }),
    )


@admin.register(PublicHoliday)
class PublicHolidayAdmin(admin.ModelAdmin):
    list_display = [
        'name',
        'ad_date',
        'bs_date_display',
        'holiday_type',
        'nepse_status'
    ]
    list_filter = ['holiday_type', 'is_nepse_trading_day', 'bs_year', 'ad_date']
    search_fields = ['name', 'name_nepali', 'description']
    date_hierarchy = 'ad_date'
    ordering = ['-ad_date']
    list_per_page = 50
    
    def bs_date_display(self, obj):
        return f"{obj.bs_year}/{obj.bs_month:02d}/{obj.bs_day:02d}"
    bs_date_display.short_description = 'BS Date'
    
    def nepse_status(self, obj):
        if obj.is_nepse_trading_day:
            return format_html(
                '<span style="background-color: #28a745; color: white; padding: 2px 8px; '
                'border-radius: 3px; font-size: 11px;">OPEN</span>'
            )
        return format_html(
            '<span style="background-color: #dc3545; color: white; padding: 2px 8px; '
            'border-radius: 3px; font-size: 11px;">CLOSED</span>'
        )
    nepse_status.short_description = 'NEPSE'
    
    fieldsets = (
        ('Holiday Information', {
            'fields': ('name', 'name_nepali', 'holiday_type', 'description')
        }),
        ('Nepali Date (BS)', {
            'fields': (('bs_year', 'bs_month', 'bs_day'),)
        }),
        ('Gregorian Date (AD)', {
            'fields': ('ad_date',)
        }),
        ('Trading Status', {
            'fields': ('is_nepse_trading_day',)
        }),
    )
    
    actions = ['mark_as_nepse_closed', 'mark_as_nepse_open']
    
    def mark_as_nepse_closed(self, request, queryset):
        updated = queryset.update(is_nepse_trading_day=False)
        self.message_user(request, f"{updated} holidays marked as NEPSE closed.")
    mark_as_nepse_closed.short_description = "Mark NEPSE as closed"
    
    def mark_as_nepse_open(self, request, queryset):
        updated = queryset.update(is_nepse_trading_day=True)
        self.message_user(request, f"{updated} holidays marked as NEPSE open.")
    mark_as_nepse_open.short_description = "Mark NEPSE as open"
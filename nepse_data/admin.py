from django.contrib import admin
from django.utils.html import format_html
from .models import (
    StockPrices, 
    Indices, 
    Marcap, 
    FloorsheetRaw, 
    Brokers, 
    DividendHistory
)

@admin.register(StockPrices)
class StockPricesAdmin(admin.ModelAdmin):
    list_display = (
        'symbol', 
        'business_date', 
        'close_price', 
        'total_traded_quantity', 
        'total_traded_value', 
        'total_trades'
    )
    list_filter = ('business_date',)
    search_fields = ('symbol', 'security_name', 'security_id')
    date_hierarchy = 'business_date'
    ordering = ('-business_date', 'symbol')
    list_per_page = 50

@admin.register(Indices)
class IndicesAdmin(admin.ModelAdmin):
    list_display = (
        'sector', 
        'date', 
        'close', 
        'formatted_change', 
        'turnover_values'
    )
    list_filter = ('sector', 'date')
    search_fields = ('sector',)
    date_hierarchy = 'date'
    ordering = ('-date', 'sector')

    def formatted_change(self, obj):
        """Color codes the percentage change: Green for positive, Red for negative."""
        try:
            # Remove % sign if present and convert to float
            val_str = str(obj.percentage_change).replace('%', '').strip()
            val = float(val_str)
            
            if val > 0:
                color = 'green'
            elif val < 0:
                color = 'red'
            else:
                color = 'black'
                
            return format_html(
                '<span style="color: {}; font-weight: bold;">{}%</span>', 
                color, val
            )
        except (ValueError, TypeError):
            return obj.percentage_change
    
    formatted_change.short_description = 'Change %'


@admin.register(Marcap)
class MarcapAdmin(admin.ModelAdmin):
    list_display = (
        'business_date', 
        'market_capitalization', 
        'total_turnover', 
        'total_transactions'
    )
    date_hierarchy = 'business_date'
    ordering = ('-business_date',)


@admin.register(FloorsheetRaw)
class FloorsheetRawAdmin(admin.ModelAdmin):
    list_display = (
        'contract_no', 
        'stock_symbol', 
        'calculation_date', 
        'buyer', 
        'seller', 
        'quantity', 
        'rate', 
        'amount'
    )
    list_filter = ('calculation_date', 'sector')
    search_fields = ('contract_no', 'stock_symbol', 'buyer', 'seller')
    date_hierarchy = 'calculation_date'
    ordering = ('-calculation_date', '-contract_no')
    list_per_page = 100  # Higher pagination for easy scanning
    
    # Since this is managed=False and high volume, let's protect the data
    def has_add_permission(self, request):
        return False

    def has_change_permission(self, request, obj=None):
        return False


@admin.register(Brokers)
class BrokersAdmin(admin.ModelAdmin):
    list_display = ('broker_no', 'name', 'contact_person', 'status', 'tms_link_display')
    search_fields = ('broker_no', 'name')
    list_display_links = ('broker_no', 'name')
    ordering = ('broker_no',)

    def tms_link_display(self, obj):
        if obj.tms_link:
            return format_html('<a href="{}" target="_blank">Open TMS</a>', obj.tms_link)
        return "-"
    tms_link_display.short_description = "TMS"

@admin.register(DividendHistory)
class DividendHistoryAdmin(admin.ModelAdmin):
    # 1. Columns shown in the list view
    list_display = (
        'symbol', 
        'fiscal_year', 
        'total_percent', 
        'bonus_percent', 
        'cash_percent', 
        'right_percent',  # Added Right Share %
        'book_closure_date',
        'colored_status'  # Custom color-coded status
    )

    # 2. Filters on the right sidebar
    list_filter = (
        'fiscal_year', 
        'book_closure_status', 
        'book_closure_date'
    )

    # 3. Search bar capability
    search_fields = ('symbol', 'company_name')

    # 4. Date navigation bar at the top
    date_hierarchy = 'book_closure_date'
    
    # 5. Items per page
    list_per_page = 50

    # 6. Organize the Edit/Add form into specific groups
    fieldsets = (
        ('Company Information', {
            'fields': (
                ('symbol', 'fiscal_year'),
                'company_name'
            )
        }),
        ('Dividend Details (%)', {
            'fields': (
                ('bonus_percent', 'cash_percent'),
                ('right_percent', 'tax_percent'),
                'total_percent'
            ),
            'description': 'Enter percentages as numbers (e.g., 10.5 for 10.5%)'
        }),
        ('Important Dates', {
            'fields': (
                ('announcement_date', 'book_closure_date'),
                ('distribution_date', 'bonus_listing_date')
            )
        }),
        ('Administrative', {
            'fields': ('book_closure_status',)
        }),
    )

    def colored_status(self, obj):
        """
        Displays 'Posted' in green and other statuses in orange/red.
        """
        status = str(obj.book_closure_status).strip() if obj.book_closure_status else ''
        if status.lower() == 'posted':
            color = 'green'
            weight = 'bold'
        else:
            color = '#d6810b' # Orange-ish
            weight = 'normal'

        return format_html(
            '<span style="color: {}; font-weight: {};">{}</span>', 
            color, weight, status
        )
    
    colored_status.short_description = 'Status'
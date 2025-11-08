from django.contrib import admin
from .models import PriceAdjustments

@admin.register(PriceAdjustments)
class PriceAdjustmentAdmin(admin.ModelAdmin):
    list_display = ('symbol', 'adjustment_type', 'book_close_date', 'adjustment_percent', 'par_value')
    search_fields = ('symbol__script_ticker',)  # This lets you search by the company ticker
    list_filter = ('adjustment_type',)
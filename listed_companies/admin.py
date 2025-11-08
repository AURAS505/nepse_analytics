from django.contrib import admin
from .models import Companies

@admin.register(Companies)
class CompanyAdmin(admin.ModelAdmin):
    list_display = ('script_ticker', 'company_name', 'sector', 'status', 'par_value')
    search_fields = ('script_ticker', 'company_name')
    list_filter = ('sector', 'status')
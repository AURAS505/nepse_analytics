# listed_companies/models.py
from django.db import models

class Companies(models.Model):
    # This is fine, Django sees it's the primary key
    nepse_code = models.CharField(primary_key=True, max_length=50)
    script_ticker = models.CharField(unique=True, max_length=20)
    company_name = models.CharField(max_length=255)
    sector = models.CharField(max_length=100, blank=True, null=True)
    type = models.CharField(max_length=50, blank=True, null=True)
    status = models.CharField(max_length=50, blank=True, null=True)
    instrument = models.CharField(max_length=50, blank=True, null=True)
    par_value = models.DecimalField(max_digits=10, decimal_places=2, default=100.00)

    class Meta:
        # managed = False  <-- REMOVED. This lets Django manage the table.
        db_table = 'companies'

    def __str__(self):
        # This gives a nice name in the admin panel
        return self.company_name
    
class FloorsheetRaw(models.Model):
    # We need a primary key for Django, but the DB uses contract_no
    # We will use the 'id' from your generated_models.py
    id = models.BigIntegerField(primary_key=True)
    contract_no = models.CharField(max_length=255, blank=True, null=True)
    stock_symbol = models.CharField(max_length=255)
    buyer = models.IntegerField(blank=True, null=True)
    seller = models.IntegerField(blank=True, null=True)
    quantity = models.IntegerField(blank=True, null=True)
    rate = models.DecimalField(max_digits=10, decimal_places=2, blank=True, null=True)
    amount = models.DecimalField(max_digits=15, decimal_places=2, blank=True, null=True)
    calculation_date = models.DateField(blank=True, null=True)
    sector = models.CharField(max_length=255, blank=True, null=True)

    class Meta:
        managed = False  # <-- Tells Django to *not* create or change this table
        db_table = 'floorsheet_raw'
        verbose_name = 'Floorsheet (Raw)'
        verbose_name_plural = 'Floorsheet (Raw)'

    def __str__(self):
        return f"{self.stock_symbol} ({self.contract_no})"
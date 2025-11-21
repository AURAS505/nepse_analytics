from django.db import models

class FloorsheetRaw(models.Model):
    # Based on the fields used in your SQL queries in views.py
    id = models.BigAutoField(primary_key=True)
    contract_no = models.CharField(max_length=100)
    stock_symbol = models.CharField(max_length=20)
    buyer = models.IntegerField()
    seller = models.IntegerField()
    quantity = models.IntegerField()
    rate = models.DecimalField(max_digits=10, decimal_places=2)
    amount = models.DecimalField(max_digits=15, decimal_places=2)
    sector = models.CharField(max_length=100, null=True, blank=True)
    calculation_date = models.DateField()

    class Meta:
        # This connects the model to your existing database table
        db_table = 'floorsheet_raw'
        managed = False  # Tells Django not to try to create/change this table, just read it
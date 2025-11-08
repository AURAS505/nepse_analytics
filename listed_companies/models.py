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
# accounting_desk/utils.py
from decimal import Decimal
from django.db import transaction
from .models import PledgeEntrySheet

def recalculate_ledger(loan_facility_id, symbol_ticker):
    """
    Recalculates running balances (cl_kitta, cl_drawing_power, cl_utilized)
    for a specific Bank + Script combination, strictly ordered by Date + CreatedAt.
    """
    entries = PledgeEntrySheet.objects.filter(
        loan_facility_id=loan_facility_id,
        symbol__script_ticker=symbol_ticker
    ).order_by('date', 'created_at')

    running_kitta = 0
    running_dp = Decimal(0)
    running_utilized = Decimal(0)

    entries_to_update = []

    for entry in entries:
        # --- 1. Calculate Transaction Level Values ---
        # (This duplicates logic in save(), but ensures consistency during bulk updates)
        
        # Calculate Min Price
        p1 = entry.tx_180_avg if entry.tx_180_avg else Decimal(0)
        p2 = entry.tx_closing_price if entry.tx_closing_price else Decimal(0)
        
        if p1 > 0 and p2 > 0:
            entry.tx_min_price = min(p1, p2)
        else:
            entry.tx_min_price = max(p1, p2)

        # Calculate Drawing Power for THIS transaction only
        if entry.kitta > 0 and entry.tx_min_price > 0:
            valuation = Decimal(entry.kitta) * entry.tx_min_price
            entry.tx_drawing_power = valuation * (entry.tx_margin / Decimal(100))
        else:
            entry.tx_drawing_power = Decimal(0)

        # --- 2. Update Running Balances ---
        if entry.action == 'BALANCE_BD':
            running_kitta = entry.kitta
            running_dp = entry.tx_drawing_power
            running_utilized = entry.tx_utilized

        elif entry.action == 'PLEDGE':
            running_kitta += entry.kitta
            running_dp += entry.tx_drawing_power
            running_utilized += entry.tx_utilized

        elif entry.action == 'UNPLEDGE':
            running_kitta = max(0, running_kitta - entry.kitta)
            running_dp = max(Decimal(0), running_dp - entry.tx_drawing_power)
            running_utilized = max(Decimal(0), running_utilized - entry.tx_utilized)

        elif entry.action == 'VALUATION':
            # For valuation, re-assess the ENTIRE inventory at the new price
            total_valuation = Decimal(running_kitta) * entry.tx_min_price
            new_total_dp = total_valuation * (entry.tx_margin / Decimal(100))
            
            # The transaction DP is the difference (Gain/Loss)
            entry.tx_drawing_power = new_total_dp - running_dp
            
            running_dp = new_total_dp
            # Utilized usually doesn't change on valuation, unless specific repayment
            running_utilized += entry.tx_utilized 

        # Set Closing Values on Object
        entry.cl_kitta = running_kitta
        entry.cl_drawing_power = running_dp
        entry.cl_utilized = running_utilized
        
        entries_to_update.append(entry)

    # Bulk update to save database hits
    with transaction.atomic():
        PledgeEntrySheet.objects.bulk_update(entries_to_update, [
            'tx_min_price', 'tx_drawing_power', 
            'cl_kitta', 'cl_drawing_power', 'cl_utilized'
        ])
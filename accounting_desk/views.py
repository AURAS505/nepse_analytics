import csv
from django.shortcuts import render, redirect, get_object_or_404
from django.contrib import messages
from django.http import JsonResponse, HttpResponse
from django.db.models import Sum, F
from django.views.decorators.http import require_POST
from django.utils import timezone
from datetime import datetime
from decimal import Decimal

from .models import LoanFacility, PledgedScrip, PledgeEntry, StockMargin, MeroShareHolding, DematAccount
from .forms import LoanFacilityForm, PledgeEntryForm, StockMarginForm
from nepse_data.models import StockPrices



# ==========================================
# HELPER FUNCTIONS (To avoid code duplication)
# ==========================================

def _update_inventory(entry):
    """Applies a PledgeEntry's effect to Inventory & MeroShare"""
    # 1. Update Inventory (PledgedScrip)
    scrip, created = PledgedScrip.objects.get_or_create(
        loan_facility=entry.loan_facility,
        demat_account=entry.demat_account,
        symbol=entry.symbol,
        defaults={'valuation_percent': entry.margin}
    )
    
    # Use Python math
    if entry.action in ['BALANCE', 'PLEDGE']:
        scrip.quantity += entry.kitta
        scrip.utilized_amount += entry.utilized_loan
    elif entry.action == 'UNPLEDGE':
        if scrip.quantity >= entry.kitta:
            scrip.quantity -= entry.kitta
        else:
            scrip.quantity = 0
        scrip.utilized_amount -= entry.utilized_loan
    
    scrip.average_price = entry.average_closing_price
    scrip.closing_price = entry.closing_price
    scrip.valuation_percent = entry.margin
    scrip.save()
    
    # 2. Update MeroShare
    holding = MeroShareHolding.objects.filter(demat_account=entry.demat_account, symbol__script_ticker=entry.symbol).order_by('-snapshot_date').first()
    if holding:
        if entry.action in ['BALANCE', 'PLEDGE']:
            holding.free_balance = F('free_balance') - entry.kitta
            holding.pledge_balance = F('pledge_balance') + entry.kitta
        elif entry.action == 'UNPLEDGE':
            holding.free_balance = F('free_balance') + entry.kitta
            holding.pledge_balance = F('pledge_balance') - entry.kitta
        holding.save()

def _revert_inventory(entry):
    """Reverses a PledgeEntry's effect (Used for Edit/Delete)"""
    # 1. Reverse Inventory
    scrip = PledgedScrip.objects.filter(
        loan_facility=entry.loan_facility,
        demat_account=entry.demat_account,
        symbol=entry.symbol
    ).first()

    if scrip:
        if entry.action in ['BALANCE', 'PLEDGE']:
            # Originally added, so subtract
            scrip.quantity = max(0, scrip.quantity - entry.kitta)
            scrip.utilized_amount -= entry.utilized_loan
        elif entry.action == 'UNPLEDGE':
            # Originally subtracted, so add back
            scrip.quantity += entry.kitta
            scrip.utilized_amount += entry.utilized_loan
        scrip.save()

    # 2. Reverse MeroShare
    holding = MeroShareHolding.objects.filter(demat_account=entry.demat_account, symbol__script_ticker=entry.symbol).order_by('-snapshot_date').first()
    if holding:
        if entry.action in ['BALANCE', 'PLEDGE']:
            # Was deducted from free, so add back
            holding.free_balance = F('free_balance') + entry.kitta
            holding.pledge_balance = F('pledge_balance') - entry.kitta
        elif entry.action == 'UNPLEDGE':
            # Was added to free, so deduct
            holding.free_balance = F('free_balance') - entry.kitta
            holding.pledge_balance = F('pledge_balance') + entry.kitta
        holding.save()


# ==========================================
# 1. DASHBOARD & CORE UTILS
# ==========================================

def accounting_dashboard(request):
    """Main landing page for Accounting Desk"""
    return render(request, 'accounting_desk/accounting_dashboard.html', {})

def get_scrip_info(request):
    """API to fetch Price and Margin for a script"""
    loan_id = request.GET.get('loan_id')
    symbol = request.GET.get('symbol', '').upper()
    
    data = {'found': False, 'closing_price': 0, 'margin': 50} # Default margin

    if symbol:
        # 1. Fetch Price
        price_obj = StockPrices.objects.filter(symbol=symbol).order_by('-business_date').first()
        if price_obj:
            data['found'] = True
            data['closing_price'] = float(price_obj.close_price)
        
        # 2. Fetch Specific Margin for this Bank+Script
        if loan_id:
            margin_obj = StockMargin.objects.filter(
                loan_facility_id=loan_id, 
                script=symbol
            ).order_by('-date').first()
            
            if margin_obj:
                data['margin'] = margin_obj.margin
                data['margin_source'] = 'Custom'
            else:
                data['margin_source'] = 'Default'

    return JsonResponse(data)


# ==========================================
# 2. BANK LOAN REPORT (DASHBOARD)
# ==========================================

def bank_loan_report(request):
    loan_form = LoanFacilityForm()

    if request.method == 'POST' and 'add_loan' in request.POST:
        loan_form = LoanFacilityForm(request.POST)
        if loan_form.is_valid():
            loan_form.save()
            messages.success(request, "New Loan Facility Added")
            return redirect('accounting_desk:bank_loan_report')
        else:
             messages.error(request, "Error adding loan.")
            
    loans = LoanFacility.objects.prefetch_related('pledged_scrips').all()
    
    grand_totals = {
        'total_sanctioned': 0,
        'total_used': 0,
        'total_drawing_power': 0,
        'total_collateral_value': 0,
    }

    processed_loans = []
    for loan in loans:
        loan_dp = 0
        loan_collat = 0
        for scrip in loan.pledged_scrips.all():
            # Real-time price fetch
            price_obj = StockPrices.objects.filter(symbol=scrip.symbol).order_by('-business_date').first()
            ltp = float(price_obj.close_price) if price_obj else float(scrip.closing_price)
            
            # Recalculate Scrip DP
            base = min(float(scrip.average_price), ltp)
            scrip_dp = base * scrip.quantity * (scrip.valuation_percent / 100)
            scrip_val = ltp * scrip.quantity
            
            loan_dp += scrip_dp
            loan_collat += scrip_val
            
            scrip.ltp = ltp
            scrip.current_dp = scrip_dp
            scrip.current_value = scrip_val

        loan.calculated_dp = loan_dp
        loan.calculated_collateral = loan_collat
        
        used = float(loan.current_used_amount)
        loan.headroom = loan_dp - used
        loan.utilization_percent = (used / loan_dp * 100) if loan_dp > 0 else 0
        
        processed_loans.append(loan)
        
        grand_totals['total_sanctioned'] += float(loan.sanctioned_limit)
        grand_totals['total_used'] += used
        grand_totals['total_drawing_power'] += loan_dp
        grand_totals['total_collateral_value'] += loan_collat

    grand_totals['total_headroom'] = grand_totals['total_drawing_power'] - grand_totals['total_used']

    context = {
        'loans': processed_loans,
        'grand_totals': grand_totals,
        'loan_form': loan_form,
    }
    return render(request, 'accounting_desk/bank_loan_report.html', context)

@require_POST
def update_loan_usage(request):
    return redirect('accounting_desk:bank_loan_report')

@require_POST
def sync_loan_valuations(request):
    count = 0
    for scrip in PledgedScrip.objects.all():
        price = StockPrices.objects.filter(symbol=scrip.symbol).order_by('-business_date').first()
        if price:
            scrip.closing_price = price.close_price
            scrip.save()
            count += 1
    return JsonResponse({'success': True, 'updated': count})


# ==========================================
# 3. PLEDGE ENTRY SHEET & ACTIONS
# ==========================================

def pledge_entry_sheet(request):
    if request.method == 'POST':
        form = PledgeEntryForm(request.POST)
        if form.is_valid():
            entry = form.save(commit=False)
            entry.symbol = entry.symbol.upper()
            
            # Logic: Closing Price
            if entry.action == 'BALANCE' and form.cleaned_data.get('closing_price'):
                entry.closing_price = form.cleaned_data['closing_price']
            else:
                price_obj = StockPrices.objects.filter(symbol=entry.symbol).order_by('-business_date').first()
                entry.closing_price = price_obj.close_price if price_obj else 0
            
            entry.save() 
            
            # Update Inventory
            scrip, created = PledgedScrip.objects.get_or_create(
                loan_facility=entry.loan_facility,
                demat_account=entry.demat_account,
                symbol=entry.symbol,
                defaults={'valuation_percent': entry.margin}
            )
            
            # Use Python Math for updates (avoid F() expression error on save)
            if entry.action in ['BALANCE', 'PLEDGE']:
                scrip.quantity += entry.kitta
                scrip.utilized_amount += entry.utilized_loan
            elif entry.action == 'UNPLEDGE':
                if scrip.quantity >= entry.kitta:
                    scrip.quantity -= entry.kitta
                else:
                    scrip.quantity = 0
                scrip.utilized_amount -= entry.utilized_loan
            
            scrip.average_price = entry.average_closing_price
            scrip.closing_price = entry.closing_price
            scrip.valuation_percent = entry.margin
            scrip.save()
            
            # MeroShare Update
            holding = MeroShareHolding.objects.filter(demat_account=entry.demat_account, symbol__script_ticker=entry.symbol).order_by('-snapshot_date').first()
            if holding:
                if entry.action in ['BALANCE', 'PLEDGE']:
                    holding.free_balance = F('free_balance') - entry.kitta
                    holding.pledge_balance = F('pledge_balance') + entry.kitta
                elif entry.action == 'UNPLEDGE':
                    holding.free_balance = F('free_balance') + entry.kitta
                    holding.pledge_balance = F('pledge_balance') - entry.kitta
                holding.save()

            messages.success(request, "Entry Saved.")
            return redirect('accounting_desk:pledge_entry_sheet')
    else:
        form = PledgeEntryForm()

    entries = PledgeEntry.objects.select_related('loan_facility', 'demat_account').order_by('-date', '-id')
    total_utilization = LoanFacility.objects.aggregate(Sum('current_used_amount'))['current_used_amount__sum'] or 0

    return render(request, 'accounting_desk/pledge_entry_sheet.html', {
        'form': form, 'entries': entries, 'total_utilization': total_utilization
    })

def edit_pledge_entry(request, pk):
    """
    To edit safely:
    1. Revert the effect of the OLD entry data.
    2. Save the NEW entry data.
    3. Apply the effect of the NEW entry data.
    """
    entry = get_object_or_404(PledgeEntry, pk=pk)
    
    if request.method == 'POST':
        form = PledgeEntryForm(request.POST, instance=entry)
        if form.is_valid():
            # 1. Revert Old Impact (using current DB state before save)
            # We must fetch the object again or rely on the fact that 'entry' still has old data 
            # (Django form save(commit=False) updates the instance, so we must reverse BEFORE form processing)
            original_entry = PledgeEntry.objects.get(pk=pk) # Fetch fresh copy of old data
            _revert_inventory(original_entry)
            
            # 2. Save New Data
            new_entry = form.save(commit=False)
            new_entry.symbol = new_entry.symbol.upper()
            
            # Recalculate price if needed, or keep existing logic
            if new_entry.action == 'BALANCE' and form.cleaned_data.get('closing_price'):
                new_entry.closing_price = form.cleaned_data['closing_price']
            else:
                price_obj = StockPrices.objects.filter(symbol=new_entry.symbol).order_by('-business_date').first()
                new_entry.closing_price = price_obj.close_price if price_obj else 0
            
            new_entry.save()
            
            # 3. Apply New Impact
            _update_inventory(new_entry)
            
            # 4. Trigger Usage Recalc
            new_entry.loan_facility.recalculate_usage()
            if original_entry.loan_facility != new_entry.loan_facility:
                original_entry.loan_facility.recalculate_usage()

            messages.success(request, "Entry Updated Successfully.")
            return redirect('accounting_desk:pledge_entry_sheet')
    else:
        form = PledgeEntryForm(instance=entry)

    return render(request, 'accounting_desk/edit_pledge.html', {'form': form, 'entry': entry})

def delete_pledge_entry(request, pk):
    """Deletes an entry and REVERSES its impact on Inventory"""
    entry = get_object_or_404(PledgeEntry, pk=pk)
    
    # 1. Reverse Impact on Inventory (PledgedScrip)
    scrip = PledgedScrip.objects.filter(
        loan_facility=entry.loan_facility,
        demat_account=entry.demat_account,
        symbol=entry.symbol
    ).first()

    if scrip:
        if entry.action in ['BALANCE', 'PLEDGE']:
            # Originally added, so we subtract
            scrip.quantity = max(0, scrip.quantity - entry.kitta)
            scrip.utilized_amount -= entry.utilized_loan
        elif entry.action == 'UNPLEDGE':
            # Originally subtracted, so we add back
            scrip.quantity += entry.kitta
            scrip.utilized_amount += entry.utilized_loan
        scrip.save()

    # 2. Reverse Impact on MeroShare
    holding = MeroShareHolding.objects.filter(demat_account=entry.demat_account, symbol__script_ticker=entry.symbol).order_by('-snapshot_date').first()
    if holding:
        if entry.action in ['BALANCE', 'PLEDGE']:
            holding.free_balance = F('free_balance') + entry.kitta
            holding.pledge_balance = F('pledge_balance') - entry.kitta
        elif entry.action == 'UNPLEDGE':
            holding.free_balance = F('free_balance') - entry.kitta
            holding.pledge_balance = F('pledge_balance') + entry.kitta
        holding.save()

    # 3. Delete Entry & Recalculate Loan Usage
    loan_facility = entry.loan_facility
    entry.delete()
    loan_facility.recalculate_usage()
    
    messages.warning(request, "Entry Deleted and Inventory Reversed.")
    return redirect('accounting_desk:pledge_entry_sheet')

def delete_all_pledges(request):
    """Wipes all data and resets inventory"""
    if request.method == 'POST':
        PledgeEntry.objects.all().delete()
        PledgedScrip.objects.all().delete()
        
        # Reset Loan Usage
        for loan in LoanFacility.objects.all():
            loan.current_used_amount = 0
            loan.save()
            
        messages.error(request, "All Pledge Entries & Inventory Deleted!")
    return redirect('accounting_desk:pledge_entry_sheet')

def download_pledge_sample(request):
    response = HttpResponse(content_type='text/csv')
    response['Content-Disposition'] = 'attachment; filename="pledge_sample.csv"'
    writer = csv.writer(response)
    writer.writerow(['Date', 'Bank Name', 'Demat Name', 'Script', 'Action (BALANCE/PLEDGE/UNPLEDGE)', 'Kitta', 'Margin', 'Avg Price', 'Utilized Loan'])
    writer.writerow(['2023-10-01', 'Nabil Bank', 'My Demat 1', 'NICA', 'BALANCE', '100', '50', '450', '50000'])
    return response

def upload_pledge_entries(request):
    if request.method == 'POST' and request.FILES.get('file'):
        csv_file = request.FILES['file']
        decoded_file = csv_file.read().decode('utf-8').splitlines()
        reader = csv.DictReader(decoded_file)
        
        count = 0
        try:
            for row in reader:
                bank = LoanFacility.objects.filter(bank_name__iexact=row['Bank Name']).first()
                demat = DematAccount.objects.filter(capital_name__icontains=row['Demat Name']).first()
                
                if not bank or not demat:
                    continue # Skip invalid rows

                PledgeEntry.objects.create(
                    date=row['Date'],
                    loan_facility=bank,
                    demat_account=demat,
                    symbol=row['Script'].upper(),
                    action=row['Action (BALANCE/PLEDGE/UNPLEDGE)'],
                    kitta=int(row['Kitta']),
                    margin=float(row['Margin']),
                    average_closing_price=float(row['Avg Price']),
                    utilized_loan=float(row['Utilized Loan'])
                )
                count += 1
            
            # Simple sync after bulk upload (better to optimize later)
            for loan in LoanFacility.objects.all():
                loan.recalculate_usage()

            messages.success(request, f"Uploaded {count} entries.")
        except Exception as e:
            messages.error(request, f"Error in CSV: {str(e)}")
            
    return redirect('accounting_desk:pledge_entry_sheet')


# ==========================================
# 4. MARGIN MANAGEMENT & ACTIONS
# ==========================================

def manage_margins(request):
    if request.method == 'POST':
        form = StockMarginForm(request.POST)
        if form.is_valid():
            form.save()
            messages.success(request, "Margin saved.")
            return redirect('accounting_desk:manage_margins')
    else:
        form = StockMarginForm()
    
    margins = StockMargin.objects.select_related('loan_facility').order_by('-date')
    return render(request, 'accounting_desk/margin_management.html', {'form': form, 'margins': margins})


def edit_margin(request, pk):
    margin = get_object_or_404(StockMargin, pk=pk)
    if request.method == 'POST':
        form = StockMarginForm(request.POST, instance=margin)
        if form.is_valid():
            form.save()
            messages.success(request, "Margin Updated.")
            return redirect('accounting_desk:manage_margins')
    else:
        form = StockMarginForm(instance=margin)
    
    return render(request, 'accounting_desk/edit_margin.html', {'form': form, 'margin': margin})

def delete_margin(request, pk):
    margin = get_object_or_404(StockMargin, pk=pk)
    margin.delete()
    messages.warning(request, "Margin Deleted.")
    return redirect('accounting_desk:manage_margins')

def delete_all_margins(request):
    if request.method == 'POST':
        StockMargin.objects.all().delete()
        messages.error(request, "All Margins Deleted!")
    return redirect('accounting_desk:manage_margins')

def download_margin_sample(request):
    response = HttpResponse(content_type='text/csv')
    response['Content-Disposition'] = 'attachment; filename="margin_sample.csv"'
    writer = csv.writer(response)
    writer.writerow(['Date', 'Bank Name', 'Script', 'Margin', 'Remarks'])
    writer.writerow(['2023-10-01', 'Nabil Bank', 'NICA', '50', 'Quarterly Review'])
    return response

def upload_margins(request):
    if request.method == 'POST' and request.FILES.get('file'):
        csv_file = request.FILES['file']
        decoded_file = csv_file.read().decode('utf-8').splitlines()
        reader = csv.DictReader(decoded_file)
        
        count = 0
        try:
            for row in reader:
                bank = LoanFacility.objects.filter(bank_name__iexact=row['Bank Name']).first()
                if bank:
                    StockMargin.objects.create(
                        date=row['Date'],
                        loan_facility=bank,
                        script=row['Script'].upper(),
                        margin=float(row['Margin']),
                        remarks=row.get('Remarks', '')
                    )
                    count += 1
            messages.success(request, f"Uploaded {count} margins.")
        except Exception as e:
            messages.error(request, f"Error: {str(e)}")
            
    return redirect('accounting_desk:manage_margins')


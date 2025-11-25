from django.shortcuts import render, redirect, get_object_or_404
from django.contrib.auth.decorators import login_required
from django.contrib import messages
from django.db.models import Sum
from django.http import JsonResponse, HttpResponse
from django.utils import timezone
from decimal import Decimal
from datetime import datetime, timedelta

# --- UPDATED IMPORTS (Using new model/form names) ---
from .models import (
    LoanFacility, 
    LoanInterestHistory, 
    MarginRule, 
    PledgeEntrySheet  # <--- Renamed from PledgeLedger
)
from .forms import (
    LoanFacilityForm, 
    LoanInterestForm, 
    MarginRuleForm, 
    PledgeEntrySheetForm # <--- Renamed from PledgeLedgerForm
)

# Other Apps
from nepse_data.models import StockPrices, Indices
from listed_companies.models import Companies

@login_required
def accounting_dashboard(request):
    """
    Main Landing Page / Dashboard
    """
    context = {
        'total_loans': LoanFacility.objects.count(),
        'active_pledges': PledgeEntrySheet.objects.filter(cl_kitta__gt=0).count(),
    }
    return render(request, 'accounting_desk/accounting_dashboard.html', context)


@login_required
def bank_loan_report(request):
    """
    Entry Sheet 1 & 2 Management (Loans & Rates)
    Also shows summary of utilization.
    """
    # Initialize forms
    loan_form = LoanFacilityForm(prefix='loan')
    rate_form = LoanInterestForm(prefix='rate', initial={'effective_date': timezone.now().date()})

    if request.method == 'POST':
        if 'add_loan' in request.POST:
            loan_form = LoanFacilityForm(request.POST, prefix='loan')
            if loan_form.is_valid():
                loan_form.save()
                messages.success(request, "New Loan Facility created!")
                return redirect('accounting_desk:bank_loan_report')
        
        elif 'add_rate' in request.POST:
            rate_form = LoanInterestForm(request.POST, prefix='rate')
            if rate_form.is_valid():
                rate_form.save()
                messages.success(request, "Interest rate updated!")
                return redirect('accounting_desk:bank_loan_report')

    # Calculate Totals
    loans = LoanFacility.objects.all()
    loan_data = []
    
    grand_total_sanctioned = Decimal(0)
    grand_total_dp = Decimal(0)
    grand_total_used = Decimal(0)

    for loan in loans:
        # Get latest status for every script in this loan to calculate totals
        # We find distinct scripts, then find the latest entry for each
        scripts = loan.pledge_entries.values_list('symbol', flat=True).distinct()
        current_dp = Decimal(0)
        current_utilized = Decimal(0)
        
        for script in scripts:
            # Get latest entry for this script + bank
            last = PledgeEntrySheet.objects.filter(
                loan_facility=loan, 
                symbol__script_ticker=script
            ).order_by('-date', '-created_at').first()
            
            if last:
                current_dp += last.cl_drawing_power
                current_utilized += last.cl_utilized
        
        loan_data.append({
            'obj': loan,
            'dp': current_dp,
            'utilized': current_utilized,
            'headroom': loan.sanctioned_limit - current_utilized,
            'util_percent': (current_utilized / loan.sanctioned_limit * 100) if loan.sanctioned_limit else 0
        })

        grand_total_sanctioned += loan.sanctioned_limit
        grand_total_dp += current_dp
        grand_total_used += current_utilized

    context = {
        'loan_data': loan_data,
        'loan_form': loan_form,
        'rate_form': rate_form,
        'grand_totals': {
            'sanctioned': grand_total_sanctioned,
            'dp': grand_total_dp,
            'used': grand_total_used,
            'headroom': grand_total_sanctioned - grand_total_used
        }
    }
    return render(request, 'accounting_desk/bank_loan_report.html', context)


@login_required
def pledge_entry_sheet(request):
    """
    Entry Sheet 4: The Main Transaction Sheet
    """
    selected_loan_id = request.GET.get('loan_facility')
    selected_symbol = request.GET.get('symbol')
    
    entries = []
    initial_data = {'date': timezone.now().date()}
    
    if selected_loan_id:
        initial_data['loan_facility'] = selected_loan_id
    if selected_symbol:
        initial_data['symbol'] = selected_symbol

    # Load existing entries if filters applied
    if selected_loan_id and selected_symbol:
        entries = PledgeEntrySheet.objects.filter(
            loan_facility_id=selected_loan_id,
            symbol__script_ticker=selected_symbol
        ).order_by('-date', '-created_at')

    if request.method == 'POST':
        form = PledgeEntrySheetForm(request.POST)
        if form.is_valid():
            # The model .save() method now handles all the complex logic (Opening/Closing balance)
            entry = form.save()
            messages.success(request, f"Entry {entry.unique_id} saved successfully!")
            return redirect(f"{request.path}?loan_facility={entry.loan_facility.id}&symbol={entry.symbol.script_ticker}")
        else:
            messages.error(request, f"Error: {form.errors}")
    else:
        form = PledgeEntrySheetForm(initial=initial_data)

    context = {
        'form': form,
        'ledger_entries': entries,
        'loans': LoanFacility.objects.all(),
        'scripts': Companies.objects.all().order_by('script_ticker'),
        'selected_loan_id': int(selected_loan_id) if selected_loan_id else None,
        'selected_symbol': selected_symbol,
    }
    return render(request, 'accounting_desk/pledge_entry_sheet.html', context)


@login_required
def manage_margins(request):
    """
    Entry Sheet 3: Margin Rules
    """
    rules = MarginRule.objects.all().select_related('loan_facility')
    if request.method == 'POST':
        form = MarginRuleForm(request.POST)
        if form.is_valid():
            form.save()
            messages.success(request, "Margin Rule Added")
            return redirect('accounting_desk:manage_margins')
    else:
        form = MarginRuleForm()
    return render(request, 'accounting_desk/margin_management.html', {'rules': rules, 'form': form})


# --- API ---
@login_required
def get_scrip_info(request):
    """
    Returns 180 Avg, CP, and Margin % for the JS frontend
    """
    symbol = request.GET.get('symbol')
    date_str = request.GET.get('date')
    loan_id = request.GET.get('loan_facility')
    
    if not symbol or not date_str: 
        return JsonResponse({'error': 'Missing params'}, status=400)
    
    try:
        target_date = datetime.strptime(date_str, '%Y-%m-%d').date()
        
        # 1. Get Closing Price
        price_obj = StockPrices.objects.filter(symbol=symbol, business_date__lte=target_date).order_by('-business_date').first()
        closing_price = float(price_obj.close_price) if price_obj else 0.0
        
        # 2. Get 180 Day Avg
        start_date = target_date - timedelta(days=180)
        # Note: Ideally you'd query specific trading days, but strictly between dates works for approximation
        prices = StockPrices.objects.filter(symbol=symbol, business_date__gte=start_date, business_date__lte=target_date).values_list('close_price', flat=True)
        avg_price = float(sum(prices) / len(prices)) if prices else 0.0
        
        # 3. Get Margin
        margin = 50.00
        if loan_id:
            rule = MarginRule.objects.filter(loan_facility_id=loan_id, symbol__script_ticker=symbol).first()
            if rule: 
                margin = float(rule.margin_percent)

        return JsonResponse({
            'closing_price': closing_price, 
            'avg_price': avg_price, 
            'margin': margin
        })
    except Exception as e:
        return JsonResponse({'error': str(e)}, status=500)


# --- HELPERS / DELETION ---
@login_required
def delete_pledge_entry(request, pk): 
    entry = get_object_or_404(PledgeEntrySheet, pk=pk)
    loan_id = entry.loan_facility.id
    symbol = entry.symbol.script_ticker
    entry.delete()
    messages.warning(request, "Entry deleted. Note: Future running balances may need re-saving.")
    return redirect(f"/accounting/loans/entry-sheet/?loan_facility={loan_id}&symbol={symbol}")

@login_required
def delete_margin(request, pk):
    MarginRule.objects.filter(pk=pk).delete()
    return redirect('accounting_desk:manage_margins')

# Placeholders for URL patterns not fully implemented yet
def edit_pledge_entry(request, pk): return redirect('accounting_desk:pledge_entry_sheet')
def delete_all_pledges(request): return redirect('accounting_desk:pledge_entry_sheet')
def upload_pledge_entries(request): return redirect('accounting_desk:pledge_entry_sheet')
def download_pledge_sample(request): return HttpResponse("Sample")
def delete_all_margins(request): return redirect('accounting_desk:manage_margins')
def upload_margins(request): return redirect('accounting_desk:manage_margins')
def download_margin_sample(request): return HttpResponse("Sample")
def sync_loan_valuations(request): return JsonResponse({'status': 'ok'})
def update_loan_usage(request): return JsonResponse({'status': 'ok'})
def edit_margin(request, pk): return redirect('accounting_desk:manage_margins')
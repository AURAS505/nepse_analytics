from django.shortcuts import render, redirect, get_object_or_404
from django.contrib.auth.decorators import login_required
from django.contrib import messages
from django.db.models import Sum
from django.http import JsonResponse, HttpResponse
from django.utils import timezone
from decimal import Decimal
from datetime import datetime, timedelta
from django.db import transaction
import csv
import io

# --- Local Imports ---
from .utils import recalculate_ledger 
from .models import (
    LoanFacility, 
    LoanInterestHistory, 
    MarginRule, 
    PledgeEntrySheet
)
from .forms import (
    LoanFacilityForm, 
    LoanInterestForm, 
    MarginRuleForm, 
    PledgeEntrySheetForm
)

# --- External Apps ---
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
    """
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
        scripts = loan.pledge_entries.values_list('symbol', flat=True).distinct()
        current_dp = Decimal(0)
        current_utilized = Decimal(0)
        
        for script in scripts:
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

    if selected_loan_id and selected_symbol:
        entries = PledgeEntrySheet.objects.filter(
            loan_facility_id=selected_loan_id,
            symbol__script_ticker=selected_symbol
        ).order_by('-date', '-created_at')

    if request.method == 'POST':
        form = PledgeEntrySheetForm(request.POST)
        if form.is_valid():
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
    Entry Sheet 3: Margin Rules with Search & Filter
    """
    # 1. Base Query
    rules = MarginRule.objects.all().select_related('loan_facility', 'symbol').order_by('loan_facility__bank_name', 'symbol__script_ticker')

    # 2. Capture Search Parameters
    q_bank = request.GET.get('q_bank', '').strip()
    q_script = request.GET.get('q_script', '').strip()

    # 3. Apply Filters
    if q_bank:
        rules = rules.filter(loan_facility__bank_name__icontains=q_bank)
    if q_script:
        rules = rules.filter(symbol__script_ticker__icontains=q_script)

    # 4. Handle Add Rule Form (POST)
    if request.method == 'POST':
        form = MarginRuleForm(request.POST)
        if form.is_valid():
            form.save()
            messages.success(request, "Margin Rule Added")
            return redirect('accounting_desk:manage_margins')
    else:
        form = MarginRuleForm()
    
    context = {
        'rules': rules, 
        'form': form,
        # Pass search terms back to template to keep input filled
        'q_bank': q_bank,
        'q_script': q_script
    }
    return render(request, 'accounting_desk/margin_management.html', context)


# --- API ---
@login_required
def get_scrip_info(request):
    symbol = request.GET.get('symbol')
    date_str = request.GET.get('date')
    loan_id = request.GET.get('loan_facility')
    
    if not symbol or not date_str: 
        return JsonResponse({'error': 'Missing params'}, status=400)
    
    try:
        target_date = datetime.strptime(date_str, '%Y-%m-%d').date()
        price_obj = StockPrices.objects.filter(symbol=symbol, business_date__lte=target_date).order_by('-business_date').first()
        closing_price = float(price_obj.close_price) if price_obj else 0.0
        
        start_date = target_date - timedelta(days=180)
        prices = StockPrices.objects.filter(symbol=symbol, business_date__gte=start_date, business_date__lte=target_date).values_list('close_price', flat=True)
        avg_price = float(sum(prices) / len(prices)) if prices else 0.0
        
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


# --- ENTRY MANAGEMENT ---

@login_required
def edit_pledge_entry(request, pk):
    entry = get_object_or_404(PledgeEntrySheet, pk=pk)
    original_loan_id = entry.loan_facility.id
    original_symbol = entry.symbol.script_ticker

    if request.method == 'POST':
        form = PledgeEntrySheetForm(request.POST, instance=entry)
        if form.is_valid():
            saved_entry = form.save()
            # Recalculate New Ledger
            recalculate_ledger(saved_entry.loan_facility.id, saved_entry.symbol.script_ticker)
            # Recalculate Old Ledger if changed
            if (saved_entry.loan_facility.id != original_loan_id) or (saved_entry.symbol.script_ticker != original_symbol):
                recalculate_ledger(original_loan_id, original_symbol)

            messages.success(request, "Entry updated and ledger recalculated.")
            return redirect('accounting_desk:pledge_entry_sheet')
    else:
        form = PledgeEntrySheetForm(instance=entry)

    return render(request, 'accounting_desk/edit_pledge.html', {'form': form, 'entry': entry})

@login_required
def delete_pledge_entry(request, pk): 
    entry = get_object_or_404(PledgeEntrySheet, pk=pk)
    loan_id = entry.loan_facility.id
    symbol = entry.symbol.script_ticker
    
    entry.delete()
    
    recalculate_ledger(loan_id, symbol)
    
    messages.warning(request, "Entry deleted. Ledger recalculated.")
    return redirect(f"/accounting/loans/entry-sheet/?loan_facility={loan_id}&symbol={symbol}")

@login_required
def delete_margin(request, pk):
    MarginRule.objects.filter(pk=pk).delete()
    return redirect('accounting_desk:manage_margins')


# --- UPLOADS & DOWNLOADS ---

@login_required
def download_margin_sample(request):
    response = HttpResponse(content_type='text/csv')
    response['Content-Disposition'] = 'attachment; filename="margin_rules_sample.csv"'
    writer = csv.writer(response)
    writer.writerow(['Date', 'Bank Name', 'Script', 'Margin', 'Remarks'])
    writer.writerow(['2023-11-26', 'Nabil Bank', 'NABIL', '50', 'Standard Rule'])
    writer.writerow(['2023-11-26', 'Global IME', 'GBIME', '40', 'High Risk Adjustment'])
    return response

@login_required
def upload_margins(request):
    if request.method == 'POST' and request.FILES.get('file'):
        csv_file = request.FILES['file']
        
        if not csv_file.name.endswith('.csv'):
            messages.error(request, 'Error: The file must be a .csv')
            return redirect('accounting_desk:manage_margins')

        try:
            decoded_file = csv_file.read().decode('utf-8').splitlines()
            reader = csv.DictReader(decoded_file)
            
            success_count = 0
            errors = []
            
            with transaction.atomic():
                for row_idx, row in enumerate(reader, start=2):
                    bank_name = row.get('Bank Name', '').strip()
                    script_ticker = row.get('Script', '').strip()
                    margin_str = row.get('Margin', '50').strip()
                    remarks = row.get('Remarks', '').strip()
                    
                    if not bank_name or not script_ticker:
                        continue 

                    # Find Bank
                    loan = LoanFacility.objects.filter(bank_name__iexact=bank_name).first()
                    if not loan:
                        loan = LoanFacility.objects.filter(bank_name__icontains=bank_name).first()
                    
                    # Find Company
                    company = Companies.objects.filter(script_ticker__iexact=script_ticker).first()

                    if loan and company:
                        MarginRule.objects.update_or_create(
                            loan_facility=loan,
                            symbol=company,
                            defaults={
                                'margin_percent': float(margin_str) if margin_str else 50.0,
                                'remarks': remarks
                            }
                        )
                        success_count += 1
                    else:
                        errors.append(f"Row {row_idx}: {script_ticker} @ {bank_name} - Not Found")

            if success_count > 0:
                messages.success(request, f"Success! Updated {success_count} margin rules.")
            if errors:
                messages.warning(request, f"Issues: {' | '.join(errors[:3])}")

        except Exception as e:
            messages.error(request, f"CRITICAL ERROR: {str(e)}")
            
    return redirect('accounting_desk:manage_margins')

@login_required
def upload_pledge_entries(request):
    if request.method == 'POST' and request.FILES.get('file'):
        csv_file = request.FILES['file']
        if not csv_file.name.endswith('.csv'):
            messages.error(request, 'Please upload a CSV file.')
            return redirect('accounting_desk:pledge_entry_sheet')

        try:
            data_set = csv_file.read().decode('UTF-8')
            io_string = io.StringIO(data_set)
            next(io_string) 
            
            affected_ledgers = set()

            for column in csv.reader(io_string, delimiter=',', quotechar='"'):
                date_str = column[0]
                bank_name = column[1]
                script_ticker = column[2]
                action = column[3].upper() 
                kitta = int(column[4] or 0)
                utilized = float(column[5] or 0)
                
                loan = LoanFacility.objects.filter(bank_name__icontains=bank_name).first()
                comp = Companies.objects.filter(script_ticker=script_ticker).first()

                if loan and comp:
                    PledgeEntrySheet.objects.create(
                        date=date_str,
                        loan_facility=loan,
                        symbol=comp,
                        action=action,
                        kitta=kitta,
                        tx_utilized=utilized,
                        tx_180_avg=0, tx_closing_price=0, tx_margin=50
                    )
                    affected_ledgers.add((loan.id, script_ticker))

            for loan_id, sym in affected_ledgers:
                recalculate_ledger(loan_id, sym)

            messages.success(request, f"Processed CSV. Updated {len(affected_ledgers)} ledgers.")
            
        except Exception as e:
            messages.error(request, f"Error processing CSV: {e}")

    return redirect('accounting_desk:pledge_entry_sheet')


# --- VALUATION SYNC ---

@login_required
def sync_loan_valuations(request):
    active_combos = PledgeEntrySheet.objects.values('loan_facility', 'symbol').distinct()
    count = 0
    today = timezone.now().date()

    for combo in active_combos:
        last_entry = PledgeEntrySheet.objects.filter(
            loan_facility_id=combo['loan_facility'],
            symbol_id=combo['symbol']
        ).order_by('-date', '-created_at').first()

        if last_entry and last_entry.cl_kitta > 0:
            price_obj = StockPrices.objects.filter(
                symbol=last_entry.symbol.script_ticker
            ).order_by('-business_date').first()
            
            if not price_obj: continue

            new_cp = Decimal(price_obj.close_price)
            
            if last_entry.tx_closing_price != new_cp:
                PledgeEntrySheet.objects.create(
                    date=today,
                    loan_facility_id=combo['loan_facility'],
                    symbol_id=combo['symbol'],
                    action='VALUATION',
                    kitta=0,
                    tx_utilized=0,
                    tx_180_avg=last_entry.tx_180_avg,
                    tx_closing_price=new_cp,
                    tx_margin=last_entry.tx_margin,
                    remarks=f"Auto-Sync Valuation @ Rs. {new_cp}"
                )
                recalculate_ledger(combo['loan_facility'], last_entry.symbol.script_ticker)
                count += 1

    return JsonResponse({'status': 'success', 'updated_count': count})

@login_required
def edit_margin(request, pk):
    """
    Edit a specific Margin Rule.
    """
    rule = get_object_or_404(MarginRule, pk=pk)
    if request.method == 'POST':
        form = MarginRuleForm(request.POST, instance=rule)
        if form.is_valid():
            form.save()
            messages.success(request, f"Margin rule for {rule.symbol.script_ticker} updated.")
            return redirect('accounting_desk:manage_margins')
    else:
        form = MarginRuleForm(instance=rule)
    
    return render(request, 'accounting_desk/edit_margin.html', {'form': form, 'rule': rule})

@login_required
def delete_margin(request, pk):
    """
    Delete a single rule.
    """
    rule = get_object_or_404(MarginRule, pk=pk)
    name = rule.symbol.script_ticker
    rule.delete()
    messages.warning(request, f"Rule for {name} deleted.")
    return redirect('accounting_desk:manage_margins')

@login_required
def delete_all_margins(request):
    """
    Delete ALL margin rules in the database.
    """
    if request.method == 'POST':
        count = MarginRule.objects.count()
        MarginRule.objects.all().delete()
        messages.error(request, f"Deleted ALL {count} margin rules.")
    return redirect('accounting_desk:manage_margins')

@login_required
def delete_margins_by_bank(request):
    """
    Delete all rules associated with a specific bank.
    """
    if request.method == 'POST':
        loan_id = request.POST.get('loan_facility')
        if loan_id:
            loan = get_object_or_404(LoanFacility, pk=loan_id)
            deleted_count, _ = MarginRule.objects.filter(loan_facility=loan).delete()
            if deleted_count > 0:
                messages.warning(request, f"Deleted {deleted_count} rules for {loan.bank_name}.")
            else:
                messages.info(request, f"No rules found for {loan.bank_name}.")
        else:
            messages.error(request, "No bank selected.")
            
    return redirect('accounting_desk:manage_margins')


# --- PLACEHOLDERS ---
def update_loan_usage(request): return JsonResponse({'status': 'ok'})

def delete_all_pledges(request): return redirect('accounting_desk:pledge_entry_sheet')

def download_pledge_sample(request): return HttpResponse("Sample")
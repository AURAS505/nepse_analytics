from django.shortcuts import render, redirect
from django.contrib import messages
from .models import LoanFacility, PledgedScrip, LoanInterestHistory
from .forms import LoanFacilityForm, PledgedScripForm, LoanInterestForm
from datetime import date
from django.shortcuts import render, redirect
from django.http import JsonResponse
from django.contrib import messages
from django.db.models import Sum
from .models import LoanFacility, PledgedScrip, LoanInterestHistory
from .forms import LoanFacilityForm, PledgedScripForm, LoanInterestForm
from my_portfolio.models import MeroShareHolding
from nepse_data.models import StockPrices


try:
    from nepse_data.models import DailyPrice 
except ImportError:
    DailyPrice = None

def get_latest_price(symbol):
    if DailyPrice:
        latest = DailyPrice.objects.filter(symbol=symbol).order_by('-date').first()
        if latest: return float(latest.close_price)
    return 0.0

# --- DASHBOARD ---
def accounting_dashboard(request):
    # Placeholder for main desk
    return render(request, 'accounting_desk/accounting_dashboard.html', {})

# --- 1. AJAX API: Get Scrip Info & Price ---
def get_scrip_info(request):
    """
    Called by the Add Pledge Modal to fetch:
    1. Free Balance from MeroShare (Validation)
    2. Latest Closing Price from StockPrices (Valuation)
    """
    demat_id = request.GET.get('demat_id')
    symbol = request.GET.get('symbol', '').upper()
    
    data = {
        'found': False,
        'free_balance': 0,
        'closing_price': 0,
        'business_date': 'N/A'
    }

    if demat_id and symbol:
        # A. Fetch Holding
        holding = MeroShareHolding.objects.filter(
            demat_account_id=demat_id, 
            symbol__script_ticker=symbol
        ).order_by('-snapshot_date').first()
        
        if holding:
            data['found'] = True
            data['current_balance'] = float(holding.current_balance)
            data['pledge_balance'] = float(holding.pledge_balance)
            data['free_balance'] = float(holding.free_balance)
            data['lockin_balance'] = float(holding.lockin_balance)
        
        # B. Fetch Price
        price_obj = StockPrices.objects.filter(symbol=symbol).order_by('-business_date').first()
        if price_obj:
            data['closing_price'] = float(price_obj.close_price)
            data['business_date'] = str(price_obj.business_date)
        else:
            # If no price found, we can't calculate margin properly
            data['closing_price'] = 0 
            
    return JsonResponse(data)



# --- 2. MAIN BANK LOAN REPORT VIEW ---
def bank_loan_report(request):
    
    # --- HANDLE FORM SUBMISSIONS ---
    if request.method == 'POST':
        
        # A. Add New Loan Facility
        if 'add_loan' in request.POST:
            form = LoanFacilityForm(request.POST)
            if form.is_valid():
                form.save()
                messages.success(request, "New Loan Facility Added Successfully!")
                return redirect('accounting_desk:bank_loan_report')
            else:
                messages.error(request, "Error adding loan. Please check inputs.")

        # B. Add Pledged Scrip (With Auto Valuation)
        elif 'add_pledge' in request.POST:
            form = PledgedScripForm(request.POST)
            if form.is_valid():
                pledge = form.save(commit=False)
                pledge.symbol = pledge.symbol.upper()
                
                # Fetch Closing Price Backend Side (Double Check)
                price_obj = StockPrices.objects.filter(symbol=pledge.symbol).order_by('-business_date').first()
                if price_obj:
                    pledge.closing_price = price_obj.close_price
                else:
                    # Fallback: If no market price, assume Avg Price to prevent crash, 
                    # but in reality you should block this.
                    pledge.closing_price = pledge.average_price 

                pledge.save()
                
                # Note: Ideally, you should also deduct this qty from MeroShareHolding 'free_balance' here.
                # For now, we are just recording the pledge.

                messages.success(request, f"Pledged {pledge.quantity} units of {pledge.symbol}. Limit Increased!")
                return redirect('accounting_desk:bank_loan_report')
            else:
                # If form is invalid (e.g. insufficient balance), show error
                for field, errors in form.errors.items():
                    for error in errors:
                        messages.error(request, f"{field}: {error}")

        # C. Update Interest Rate (Base + Premium)
        elif 'add_rate' in request.POST:
            form = LoanInterestForm(request.POST)
            if form.is_valid():
                hist = form.save()
                messages.success(request, f"Rate Updated: {hist.base_rate}% + {hist.premium}% = {hist.rate}%")
                return redirect('accounting_desk:bank_loan_report')
            else:
                messages.error(request, "Error updating rate.")

    # --- GENERATE REPORT DATA ---
    loans = LoanFacility.objects.prefetch_related('pledged_scrips', 'interest_history').all()
    
    grand_totals = {
        'total_sanctioned': 0,
        'total_used': 0,
        'total_drawing_power': 0,
        'total_collateral_value': 0,
    }

    processed_loans = []
    today = date.today()

    for loan in loans:
        # 1. Scrip Calculations
        loan_collateral_value = 0
        loan_drawing_power = 0
        scrips_data = []

        for scrip in loan.pledged_scrips.all():
            # Get real-time price for "Current Market Value" display
            # Note: 'scrip.closing_price' stored in DB is the price *at time of pledge* used for limit.
            # Here we want *current* market value for risk monitoring.
            current_price_obj = StockPrices.objects.filter(symbol=scrip.symbol).order_by('-business_date').first()
            ltp = float(current_price_obj.close_price) if current_price_obj else float(scrip.closing_price)
            
            # Current Market Value (for display)
            market_val = ltp * scrip.quantity
            
            # Allowable Drawing Power (Fixed at pledge time or re-calculated?)
            # Usually banks re-value quarterly. For now, we use the value stored in DB 'allowable_drawing_power'
            # which was calculated in models.py save() method.
            dp = float(scrip.allowable_drawing_power)

            scrip.ltp = ltp
            scrip.current_value = market_val
            scrip.current_dp = dp
            
            scrips_data.append(scrip)
            
            loan_collateral_value += market_val
            loan_drawing_power += dp

        # 2. Loan Metrics
        loan.calculated_collateral = loan_collateral_value
        loan.calculated_dp = loan_drawing_power
        loan.headroom = loan_drawing_power - float(loan.current_used_amount)
        
        # Utilization %
        if loan_drawing_power > 0:
            loan.utilization_percent = (float(loan.current_used_amount) / loan_drawing_power) * 100
        else:
            loan.utilization_percent = 0 if float(loan.current_used_amount) == 0 else 100

        # 3. Active Rate & Expiry
        loan.active_rate = loan.get_active_rate
        if loan.expiry_date:
            loan.days_to_expiry = (loan.expiry_date - today).days
        else:
            loan.days_to_expiry = 999

        loan.cached_scrips = scrips_data
        processed_loans.append(loan)

        # 4. Grand Totals
        grand_totals['total_sanctioned'] += float(loan.sanctioned_limit)
        grand_totals['total_used'] += float(loan.current_used_amount)
        grand_totals['total_drawing_power'] += loan_drawing_power
        grand_totals['total_collateral_value'] += loan_collateral_value

    grand_totals['total_headroom'] = grand_totals['total_drawing_power'] - grand_totals['total_used']

    # --- INITIALIZE FORMS ---
    loan_form = LoanFacilityForm()
    pledge_form = PledgedScripForm()
    interest_form = LoanInterestForm()

    context = {
        'loans': processed_loans,
        'grand_totals': grand_totals,
        'loan_form': loan_form,
        'pledge_form': pledge_form,
        'interest_form': interest_form,
    }
    
    return render(request, 'accounting_desk/bank_loan_report.html', context)



#end
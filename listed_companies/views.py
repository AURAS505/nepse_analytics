# listed_companies/views.py
from django.shortcuts import render, redirect, get_object_or_404
from django.http import JsonResponse, HttpResponse
from django.views.decorators.http import require_POST, require_GET
from django.contrib import messages
from django.db.models import Q
from django.utils import timezone
from django.conf import settings

# Import your models
from .models import Companies, ShareholdingPattern, LockInPeriod, CorporateAction
# Assumes you have a 'nepse_data' app with StockPrices. If not, comment this import out.
try:
    from nepse_data.models import StockPrices
except ImportError:
    StockPrices = None

import pandas as pd
import csv
from decimal import Decimal
from datetime import timedelta, datetime
import io

# ========================================
# SECTION 1: EXISTING CRUD VIEWS (Reconstructed)
# ========================================

def listed_company_view(request):
    """Display the list of all companies."""
    companies = Companies.objects.all().order_by('script_ticker')
    context = {
        'title': 'Listed Companies',
        'companies': companies
    }
    return render(request, 'listed_companies/listed_company.html', context)

@require_POST
def add_company_view(request):
    """Handle AJAX request to add a new company."""
    try:
        # Extract data from POST
        nepse_code = request.POST.get('nepse_code')
        script_ticker = request.POST.get('script_ticker')
        company_name = request.POST.get('company_name')
        sector = request.POST.get('sector')
        type_ = request.POST.get('type')
        status = request.POST.get('status')
        instrument = request.POST.get('instrument')
        par_value = request.POST.get('par_value')

        if Companies.objects.filter(nepse_code=nepse_code).exists():
            return JsonResponse({'status': 'error', 'message': f'Company with Code {nepse_code} already exists.'}, status=400)
        
        if Companies.objects.filter(script_ticker=script_ticker).exists():
            return JsonResponse({'status': 'error', 'message': f'Ticker {script_ticker} already exists.'}, status=400)

        Companies.objects.create(
            nepse_code=nepse_code,
            script_ticker=script_ticker,
            company_name=company_name,
            sector=sector,
            type=type_,
            status=status,
            instrument=instrument,
            par_value=par_value
        )
        return JsonResponse({'status': 'success', 'message': 'Company added successfully!'})
        
    except Exception as e:
        return JsonResponse({'status': 'error', 'message': str(e)}, status=500)

def edit_company_view(request, nepse_code):
    """Edit an existing company."""
    company = get_object_or_404(Companies, nepse_code=nepse_code)
    
    if request.method == 'POST':
        try:
            company.script_ticker = request.POST.get('script_ticker')
            company.company_name = request.POST.get('company_name')
            company.sector = request.POST.get('sector')
            company.type = request.POST.get('type')
            company.status = request.POST.get('status')
            company.instrument = request.POST.get('instrument')
            company.par_value = request.POST.get('par_value')
            company.save()
            messages.success(request, f"Company {company.script_ticker} updated successfully.")
            return redirect('listed_companies:list')
        except Exception as e:
            messages.error(request, f"Error updating company: {e}")
            
    return render(request, 'listed_companies/edit_company.html', {'company': company})

@require_POST
def delete_company_view(request, nepse_code):
    """Delete a specific company."""
    company = get_object_or_404(Companies, nepse_code=nepse_code)
    ticker = company.script_ticker
    company.delete()
    messages.success(request, f"Company {ticker} deleted successfully.")
    return redirect('listed_companies:list')

@require_POST
def delete_all_companies_view(request):
    """Delete ALL companies."""
    count = Companies.objects.count()
    Companies.objects.all().delete()
    messages.warning(request, f"All {count} companies have been deleted.")
    return redirect('listed_companies:list')

@require_POST
def upload_companies_view(request):
    """Upload companies via CSV/XLSX."""
    file = request.FILES.get('file')
    if not file:
        messages.error(request, "No file selected.")
        return redirect('listed_companies:list')

    try:
        if file.name.endswith('.csv'):
            df = pd.read_csv(file)
        else:
            df = pd.read_excel(file)

        # Normalize headers
        df.columns = df.columns.str.strip().str.lower().str.replace(' ', '_')
        
        success_count = 0
        updated_count = 0
        
        for _, row in df.iterrows():
            # Map your CSV columns to model fields. Adjust these keys based on your CSV structure.
            # Providing fallbacks for common column name variations
            nepse_code = str(row.get('nepse_code') or row.get('symbol') or row.get('scrip') or '')
            script_ticker = str(row.get('script_ticker') or row.get('symbol') or row.get('stock_symbol') or '')
            
            if not nepse_code or not script_ticker:
                continue

            obj, created = Companies.objects.update_or_create(
                nepse_code=nepse_code,
                defaults={
                    'script_ticker': script_ticker,
                    'company_name': row.get('company_name', script_ticker),
                    'sector': row.get('sector', 'Others'),
                    'type': row.get('type', 'Public'),
                    'status': row.get('status', 'Active'),
                    'instrument': row.get('instrument', 'Equity'),
                    'par_value': row.get('par_value', 100.00)
                }
            )
            if created:
                success_count += 1
            else:
                updated_count += 1
                
        messages.success(request, f"Upload Complete. Created: {success_count}, Updated: {updated_count}")
        
    except Exception as e:
        messages.error(request, f"Error processing file: {str(e)}")
        
    return redirect('listed_companies:list')

def download_companies_view(request):
    """Download companies as CSV."""
    response = HttpResponse(content_type='text/csv')
    response['Content-Disposition'] = 'attachment; filename="nepse_companies.csv"'
    
    writer = csv.writer(response)
    writer.writerow(['NEPSE Code', 'Ticker', 'Name', 'Sector', 'Type', 'Status', 'Instrument', 'Par Value'])
    
    for company in Companies.objects.all():
        writer.writerow([
            company.nepse_code, company.script_ticker, company.company_name,
            company.sector, company.type, company.status, 
            company.instrument, company.par_value
        ])
        
    return response

def download_sample_csv_view(request):
    """Download a sample CSV for uploading companies."""
    response = HttpResponse(content_type='text/csv')
    response['Content-Disposition'] = 'attachment; filename="sample_companies.csv"'
    writer = csv.writer(response)
    writer.writerow(['nepse_code', 'script_ticker', 'company_name', 'sector', 'type', 'status', 'instrument', 'par_value'])
    writer.writerow(['NABIL', 'NABIL', 'Nabil Bank Ltd.', 'Commercial Banks', 'Public', 'Active', 'Equity', '100'])
    return response

def download_sample_xlsx_view(request):
    """Download sample XLSX (using pandas for simplicity if available, else error)."""
    try:
        output = io.BytesIO()
        df = pd.DataFrame([{
            'nepse_code': 'NABIL', 'script_ticker': 'NABIL', 
            'company_name': 'Nabil Bank Ltd.', 'sector': 'Commercial Banks',
            'type': 'Public', 'status': 'Active', 'instrument': 'Equity', 'par_value': 100
        }])
        with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
            df.to_excel(writer, index=False)
        output.seek(0)
        
        response = HttpResponse(output.read(), content_type='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet')
        response['Content-Disposition'] = 'attachment; filename="sample_companies.xlsx"'
        return response
    except Exception:
        return download_sample_csv_view(request)

def check_missing_companies_view(request):
    """Check which companies are in Price data but missing in Companies table."""
    try:
        if StockPrices is None:
            return JsonResponse({'status': 'error', 'message': 'StockPrices model not found/imported.'})

        # Get all unique symbols from StockPrices
        price_symbols = set(StockPrices.objects.values_list('stock_symbol', flat=True).distinct())
        # Get all unique tickers from Companies
        listed_tickers = set(Companies.objects.values_list('script_ticker', flat=True))
        
        # Find difference
        missing = list(price_symbols - listed_tickers)
        missing.sort()
        
        return JsonResponse({'status': 'success', 'missing_companies': missing})
    except Exception as e:
        return JsonResponse({'status': 'error', 'message': str(e)})

# ========================================
# SECTION 2: NEW SHAREHOLDING VIEWS
# ========================================

def company_shareholding_view(request, script_ticker):
    """Display shareholding history for a specific company"""
    company = get_object_or_404(Companies, script_ticker=script_ticker.upper())
    
    shareholding_history = ShareholdingPattern.objects.filter(
        company_symbol=script_ticker.upper()
    ).order_by('-as_of_date')
    
    latest_shareholding = shareholding_history.first()
    
    active_locks = LockInPeriod.objects.filter(
        company_symbol=script_ticker.upper(),
        is_active=True,
        lock_in_end_date__gte=timezone.now().date()
    ).order_by('lock_in_end_date')
    
    recent_actions = CorporateAction.objects.filter(
        company_symbol=script_ticker.upper()
    ).order_by('-announcement_date')[:10]
    
    context = {
        'title': f'{company.company_name} - Shareholding',
        'company': company,
        'latest_shareholding': latest_shareholding,
        'shareholding_history': shareholding_history,
        'active_locks': active_locks,
        'recent_actions': recent_actions,
    }
    
    return render(request, 'listed_companies/company_shareholding.html', context)

def shareholding_dashboard_view(request):
    """Dashboard showing all companies with their latest shareholding data"""
    search = request.GET.get('search', '').strip()
    sector = request.GET.get('sector', '').strip()
    
    companies = Companies.objects.all()
    
    if search:
        companies = companies.filter(
            Q(script_ticker__icontains=search) | 
            Q(company_name__icontains=search)
        )
    
    if sector:
        companies = companies.filter(sector=sector)
    
    companies = companies.order_by('script_ticker')
    
    companies_data = []
    for company in companies:
        latest = ShareholdingPattern.objects.filter(
            company_symbol=company.script_ticker
        ).order_by('-as_of_date').first()
        
        companies_data.append({
            'company': company,
            'latest_shareholding': latest
        })
    
    all_sectors = Companies.objects.values_list('sector', flat=True).distinct().order_by('sector')
    
    context = {
        'title': 'Shareholding Dashboard',
        'companies_data': companies_data,
        'search': search,
        'sector': sector,
        'all_sectors': all_sectors,
    }
    
    return render(request, 'listed_companies/shareholding_dashboard.html', context)

def lock_in_dashboard_view(request):
    """Dashboard showing all active lock-in periods"""
    lock_in_type = request.GET.get('type', '')
    days_filter = request.GET.get('days', '')
    
    lock_ins = LockInPeriod.objects.filter(is_active=True)
    
    if lock_in_type:
        lock_ins = lock_ins.filter(lock_in_type=lock_in_type)
    
    today = timezone.now().date()
    
    if days_filter == '30':
        lock_ins = lock_ins.filter(
            lock_in_end_date__gte=today,
            lock_in_end_date__lte=today + timedelta(days=30)
        )
    elif days_filter == '90':
        lock_ins = lock_ins.filter(
            lock_in_end_date__gte=today,
            lock_in_end_date__lte=today + timedelta(days=90)
        )
    elif days_filter == 'expired':
        lock_ins = lock_ins.filter(lock_in_end_date__lt=today)
    
    lock_ins = lock_ins.order_by('lock_in_end_date')
    
    lock_ins_data = []
    for lock_in in lock_ins:
        lock_ins_data.append({
            'lock_in': lock_in,
            'company': lock_in.company,
            'days_remaining': lock_in.days_remaining,
            'is_expired': lock_in.is_expired
        })
    
    context = {
        'title': 'Lock-in Periods Dashboard',
        'lock_ins_data': lock_ins_data,
        'lock_in_type': lock_in_type,
        'days_filter': days_filter,
    }
    
    return render(request, 'listed_companies/lock_in_dashboard.html', context)

@require_POST
def upload_shareholding_csv(request):
    """Upload shareholding pattern data via CSV"""
    file = request.FILES.get('shareholding_file')
    if not file:
        messages.error(request, "No file selected.")
        return redirect('listed_companies:list')
    
    try:
        df = pd.read_csv(file, encoding='utf-8-sig')
        df.columns = df.columns.str.strip().str.lower()
        
        created = 0
        updated = 0
        failed = 0
        
        for index, row in df.iterrows():
            try:
                symbol = str(row.get('symbol', '')).strip().upper()
                date_str = row.get('as of date')
                if pd.isna(date_str):
                    continue
                as_of_date = pd.to_datetime(date_str).date()
                
                if not symbol:
                    failed += 1
                    continue
                
                if not Companies.objects.filter(script_ticker=symbol).exists():
                    failed += 1
                    continue
                
                data = {
                    'promoter_percentage': row.get('promoter %'),
                    'public_percentage': row.get('public %'),
                    'institutional_percentage': row.get('institutional %'),
                    'free_float_percentage': row.get('free float %'),
                    'total_shares': row.get('total shares'),
                    'source': row.get('source', ''),
                }
                
                # Clean decimal fields
                for key in ['promoter_percentage', 'public_percentage', 
                           'institutional_percentage', 'free_float_percentage']:
                    if key in data and pd.notna(data[key]):
                        try:
                            data[key] = Decimal(str(data[key]))
                        except:
                            data[key] = 0
                    else:
                        data[key] = 0
                
                if 'total_shares' in data and pd.notna(data['total_shares']):
                    try:
                        data['total_shares'] = int(float(str(data['total_shares']).replace(',', '')))
                    except:
                        data['total_shares'] = 0
                else:
                    data['total_shares'] = 0
                
                obj, is_created = ShareholdingPattern.objects.update_or_create(
                    company_symbol=symbol,
                    as_of_date=as_of_date,
                    defaults=data
                )
                
                if is_created:
                    created += 1
                else:
                    updated += 1
                    
            except Exception as e:
                print(f"Error on row {index}: {e}")
                failed += 1
        
        messages.success(request, f"Shareholding upload complete! Created: {created}, Updated: {updated}, Failed: {failed}")
    except Exception as e:
        messages.error(request, f"Error uploading file: {e}")
    
    return redirect('listed_companies:list')

@require_POST
def upload_lockin_csv(request):
    """Upload lock-in period data via CSV"""
    file = request.FILES.get('lockin_file')
    if not file:
        messages.error(request, "No file selected.")
        return redirect('listed_companies:list')
    
    try:
        df = pd.read_csv(file, encoding='utf-8-sig')
        df.columns = df.columns.str.strip().str.lower()
        
        created = 0
        failed = 0
        
        for index, row in df.iterrows():
            try:
                symbol = str(row.get('symbol', '')).strip().upper()
                
                if not symbol:
                    failed += 1
                    continue
                
                if not Companies.objects.filter(script_ticker=symbol).exists():
                    failed += 1
                    continue
                
                LockInPeriod.objects.create(
                    company_symbol=symbol,
                    lock_in_type=str(row.get('lock-in type', 'OTHER')).upper(),
                    locked_shares=int(float(str(row.get('locked shares', 0)).replace(',', ''))),
                    lock_in_start_date=pd.to_datetime(row.get('start date')).date(),
                    lock_in_end_date=pd.to_datetime(row.get('end date')).date(),
                    shareholder_name=row.get('shareholder name', ''),
                    description=row.get('description', ''),
                    is_active=True
                )
                created += 1
                
            except Exception as e:
                print(f"Error on row {index}: {e}")
                failed += 1
        
        messages.success(request, f"Lock-in upload complete! Created: {created}, Failed: {failed}")
    except Exception as e:
        messages.error(request, f"Error uploading file: {e}")
    
    return redirect('listed_companies:list')

def download_shareholding_sample_csv(request):
    """Download sample shareholding CSV template"""
    response = HttpResponse(content_type='text/csv')
    response['Content-Disposition'] = 'attachment; filename="shareholding_sample.csv"'
    writer = csv.writer(response)
    writer.writerow(['Symbol', 'As Of Date', 'Promoter %', 'Public %', 'Institutional %', 'Free Float %', 'Total Shares', 'Source'])
    writer.writerow(['NABIL', '2024-01-15', '51.00', '39.00', '10.00', '49.00', '241000000', 'AGM Report'])
    return response

def download_lockin_sample_csv(request):
    """Download sample lock-in CSV template"""
    response = HttpResponse(content_type='text/csv')
    response['Content-Disposition'] = 'attachment; filename="lockin_sample.csv"'
    writer = csv.writer(response)
    writer.writerow(['Symbol', 'Lock-in Type', 'Locked Shares', 'Start Date', 'End Date', 'Shareholder Name', 'Description'])
    writer.writerow(['NABIL', 'PROMOTER', '10000000', '2024-01-01', '2024-12-31', 'Nepal Bangladesh Bank', 'IPO Lock-in'])
    return response

def get_company_shareholding_json(request, script_ticker):
    """AJAX endpoint to get shareholding data"""
    try:
        company = Companies.objects.get(script_ticker=script_ticker.upper())
        latest = ShareholdingPattern.objects.filter(company_symbol=script_ticker.upper()).order_by('-as_of_date').first()
        
        if not latest:
            return JsonResponse({'status': 'error', 'message': 'No shareholding data found'})
        
        data = {
            'status': 'success',
            'company': {'symbol': company.script_ticker, 'name': company.company_name, 'sector': company.sector},
            'shareholding': {
                'as_of_date': latest.as_of_date.isoformat(),
                'promoter_percentage': str(latest.promoter_percentage) if latest.promoter_percentage else None,
                'public_percentage': str(latest.public_percentage) if latest.public_percentage else None,
                'institutional_percentage': str(latest.institutional_percentage) if latest.institutional_percentage else None,
                'free_float_percentage': str(latest.free_float_percentage) if latest.free_float_percentage else None,
                'total_shares': latest.total_shares,
            }
        }
        return JsonResponse(data)
    except Companies.DoesNotExist:
        return JsonResponse({'status': 'error', 'message': 'Company not found'}, status=404)
    except Exception as e:
        return JsonResponse({'status': 'error', 'message': str(e)}, status=500)

@require_POST
def mark_lockin_expired(request, lock_in_id):
    """Mark a lock-in period as inactive"""
    try:
        lock_in = get_object_or_404(LockInPeriod, id=lock_in_id)
        lock_in.is_active = False
        lock_in.save()
        messages.success(request, f"Lock-in for {lock_in.company_symbol} marked as inactive.")
    except Exception as e:
        messages.error(request, f"Error updating lock-in: {e}")
    return redirect('listed_companies:lock_in_dashboard')


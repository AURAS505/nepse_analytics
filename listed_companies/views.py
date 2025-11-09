# listed_companies/views.py
from django.shortcuts import render, redirect, get_object_or_404
from django.http import JsonResponse, HttpResponse
from django.views.decorators.http import require_POST
from django.contrib import messages
from .models import Companies
from nepse_data.models import StockPrices # We need this for the check
import pandas as pd
import csv
import io
from decimal import Decimal

def listed_company_view(request):
    """
    Displays the list of all companies. The 'Add Company' form
    is on this page, but its POST is handled by add_company_view.
    """
    companies = Companies.objects.all().order_by('script_ticker')
    context = {
        'title': 'Listed Companies',
        'companies': companies,
    }
    return render(request, 'listed_companies/listed_company.html', context)

@require_POST  # Ensures this view only accepts POST requests
def add_company_view(request):
    """
    Handles the AJAX POST request from the "Add New Company" modal.
    """
    try:
        nepse_code = request.POST.get('nepse_code').upper()
        script_ticker = request.POST.get('script_ticker').upper()

        # Check for duplicates
        if Companies.objects.filter(nepse_code=nepse_code).exists():
            return JsonResponse({"message": "Error: NEPSE Code already exists."}, status=409)
        if Companies.objects.filter(script_ticker=script_ticker).exists():
            return JsonResponse({"message": "Error: Script Ticker already exists."}, status=409)

        Companies.objects.create(
            nepse_code=nepse_code,
            script_ticker=script_ticker,
            company_name=request.POST.get('company_name'),
            sector=request.POST.get('sector'),
            type=request.POST.get('type'),
            status=request.POST.get('status'),
            instrument=request.POST.get('instrument'),
            par_value=Decimal(request.POST.get('par_value', '100.00'))
        )
        return JsonResponse({"message": "Company added successfully!"}, status=200)

    except Exception as e:
        return JsonResponse({"message": f"An error occurred: {e}"}, status=500)

def edit_company_view(request, nepse_code):
    """
    Handles both displaying the edit form (GET) and saving
    the changes (POST).
    """
    company = get_object_or_404(Companies, nepse_code=nepse_code)

    if request.method == 'POST':
        try:
            company.script_ticker = request.POST.get('script_ticker').upper()
            company.company_name = request.POST.get('company_name')
            company.sector = request.POST.get('sector')
            company.type = request.POST.get('type')
            company.status = request.POST.get('status')
            company.instrument = request.POST.get('instrument')
            company.par_value = Decimal(request.POST.get('par_value', '100.00'))
            company.save()

            messages.success(request, f"{company.company_name} updated successfully.")
            return redirect('listed_companies:list')
        except Exception as e:
            messages.error(request, f"Failed to update company: {e}")

    context = {
        'company': company
    }
    return render(request, 'listed_companies/edit_company.html', context)

@require_POST
def delete_company_view(request, nepse_code):
    company = get_object_or_404(Companies, nepse_code=nepse_code)
    try:
        company.delete()
        messages.success(request, f"{company.company_name} has been deleted.")
    except Exception as e:
        messages.error(request, f"Error deleting company: {e}")
    return redirect('listed_companies:list')

@require_POST
def delete_all_companies_view(request):
    try:
        count, _ = Companies.objects.all().delete()
        messages.success(request, f"Successfully deleted all {count} companies.")
    except Exception as e:
        messages.error(request, f"An error occurred: {e}")
    return redirect('listed_companies:list')

@require_POST
def upload_companies_view(request):
    """
    Handles the CSV/XLSX file upload.
    """
    file = request.FILES.get('file')
    if not file:
        messages.error(request, "No file selected.")
        return redirect('listed_companies:list')

    try:
        if file.name.endswith('.csv'):
            df = pd.read_csv(file)
        elif file.name.endswith(('.xlsx', '.xls')):
            df = pd.read_excel(file)
        else:
            messages.error(request, "Unsupported file type. Please upload CSV or XLSX.")
            return redirect('listed_companies:list')

        df.columns = [col.strip().lower() for col in df.columns]

        # Use lowercase and handle potential missing 'par value'
        required_headers = ['nepse code', 'script / ticker', 'company name', 'sector', 'type', 'status', 'instrument']

        if not all(header in df.columns for header in required_headers):
            missing = [h for h in required_headers if h not in df.columns]
            messages.error(request, f"File missing required columns: {', '.join(missing)}")
            return redirect('listed_companies:list')

        par_value_header = 'par value' if 'par value' in df.columns else None

        companies_to_update = []
        companies_to_create = []

        for index, row in df.iterrows():
            nepse_code = str(row['nepse code']).upper()
            par_value = row.get(par_value_header, 100.0)
            if pd.isna(par_value) or par_value == 0:
                par_value = 100.0

            company_data = {
                'script_ticker': str(row['script / ticker']).upper(),
                'company_name': str(row['company name']),
                'sector': str(row['sector']),
                'type': str(row['type']),
                'status': str(row['status']),
                'instrument': str(row['instrument']),
                'par_value': Decimal(par_value)
            }

            # We use update_or_create to handle duplicates
            Companies.objects.update_or_create(
                nepse_code=nepse_code,
                defaults=company_data
            )

        messages.success(request, f"Successfully uploaded and processed {len(df)} company records.")

    except Exception as e:
        messages.error(request, f"An error occurred during upload: {e}")

    return redirect('listed_companies:list')


def download_companies_view(request):
    response = HttpResponse(content_type='text/csv')
    response['Content-Disposition'] = 'attachment; filename="existing_companies.csv"'

    writer = csv.writer(response)
    headers = ["NEPSE CODE", "Script / Ticker", "Company Name", "Sector", "Type", "Status", "Instrument", "Par Value"]
    writer.writerow(headers)

    companies = Companies.objects.all().values_list(
        'nepse_code', 'script_ticker', 'company_name', 'sector', 
        'type', 'status', 'instrument', 'par_value'
    )
    for company in companies:
        writer.writerow(company)

    return response

def download_sample_csv_view(request):
    buffer = io.StringIO()
    writer = csv.writer(buffer)
    headers = ["NEPSE CODE", "Script / Ticker", "Company Name", "Sector", "Type", "Status", "Instrument", "Par Value"]
    sample_data = [
        ["NABIL", "NABIL", "Nabil Bank Ltd.", "Commercial Banks", "Public", "Active", "Equity", "100"],
        ["SBL", "SBL", "Siddhartha Bank Ltd.", "Commercial Banks", "Public", "Active", "Equity", "100"],
    ]
    writer.writerow(headers)
    writer.writerows(sample_data)

    response = HttpResponse(buffer.getvalue(), content_type='text/csv')
    response['Content-Disposition'] = 'attachment; filename="companies_sample.csv"'
    return response

def download_sample_xlsx_view(request):
    buffer = io.BytesIO()
    df = pd.DataFrame([
        ["NABIL", "NABIL", "Nabil Bank Ltd.", "Commercial Banks", "Public", "Active", "Equity", "100"],
        ["SBL", "SBL", "Siddhartha Bank Ltd.", "Commercial Banks", "Public", "Active", "Equity", "100"],
    ], columns=["NEPSE CODE", "Script / Ticker", "Company Name", "Sector", "Type", "Status", "Instrument", "Par Value"])

    with pd.ExcelWriter(buffer, engine='xlsxwriter') as writer:
        df.to_excel(writer, index=False, sheet_name='Companies')

    response = HttpResponse(buffer.getvalue(), content_type='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet')
    response['Content-Disposition'] = 'attachment; filename="companies_sample.xlsx"'
    return response

def check_missing_companies_view(request):
    """
    API endpoint to find companies in stock data that are
    not in the main companies list.
    """
    try:
        # This line is fine
        existing_tickers = set(Companies.objects.values_list('script_ticker', flat=True))

        # --- THIS IS THE CORRECTED LINE ---
        # We use .values_list(...).distinct() which works on all databases
        floorsheet_symbols = set(StockPrices.objects.values_list('symbol', flat=True).distinct())
        # --- END OF FIX ---

        missing_companies = sorted(list(floorsheet_symbols - existing_tickers))

        return JsonResponse({"status": "success", "missing_companies": missing_companies})
    except Exception as e:
        return JsonResponse({"status": "error", "message": str(e)}, status=500)
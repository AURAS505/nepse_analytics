# my_portfolio/views.py
from django.shortcuts import render, redirect, get_object_or_404
from django.http import JsonResponse, HttpResponse, Http404
from django.db import connection, transaction as db_transaction
from django.urls import reverse
from django.contrib.auth.decorators import login_required # Use Django's login
from django.views.decorators.http import require_POST
from django.utils import timezone
from django.contrib import messages # Use Django's messaging framework
from .models import Transaction
from listed_companies.models import Companies
from nepse_data.models import StockPrices
from .utils import calculate_pma_details, calculate_overall_portfolio
import pandas as pd
import csv
from io import TextIOWrapper, BytesIO
from decimal import Decimal
from collections import defaultdict
from datetime import datetime
import json
from my_portfolio.models import Transaction


# --- Helper ---
def dictfetchall(cursor):
    "Return all rows from a cursor as a dict"
    columns = [col[0] for col in cursor.description]
    return [
        dict(zip(columns, row))
        for row in cursor.fetchall()
    ]

# --- Views ---

@login_required
def portfolio_home(request):
    """
    Renders the Dashboard/Home page.
    Converted from Flask `home()` route
    """
    stats = {
        'total_companies': 0,
        'total_transactions': 0,
        'total_holdings': 0,
        'total_investment': Decimal('0.0'),
        'recent_transactions': []
    }

    try:
        with connection.cursor() as cursor:
            # Get total companies (from listed_companies app)
            cursor.execute("SELECT COUNT(*) as count FROM companies")
            stats['total_companies'] = cursor.fetchone()[0]

            # Get total transactions (from my_portfolio app)
            cursor.execute("SELECT COUNT(*) as count FROM my_portfolio_transaction")
            stats['total_transactions'] = cursor.fetchone()[0]

            # Get portfolio summary
            cursor.execute("""
                SELECT
                    COUNT(DISTINCT symbol) as holdings,
                    SUM(CASE
                        WHEN transaction_type IN ('Balance b/d', 'BUY', 'IPO', 'RIGHT', 'CONVERSION(+)', 'SUSPENSE(+)') THEN billed_amount
                        WHEN transaction_type IN ('SALE', 'CONVERSION(-)', 'SUSPENSE(-)') THEN -billed_amount
                        ELSE 0
                    END) as investment
                FROM my_portfolio_transaction
            """)
            portfolio = cursor.fetchone()
            stats['total_holdings'] = portfolio[0] or 0
            stats['total_investment'] = portfolio[1] or Decimal('0.0')

        # Get recent 5 transactions (using the ORM)
        stats['recent_transactions'] = Transaction.objects.all().order_by('-date', '-created_at')[:5]

    except Exception as e:
        print(f"Error fetching dashboard stats: {e}")
        messages.error(request, f"Could not load dashboard statistics: {e}")

    return render(request, 'my_portfolio/dashboard.html', {'stats': stats})


@login_required
def transaction_list_and_add(request):
    """
    Renders the Transactions page.
    Handles GET to list transactions and POST (AJAX) to add a new one.
    Converted from Flask `transactions()` and `submit_transaction()`
    """
    if request.method == 'POST':
        # This is an AJAX request to add a new transaction
        try:
            date = request.POST.get('date')
            symbol_ticker = request.POST.get('symbol').upper()
            transaction_type = request.POST.get('transaction_type')
            kitta = int(request.POST.get('kitta'))
            
            billed_amount_str = request.POST.get('billed_amount', '')
            billed_amount = Decimal(billed_amount_str) if billed_amount_str else None
            
            broker = request.POST.get('broker', '')

            try:
                company = Companies.objects.get(script_ticker=symbol_ticker)
            except Companies.DoesNotExist:
                return JsonResponse({"message": f"Invalid symbol. Company '{symbol_ticker}' not found in database."}, status=400)

            new_txn = Transaction(
                date=date,
                symbol=company,
                transaction_type=transaction_type,
                kitta=kitta,
                billed_amount=billed_amount,
                broker=broker
            )
            new_txn.save() # .save() method handles unique_id, script, sector, rate

            return JsonResponse({"message": "Transaction added successfully!", "unique_id": new_txn.unique_id}, status=200)

        except Exception as e:
            return JsonResponse({"message": f"Error: {str(e)}"}, status=500)

    # GET Request: Display the page
    transactions = Transaction.objects.all()
    companies = Companies.objects.all().order_by('script_ticker')
    
    context = {
        'transactions': transactions,
        'companies': companies,
        'transaction_choices': Transaction.TransactionType.choices 
    }
    return render(request, 'my_portfolio/transactions.html', context)


@login_required
def transaction_edit(request, unique_id):
    """
    Edit an existing transaction.
    Converted from Flask `edit_transaction()`
    """
    txn = get_object_or_404(Transaction, unique_id=unique_id)
    
    if request.method == 'POST':
        try:
            date = request.POST.get('date')
            symbol_ticker = request.POST.get('symbol').upper()
            transaction_type = request.POST.get('transaction_type')
            kitta = int(request.POST.get('kitta'))
            
            billed_amount_str = request.POST.get('billed_amount', '')
            billed_amount = Decimal(billed_amount_str) if billed_amount_str else None
            
            broker = request.POST.get('broker', '')

            try:
                company = Companies.objects.get(script_ticker=symbol_ticker)
            except Companies.DoesNotExist:
                messages.error(request, "Invalid symbol. Company not found.")
                return redirect('my_portfolio:transaction_edit', unique_id=unique_id)

            txn.date = date
            txn.symbol = company
            txn.transaction_type = transaction_type
            txn.kitta = kitta
            txn.billed_amount = billed_amount
            txn.broker = broker
            txn.save() # .save() will update script, sector, rate
            
            messages.success(request, "Transaction updated successfully.")
            return redirect('my_portfolio:transactions')
            
        except Exception as e:
            messages.error(request, f"Error updating transaction: {e}")

    # GET request
    companies = Companies.objects.all().order_by('script_ticker')
    context = {
        'transaction': txn,
        'companies': companies
    }
    return render(request, 'my_portfolio/edit_transaction.html', context)


@login_required
@require_POST
def transaction_delete(request, unique_id):
    """
    Deletes a transaction.
    Converted from Flask `delete_transaction()`
    """
    txn = get_object_or_404(Transaction, unique_id=unique_id)
    try:
        txn.delete()
        messages.success(request, "Transaction deleted.")
    except Exception as e:
        messages.error(request, f"Error deleting transaction: {e}")
    return redirect('my_portfolio:transactions')


@login_required
@require_POST
def transaction_delete_all(request):
    """
    Deletes all transactions from the database.
    Converted from Flask `delete_all_transactions()`
    """
    try:
        Transaction.objects.all().delete()
        messages.success(request, "All transactions have been deleted.")
    except Exception as e:
        messages.error(request, f"Error deleting all transactions: {e}")
    return redirect('my_portfolio:transactions')


@login_required
@require_POST
@db_transaction.atomic # Wraps the whole function in a database transaction
def transaction_upload(request):
    """
    Handles uploading CSV/XLSX files for bulk transaction data.
    Converted from Flask `upload_transactions()`
    """
    file = request.FILES.get('file')
    if not file:
        messages.error(request, "No file selected.")
        return redirect('my_portfolio:transactions')

    filename = file.name
    success_count = 0
    error_count = 0
    errors = []

    try:
        required_headers = ['Date', 'Symbol', 'Transaction Type', 'Kitta']
        
        if filename.endswith('.csv'):
            csv_file = TextIOWrapper(file, encoding='utf-8', errors='replace')
            reader = csv.DictReader(csv_file)
            reader.fieldnames = [header.strip() for header in reader.fieldnames]
            data_iter = enumerate(reader, start=2) # start=2 for header row
            headers = reader.fieldnames
            
        elif filename.endswith(('.xlsx', '.xls')):
            df = pd.read_excel(file, dtype=str).fillna('')
            df.columns = [col.strip() for col in df.columns]
            data_iter = df.iterrows() # This index starts at 0, so row_num = index + 2
            headers = df.columns
            
        else:
            messages.error(request, "Unsupported file type. Please upload a CSV or XLSX file.")
            return redirect('my_portfolio:transactions')

        if not all(header in headers for header in required_headers):
            missing_headers = [h for h in required_headers if h not in headers]
            messages.error(request, f"File missing required columns. Missing: {', '.join(missing_headers)}")
            return redirect('my_portfolio:transactions')

        valid_symbols = set(Companies.objects.values_list('script_ticker', flat=True))
        valid_types = set(Transaction.TransactionType.values)
        companies_cache = {c.script_ticker: c for c in Companies.objects.all()}

        for index, row in data_iter:
            row_num = index + 2 if filename.endswith(('.xlsx', '.xls')) else index
            
            try:
                date_str = str(row.get('Date', '')).split()[0].strip()
                date = datetime.strptime(date_str, '%Y-%m-%d').date()

                symbol = str(row.get('Symbol', '')).strip().upper()
                transaction_type = str(row.get('Transaction Type', '')).strip()
                kitta = int(str(row.get('Kitta', '')).strip())

                # --- ADD THESE LINES TO FIX CASE-SENSITIVITY ---
                if transaction_type.lower() == 'bonus':
                    transaction_type = 'BONUS'
                elif transaction_type.lower() == 'buy':
                    transaction_type = 'BUY'
                elif transaction_type.lower() == 'sale':
                    transaction_type = 'SALE'
                elif transaction_type.lower() == 'ipo':
                    transaction_type = 'IPO'
                elif transaction_type.lower() == 'right':
                    transaction_type = 'RIGHT'
                # --- END OF FIX ---

                billed_amount_str = str(row.get('Billed Amount', '')).strip()
                billed_amount = Decimal(billed_amount_str) if billed_amount_str else None

                broker = str(row.get('Broker', '')).strip() or None

                if symbol not in valid_symbols:
                    raise ValueError(f"Symbol '{symbol}' not found in companies database")
                if transaction_type not in valid_types:
                    raise ValueError(f"Invalid Transaction Type '{transaction_type}'")
                if kitta <= 0:
                    raise ValueError("Kitta must be a positive number")
                
                company = companies_cache[symbol]
                
                Transaction(
                    date=date,
                    symbol=company,
                    transaction_type=transaction_type,
                    kitta=kitta,
                    billed_amount=billed_amount,
                    broker=broker
                ).save()

                success_count += 1

            except Exception as e:
                 errors.append(f"Row {row_num}: Error - {str(e)}")
                 error_count += 1
                 continue

        if error_count > 0:
            db_transaction.set_rollback(True)
            error_message = f"Upload failed. {error_count} rows had errors. No transactions were added. Errors: {', '.join(errors[:5])}..."
            messages.error(request, error_message)
        else:
            messages.success(request, f"Upload successful! {success_count} transactions added.")
            
    except Exception as e:
        messages.error(request, f"An unexpected error occurred: {e}")
        
    return redirect('my_portfolio:transactions')


@login_required
def download_transaction_template(request, file_type):
    """
    Download sample transaction template (CSV or Excel).
    Converted from Flask `download_transaction_template()`
    """
    sample_data = [
        {'Date': '2025-07-16', 'Symbol': 'CGH', 'Transaction Type': 'Balance b/d', 'Kitta': 7570, 'Billed Amount': '8062958.40', 'Broker': '35'},
        {'Date': '2025-07-17', 'Symbol': 'HBL', 'Transaction Type': 'BUY', 'Kitta': 11050, 'Billed Amount': '2761399.28', 'Broker': '35'},
        {'Date': '2025-07-17', 'Symbol': 'BARUN', 'Transaction Type': 'SALE', 'Kitta': 10000, 'Billed Amount': '4156963.36', 'Broker': '89'},
        {'Date': '2025-07-17', 'Symbol': 'PRIN', 'Transaction Type': 'BONUS', 'Kitta': 4319, 'Billed Amount': '', 'Broker': ''},
    ]
    fieldnames = ['Date', 'Symbol', 'Transaction Type', 'Kitta', 'Billed Amount', 'Broker']

    if file_type == 'csv':
        output = TextIOWrapper(BytesIO(), encoding='utf-8', newline='')
        writer = csv.DictWriter(output, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(sample_data)
        output.flush()
        
        response = HttpResponse(output.buffer.getvalue(), content_type='text/csv')
        response['Content-Disposition'] = 'attachment; filename="transaction_template.csv"'
        return response

    elif file_type == 'excel':
        output = BytesIO()
        df = pd.DataFrame(sample_data, columns=fieldnames)
        with pd.ExcelWriter(output, engine='openpyxl') as writer:
            df.to_excel(writer, index=False, sheet_name='Transactions')
        output.seek(0)
        
        response = HttpResponse(output, content_type='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet')
        response['Content-Disposition'] = 'attachment; filename="transaction_template.xlsx"'
        return response

    return Http404("Invalid file type")


@login_required
def company_dashboard(request):
    """
    Shows a single-stock report page and overall portfolio stats.
    Converted from Flask `portfolio_dashboard()`
    """
    
    # --- Get latest price for ALL symbols ---
    latest_prices = {}
    with connection.cursor() as cursor:
        try:
            cursor.execute("""
                WITH RankedPrices AS (
                    SELECT 
                        symbol, 
                        close_price,
                        business_date,
                        ROW_NUMBER() OVER(PARTITION BY symbol ORDER BY business_date DESC) as rn
                    FROM stock_prices
                )
                SELECT symbol, close_price, business_date
                FROM RankedPrices 
                WHERE rn = 1;
            """)
            for row in dictfetchall(cursor):
                latest_prices[row['symbol']] = {
                    'close_price': row.get('close_price') or Decimal('0.0'),
                    'business_date': row.get('business_date')
                }
        except Exception as e:
            print(f"Error fetching latest prices: {e}")

    # --- Calculate Overall Portfolio Stats ---
    try:
        # Fetch all transactions as dicts
        with connection.cursor() as cursor:
            cursor.execute("""
                SELECT * FROM my_portfolio_transaction
                ORDER BY symbol, date, created_at
            """)
            all_transactions = dictfetchall(cursor)
        
        overall_stats, holdings_summary_list = calculate_overall_portfolio(all_transactions, latest_prices)
    except Exception as e:
        print(f"Error processing overall portfolio stats: {e}")
        messages.error(request, f"Could not calculate portfolio stats: {e}")
        overall_stats, holdings_summary_list = {}, []


    # --- Check for specific symbol report ---
    symbol = request.GET.get('symbol')
    company_info, detailed_calculations, summary_data = None, [], None
    
    if symbol:
        try:
            # Filter transactions for the selected symbol
            symbol_txns = [txn for txn in all_transactions if txn['symbol'] == symbol]
            
            if symbol_txns:
                company_info = {
                    'symbol': symbol,
                    'script': symbol_txns[0]['script'],
                    'sector': symbol_txns[0]['sector']
                }
                price_info = latest_prices.get(symbol, {})
                
                detailed_calculations, summary_data = calculate_pma_details(symbol_txns, price_info)
        except Exception as e:
             print(f"Error processing single stock details for {symbol}: {e}")
             messages.error(request, f"Could not generate report for {symbol}: {e}")

    # --- Render template ---
    context = {
        'holdings_list': holdings_summary_list,
        'overall_stats': overall_stats,
        'company': company_info, 
        'details': detailed_calculations, 
        'summary': summary_data,
        'current_symbol': symbol
    }
    return render(request, 'my_portfolio/company_dashboard.html', context)


@login_required
def api_company_details(request, symbol):
    """
    API endpoint to get company details by symbol.
    Converted from Flask `get_company_details()`
    """
    try:
        # --- THIS IS THE FIX ---
        # We change from a strict, case-sensitive check:
        # company = Companies.objects.get(script_ticker=symbol.upper())
        #
        # To a flexible, case-INsensitive check:
        company = Companies.objects.get(script_ticker__iexact=symbol)
        # --- END OF FIX ---
        
        return JsonResponse({
            'script_ticker': company.script_ticker,
            'company_name': company.company_name,
            'sector': company.sector
        })
    except Companies.DoesNotExist:
        return JsonResponse({"error": "Company not found"}, status=404)
    except Exception as e:
        return JsonResponse({"error": str(e)}, status=500)
    
# --- The duplicate 'home_view' function has been removed ---
# my_portfolio/views.py
from django.shortcuts import render, redirect, get_object_or_404
from django.http import JsonResponse, HttpResponse, Http404
from django.db import connection, transaction as db_transaction
from django.urls import reverse
from django.contrib.auth.decorators import login_required
from django.views.decorators.http import require_POST
from django.utils import timezone
from django.contrib import messages
from .models import Transaction
from listed_companies.models import Companies
from nepse_data.models import StockPrices
from .utils import calculate_pma_details, calculate_overall_portfolio
import pandas as pd
import csv
from io import TextIOWrapper, BytesIO
from decimal import Decimal, InvalidOperation
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
    Renders the Dashboard/Home page with comprehensive portfolio metrics and advanced tables.
    """
    stats = {
        'total_scrips_traded': 0,
        'total_holdings': 0,
        'available_shares': 0,
        'total_investment': Decimal('0.0'),
        'total_market_value': Decimal('0.0'),
        'total_profit_loss': Decimal('0.0'),
        'realized_pl': Decimal('0.0'),
        'unrealized_pl': Decimal('0.0'),
        
        # Crore keys
        'total_investment_crore': Decimal('0.0'),
        'total_market_value_crore': Decimal('0.0'),
        'total_profit_loss_crore': Decimal('0.0'),
        
        # Tables
        'top_investments': [],
        'top_gainers': [],
        'top_losers': []
    }
    
    # --- 1. Fetch Latest Market Stats ---
    try:
        with connection.cursor() as cursor:
            cursor.execute("""
                SELECT total_scrips_traded FROM marcap 
                ORDER BY business_date DESC LIMIT 1
            """)
            result = cursor.fetchone()
            stats['total_scrips_traded'] = result[0] if result and result[0] is not None else 0
    except Exception as e:
        print(f"Error fetching marcap: {e}")

    # --- 2. Calculate Portfolio Metrics ---
    try:
        # Get latest price for ALL symbols
        latest_prices = {}
        with connection.cursor() as cursor:
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

        # Fetch all transactions
        with connection.cursor() as cursor:
            # Net Cash Outflow
            cursor.execute("""
                SELECT 
                    SUM(kitta) AS total_kitta,
                    SUM(CASE
                        WHEN transaction_type IN ('Balance b/d', 'BUY', 'IPO', 'RIGHT', 'CONVERSION(+)', 'SUSPENSE(+)') THEN billed_amount
                        WHEN transaction_type IN ('SALE', 'CONVERSION(-)', 'SUSPENSE(-)') THEN -billed_amount
                        ELSE 0
                    END) AS total_investment_amount
                FROM my_portfolio_transaction
            """)
            summary_row = cursor.fetchone()
            stats['total_holdings'] = summary_row[0] or 0
            stats['total_investment'] = summary_row[1] or Decimal('0.0')

            # Fetch raw transactions
            cursor.execute("""
                SELECT * FROM my_portfolio_transaction
                ORDER BY symbol, date, created_at
            """)
            all_transactions = dictfetchall(cursor)

        # Run Core Calculation
        overall_stats, holdings_summary_list = calculate_overall_portfolio(all_transactions, latest_prices)
        
        # --- 3. Advanced Metrics for Tables ---
        
        # Calculate Sector Totals for weighting
        sector_book_values = defaultdict(Decimal)
        portfolio_book_value = overall_stats.get('book_value', Decimal('0.0'))
        
        for h in holdings_summary_list:
            # Ensure we use the correct sector name from the holding
            sector = h.get('sector', 'Unknown')
            sector_book_values[sector] += h['book_value']

        # Enrich holdings with calculated %
        enriched_holdings = []
        for h in holdings_summary_list:
            book_val = h['book_value']
            total_pl = h['realized_pl'] + h['unrealized_pl']
            sector = h.get('sector', 'Unknown')
            sec_book_val = sector_book_values[sector]
            
            # 1. Allocation % (For Top 10 Investments Table)
            # How much of the portfolio/sector is this stock?
            h['allocation_sector'] = (book_val / sec_book_val * 100) if sec_book_val > 0 else Decimal(0)
            h['allocation_total'] = (book_val / portfolio_book_value * 100) if portfolio_book_value > 0 else Decimal(0)
            
            # 2. Return Metrics (For Gainers/Losers Table)
            # Individual ROI: (PL / Investment)
            h['roi_individual'] = (total_pl / book_val * 100) if book_val > 0 else Decimal(0)
            
            # Sector Contribution: (Stock PL / Sector Investment) - proxy for sector impact
            h['contribution_sector'] = (total_pl / sec_book_val * 100) if sec_book_val > 0 else Decimal(0)
            
            # Portfolio Contribution: (Stock PL / Total Investment) - proxy for total impact
            h['contribution_total'] = (total_pl / portfolio_book_value * 100) if portfolio_book_value > 0 else Decimal(0)
            
            h['total_pl'] = total_pl
            enriched_holdings.append(h)

        # --- 4. Prepare Stats & Tables ---
        
        CRORE = Decimal('10000000.0')

        # Metrics
        if stats['total_investment']:
            stats['total_investment_crore'] = stats['total_investment'] / CRORE
            
        stats['total_market_value'] = overall_stats.get('market_value', Decimal('0.0'))
        if stats['total_market_value']:
            stats['total_market_value_crore'] = stats['total_market_value'] / CRORE

        stats['total_profit_loss'] = overall_stats.get('total_profit', Decimal('0.0'))
        if stats['total_profit_loss']:
            stats['total_profit_loss_crore'] = stats['total_profit_loss'] / CRORE
            
        stats['realized_pl'] = overall_stats.get('realized_pl', Decimal('0.0'))
        stats['unrealized_pl'] = overall_stats.get('unrealized_pl', Decimal('0.0'))

        stats['available_shares'] = sum(h['closing_kitta'] for h in holdings_summary_list)
        stats['holdings_count'] = len(holdings_summary_list)

        # Sort Tables
        # Top 10 by Investment Amount (Book Value)
        stats['top_investments'] = sorted(enriched_holdings, key=lambda x: x['book_value'], reverse=True)[:10]
        
        # Top Gainers (Sort by Total PL desc)
        gainers = [h for h in enriched_holdings if h['total_pl'] >= 0]
        stats['top_gainers'] = sorted(gainers, key=lambda x: x['total_pl'], reverse=True)[:5]
        
        # Top Losers (Sort by Total PL asc)
        losers = [h for h in enriched_holdings if h['total_pl'] < 0]
        stats['top_losers'] = sorted(losers, key=lambda x: x['total_pl'])[:10]

    except Exception as e:
        print(f"Error fetching dashboard stats: {e}")
        messages.error(request, f"Could not load portfolio statistics: {e}")
    
    return render(request, 'my_portfolio/dashboard.html', {'stats': stats})

# ... (rest of views.py remains unchanged: transaction_list_and_add, etc.) ...
@login_required
def transaction_list_and_add(request):
    # ... [Keep existing code] ...
    if request.method == 'POST':
        try:
            date = request.POST.get('date')
            symbol_ticker = request.POST.get('symbol', '').upper()
            transaction_type = request.POST.get('transaction_type')
            kitta_str = request.POST.get('kitta')
            
            if not date or not symbol_ticker or not transaction_type or not kitta_str:
                return JsonResponse({"message": "Error: Missing required fields."}, status=400)

            try:
                kitta = int(kitta_str)
                if kitta <= 0: raise ValueError("Kitta must be positive")
            except (ValueError, TypeError):
                return JsonResponse({"message": "Error: Kitta must be a valid positive number."}, status=400)

            billed_amount_str = request.POST.get('billed_amount', '')
            billed_amount = Decimal(billed_amount_str) if billed_amount_str else None
            broker = request.POST.get('broker', '') or None

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
            new_txn.save()
            return JsonResponse({"message": "Transaction added successfully!", "unique_id": new_txn.unique_id}, status=200)

        except Exception as e:
            return JsonResponse({"message": f"An unexpected server error occurred: {str(e)}"}, status=500)

    transactions = Transaction.objects.all()
    companies = Companies.objects.all().order_by('script_ticker')
    context = {'transactions': transactions, 'companies': companies, 'transaction_choices': Transaction.TransactionType.choices}
    return render(request, 'my_portfolio/transactions.html', context)

@login_required
def transaction_edit(request, unique_id):
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
            txn.save()
            messages.success(request, "Transaction updated successfully.")
            return redirect('my_portfolio:transactions')
        except Exception as e:
            messages.error(request, f"Error updating transaction: {e}")
    companies = Companies.objects.all().order_by('script_ticker')
    context = {'transaction': txn, 'companies': companies}
    return render(request, 'my_portfolio/edit_transaction.html', context)

@login_required
@require_POST
def transaction_delete(request, unique_id):
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
    try:
        Transaction.objects.all().delete()
        messages.success(request, "All transactions have been deleted.")
    except Exception as e:
        messages.error(request, f"Error deleting all transactions: {e}")
    return redirect('my_portfolio:transactions')

@login_required
@require_POST
@db_transaction.atomic
def transaction_upload(request):
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
            data_iter = enumerate(reader, start=2)
            headers = reader.fieldnames
        elif filename.endswith(('.xlsx', '.xls')):
            df = pd.read_excel(file, dtype=str).fillna('')
            df.columns = [col.strip() for col in df.columns]
            data_iter = df.iterrows()
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
                if transaction_type.lower() == 'bonus': transaction_type = 'BONUS'
                elif transaction_type.lower() == 'buy': transaction_type = 'BUY'
                elif transaction_type.lower() == 'sale': transaction_type = 'SALE'
                elif transaction_type.lower() == 'ipo': transaction_type = 'IPO'
                elif transaction_type.lower() == 'right': transaction_type = 'RIGHT'
                billed_amount_str = str(row.get('Billed Amount', '')).strip()
                billed_amount = Decimal(billed_amount_str) if billed_amount_str else None
                broker = str(row.get('Broker', '')).strip() or None
                if symbol not in valid_symbols: raise ValueError(f"Symbol '{symbol}' not found")
                if transaction_type not in valid_types: raise ValueError(f"Invalid Transaction Type '{transaction_type}'")
                if kitta <= 0: raise ValueError("Kitta must be positive")
                company = companies_cache[symbol]
                Transaction(date=date, symbol=company, transaction_type=transaction_type, kitta=kitta, billed_amount=billed_amount, broker=broker).save()
                success_count += 1
            except Exception as e:
                 errors.append(f"Row {row_num}: Error - {str(e)}")
                 error_count += 1
                 continue
        if error_count > 0:
            db_transaction.set_rollback(True)
            messages.error(request, f"Upload failed. {error_count} errors. First error: {errors[0]}")
        else:
            messages.success(request, f"Upload successful! {success_count} transactions added.")
    except Exception as e:
        messages.error(request, f"An unexpected error occurred: {e}")
    return redirect('my_portfolio:transactions')

@login_required
def download_transaction_template(request, file_type):
    sample_data = [
        {'Date': '2025-07-16', 'Symbol': 'CGH', 'Transaction Type': 'Balance b/d', 'Kitta': 7570, 'Billed Amount': '8062958.40', 'Broker': '35'},
        {'Date': '2025-07-17', 'Symbol': 'HBL', 'Transaction Type': 'BUY', 'Kitta': 11050, 'Billed Amount': '2761399.28', 'Broker': '35'},
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
    latest_prices = {}
    with connection.cursor() as cursor:
        try:
            cursor.execute("""
                WITH RankedPrices AS (
                    SELECT symbol, close_price, business_date,
                        ROW_NUMBER() OVER(PARTITION BY symbol ORDER BY business_date DESC) as rn
                    FROM stock_prices
                )
                SELECT symbol, close_price, business_date FROM RankedPrices WHERE rn = 1;
            """)
            for row in dictfetchall(cursor):
                latest_prices[row['symbol']] = {
                    'close_price': row.get('close_price') or Decimal('0.0'),
                    'business_date': row.get('business_date')
                }
        except Exception as e:
            print(f"Error fetching latest prices: {e}")
    try:
        with connection.cursor() as cursor:
            cursor.execute("SELECT * FROM my_portfolio_transaction ORDER BY symbol, date, created_at")
            all_transactions = dictfetchall(cursor)
        overall_stats, holdings_summary_list = calculate_overall_portfolio(all_transactions, latest_prices)
    except Exception as e:
        messages.error(request, f"Could not calculate portfolio stats: {e}")
        overall_stats, holdings_summary_list = {}, []
    symbol = request.GET.get('symbol')
    company_info, detailed_calculations, summary_data = None, [], None
    if symbol:
        try:
            symbol_txns = [txn for txn in all_transactions if txn['symbol'] == symbol]
            if symbol_txns:
                company_info = {'symbol': symbol, 'script': symbol_txns[0]['script'], 'sector': symbol_txns[0]['sector']}
                price_info = latest_prices.get(symbol, {})
                detailed_calculations, summary_data = calculate_pma_details(symbol_txns, price_info)
        except Exception as e:
             messages.error(request, f"Could not generate report for {symbol}: {e}")
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
    try:
        company = Companies.objects.get(script_ticker__iexact=symbol)
        return JsonResponse({'script_ticker': company.script_ticker, 'company_name': company.company_name, 'sector': company.sector})
    except Companies.DoesNotExist:
        return JsonResponse({"error": "Company not found"}, status=404)
    except Exception as e:
        return JsonResponse({"error": str(e)}, status=500)
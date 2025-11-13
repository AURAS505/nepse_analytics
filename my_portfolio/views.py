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
# --- RESTORED IMPORT ---
from .utils import calculate_pma_details, calculate_overall_portfolio 

import pandas as pd
import csv
from io import TextIOWrapper, BytesIO
from decimal import Decimal, ROUND_HALF_UP, InvalidOperation
from collections import defaultdict
from datetime import datetime, date
import json
import openpyxl
from openpyxl.styles import Font, Alignment, PatternFill, Border, Side
from openpyxl.utils import get_column_letter

# --- Helper Functions ---

def dictfetchall(cursor):
    "Return all rows from a cursor as a dict"
    columns = [col[0] for col in cursor.description]
    return [dict(zip(columns, row)) for row in cursor.fetchall()]

def fmt_currency_short(value):
    """
    Formats Decimal values to '1.20 C', '60.00 L', '1.50 T' or standard comma string.
    """
    if value is None: return "-"
    try:
        val = Decimal(str(value))
    except:
        return value
        
    if val == 0: return "-"
    
    abs_val = abs(val)
    
    if abs_val >= 10000000: # 1 Crore
        return f"{val/10000000:.2f}C"
    elif abs_val >= 100000: # 1 Lakh
        return f"{val/100000:.2f}L"
    elif abs_val >= 1000:   # 1 Thousand
        return f"{val/1000:.2f}T"
    else:
        return f"{val:,.0f}"

def _get_valuation_data(start_date, end_date):
    # 1. Fetch ALL Transactions up to end_date
    transactions = Transaction.objects.filter(
        date__lte=end_date
    ).select_related('symbol').order_by('symbol__sector', 'symbol__script_ticker', 'date', 'created_at')

    # 2. Fetch Prices
    latest_prices = {}
    try:
        with connection.cursor() as cursor:
            cursor.execute("""
                WITH RankedPrices AS (
                    SELECT symbol, close_price,
                        ROW_NUMBER() OVER(PARTITION BY symbol ORDER BY business_date DESC) as rn
                    FROM stock_prices
                    WHERE business_date <= %s
                )
                SELECT symbol, close_price FROM RankedPrices WHERE rn = 1;
            """, [end_date])
            for r in dictfetchall(cursor):
                latest_prices[r['symbol']] = Decimal(str(r['close_price']))
    except Exception as e:
        print(f"Error fetching prices: {e}")

    # 3. Group
    grouped_txns = defaultdict(list)
    for txn in transactions:
        grouped_txns[txn.symbol].append(txn)

    sector_grouped_data = defaultdict(list)
    sector_totals = defaultdict(lambda: {
        'op_kitta': 0, 'op_amt': Decimal('0.0'), 
        'buy_kitta': 0, 'buy_amt': Decimal('0.0'), 
        'bonus_kitta': 0, 'bonus_amt': Decimal('0.0'),
        'sale_kitta': 0, 'sale_amt': Decimal('0.0'), 
        'consumption': Decimal('0.0'), 'realized_pl': Decimal('0.0'), 
        'cl_kitta': 0, 'cl_cost': Decimal('0.0'), 
        'market_val': Decimal('0.0'), 'unrealized_pl': Decimal('0.0'),
        'total_pl': Decimal('0.0')
    })
    grand_totals = defaultdict(lambda: Decimal('0.0'))
    
    TYPE_OPENING = {'Balance b/d'}
    TYPE_SIMPLE_PURCHASE = {'BUY', 'CONVERSION(+)', 'SUSPENSE(+)'}
    TYPE_PROPORTIONAL = {'BONUS', 'RIGHT', 'IPO'}
    TYPE_SALES = {'SALE', 'CONVERSION(-)', 'SUSPENSE(-)'}

    # 4. Logic Loop
    for symbol_obj, txns in grouped_txns.items():
        row = {
            'company': symbol_obj.script_ticker,
            'company_name': symbol_obj.company_name,
            'sector': symbol_obj.sector,
            'op_kitta': 0, 'op_amt': Decimal('0.0'), 
            'buy_kitta': 0, 'buy_amt': Decimal('0.0'), 
            'bonus_kitta': 0, 'bonus_amt': Decimal('0.0'),
            'sale_kitta': 0, 'sale_amt': Decimal('0.0'), 
            'consumption': Decimal('0.0'), 'realized_pl': Decimal('0.0'), 
            'cl_kitta': 0, 'cl_cost': Decimal('0.0'), 
            'market_val': Decimal('0.0'), 'unrealized_pl': Decimal('0.0'),
            'total_pl': Decimal('0.0')
        }

        # --- STEP 1: Calculate Opening Balance (State strictly BEFORE start_date) ---
        global_kitta = 0
        global_cost = Decimal('0.0')
        
        for txn in txns:
            if txn.date < start_date:
                t_type = txn.transaction_type
                kitta = int(txn.kitta)
                amount = txn.billed_amount if txn.billed_amount else Decimal('0.0')
                
                if t_type in TYPE_OPENING or t_type in TYPE_SIMPLE_PURCHASE or t_type in TYPE_PROPORTIONAL:
                    global_kitta += kitta
                    global_cost += amount
                elif t_type in TYPE_SALES:
                    wacc = (global_cost / Decimal(global_kitta)) if global_kitta > 0 else Decimal('0.0')
                    cons = (Decimal(kitta) * wacc).quantize(Decimal('0.01'), rounding=ROUND_HALF_UP)
                    global_kitta -= kitta
                    global_cost -= cons
        
        # Set Opening Column
        row['op_kitta'] = global_kitta
        row['op_amt'] = global_cost

        # --- STEP 2: Process Period Transactions & Calculate Period WACC ---
        period_total_cost = row['op_amt']
        period_total_qty = row['op_kitta']
        period_sales = [] 
        
        for txn in txns:
            if txn.date >= start_date:
                t_type = txn.transaction_type
                kitta = int(txn.kitta)
                amount = txn.billed_amount if txn.billed_amount else Decimal('0.0')

                if t_type in TYPE_OPENING:
                    row['op_kitta'] += kitta
                    row['op_amt'] += amount
                    period_total_qty += kitta
                    period_total_cost += amount

                elif t_type in TYPE_SIMPLE_PURCHASE:
                    row['buy_kitta'] += kitta
                    row['buy_amt'] += amount
                    period_total_qty += kitta
                    period_total_cost += amount

                elif t_type in TYPE_PROPORTIONAL:
                    row['bonus_kitta'] += kitta 
                    if amount > 0: row['bonus_amt'] += amount
                    
                    period_total_qty += kitta
                    period_total_cost += amount
                    
                    if t_type != 'BONUS': # Right/IPO move to Buy col
                         row['buy_kitta'] += kitta
                         row['buy_amt'] += amount
                         row['bonus_kitta'] -= kitta # Undo bonus add

                elif t_type in TYPE_SALES:
                    period_sales.append((kitta, amount))

        # --- STEP 3: Calculate ONE Weighted Average Rate for the Period ---
        if period_total_qty > 0:
            period_wacc_rate = period_total_cost / Decimal(period_total_qty)
        else:
            period_wacc_rate = Decimal('0.0')

        # --- STEP 4: Process Sales using this Fixed Rate ---
        for kitta, amount in period_sales:
            sell_qty = kitta 
            cons = (Decimal(sell_qty) * period_wacc_rate).quantize(Decimal('0.01'), rounding=ROUND_HALF_UP)
            
            row['sale_kitta'] += sell_qty
            row['sale_amt'] += amount
            row['consumption'] += cons
            row['realized_pl'] += (amount - cons)
            
            # Reduce closing
            period_total_qty -= sell_qty
            period_total_cost -= cons

        # --- STEP 5: Final Closing ---
        row['cl_kitta'] = period_total_qty
        row['cl_cost'] = period_total_cost

        # --- STEP 6: Valuation ---
        ltp = latest_prices.get(symbol_obj.script_ticker, Decimal('0.0'))
        row['ltp'] = ltp
        row['market_val'] = (Decimal(row['cl_kitta']) * ltp).quantize(Decimal('0.01'), rounding=ROUND_HALF_UP)
        row['unrealized_pl'] = row['market_val'] - row['cl_cost']
        row['total_pl'] = row['realized_pl'] + row['unrealized_pl']

        # Rates
        row['op_rate'] = (row['op_amt'] / row['op_kitta']) if row['op_kitta'] else 0
        row['buy_rate'] = (row['buy_amt'] / row['buy_kitta']) if row['buy_kitta'] else 0
        row['bonus_rate'] = (row['bonus_amt'] / row['bonus_kitta']) if row['bonus_kitta'] else 0
        row['sale_rate'] = (row['sale_amt'] / row['sale_kitta']) if row['sale_kitta'] else 0
        row['cl_rate'] = (row['cl_cost'] / row['cl_kitta']) if row['cl_kitta'] else 0

        # Add to List
        if any([row['op_kitta'], row['buy_kitta'], row['bonus_kitta'], row['sale_kitta'], row['cl_kitta']]):
            sector_grouped_data[row['sector']].append(row)
            
            st = sector_totals[row['sector']]
            st['op_kitta'] += row['op_kitta']; st['op_amt'] += row['op_amt']
            st['buy_kitta'] += row['buy_kitta']; st['buy_amt'] += row['buy_amt']
            st['bonus_kitta'] += row['bonus_kitta']; st['bonus_amt'] += row['bonus_amt']
            st['sale_kitta'] += row['sale_kitta']; st['sale_amt'] += row['sale_amt']
            st['consumption'] += row['consumption']; st['realized_pl'] += row['realized_pl']
            st['cl_kitta'] += row['cl_kitta']; st['cl_cost'] += row['cl_cost']
            st['market_val'] += row['market_val']; st['unrealized_pl'] += row['unrealized_pl']
            st['total_pl'] += row['total_pl']

            grand_totals['op_amt'] += row['op_amt']
            grand_totals['buy_amt'] += row['buy_amt']
            grand_totals['sale_amt'] += row['sale_amt']
            grand_totals['realized_pl'] += row['realized_pl']
            grand_totals['cl_cost'] += row['cl_cost']
            grand_totals['market_val'] += row['market_val']
            grand_totals['unrealized_pl'] += row['unrealized_pl']
            grand_totals['total_pl'] += row['total_pl']

    sorted_sectors = sorted(sector_grouped_data.keys())
    sn_counter = 1
    final_data = {}
    for sector in sorted_sectors:
        rows = sector_grouped_data[sector]
        rows.sort(key=lambda x: x['company']) 
        for r in rows:
            r['sn'] = sn_counter
            sn_counter += 1
        final_data[sector] = {'rows': rows, 'totals': sector_totals[sector]}

    return final_data, grand_totals

# --- STANDARD VIEWS ---

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
        'total_investment_crore': Decimal('0.0'),
        'total_market_value_crore': Decimal('0.0'),
        'total_profit_loss_crore': Decimal('0.0'),
        'top_investments': [],
        'top_gainers': [],
        'top_losers': []
    }
    
    try:
        with connection.cursor() as cursor:
            cursor.execute("SELECT total_scrips_traded FROM marcap ORDER BY business_date DESC LIMIT 1")
            result = cursor.fetchone()
            stats['total_scrips_traded'] = result[0] if result and result[0] is not None else 0
    except Exception as e:
        print(f"Error fetching marcap: {e}")

    try:
        latest_prices = {}
        with connection.cursor() as cursor:
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

        with connection.cursor() as cursor:
            cursor.execute("""
                SELECT SUM(kitta),
                    SUM(CASE WHEN transaction_type IN ('Balance b/d', 'BUY', 'IPO', 'RIGHT', 'CONVERSION(+)', 'SUSPENSE(+)') THEN billed_amount
                        WHEN transaction_type IN ('SALE', 'CONVERSION(-)', 'SUSPENSE(-)') THEN -billed_amount ELSE 0 END)
                FROM my_portfolio_transaction
            """)
            summary_row = cursor.fetchone()
            stats['total_holdings'] = summary_row[0] or 0
            stats['total_investment'] = summary_row[1] or Decimal('0.0')

            cursor.execute("SELECT * FROM my_portfolio_transaction ORDER BY symbol, date, created_at")
            all_transactions = dictfetchall(cursor)

        overall_stats, holdings_summary_list = calculate_overall_portfolio(all_transactions, latest_prices)
        
        sector_book_values = defaultdict(Decimal)
        portfolio_book_value = overall_stats.get('book_value', Decimal('0.0'))
        for h in holdings_summary_list:
            sector = h.get('sector', 'Unknown')
            sector_book_values[sector] += h['book_value']

        enriched_holdings = []
        for h in holdings_summary_list:
            book_val = h['book_value']
            total_pl = h['realized_pl'] + h['unrealized_pl']
            sector = h.get('sector', 'Unknown')
            sec_book_val = sector_book_values[sector]
            h['allocation_sector'] = (book_val / sec_book_val * 100) if sec_book_val > 0 else Decimal(0)
            h['allocation_total'] = (book_val / portfolio_book_value * 100) if portfolio_book_value > 0 else Decimal(0)
            h['roi_individual'] = (total_pl / book_val * 100) if book_val > 0 else Decimal(0)
            h['contribution_sector'] = (total_pl / sec_book_val * 100) if sec_book_val > 0 else Decimal(0)
            h['contribution_total'] = (total_pl / portfolio_book_value * 100) if portfolio_book_value > 0 else Decimal(0)
            h['total_pl'] = total_pl
            enriched_holdings.append(h)

        CRORE = Decimal('10000000.0')
        if stats['total_investment']: stats['total_investment_crore'] = stats['total_investment'] / CRORE
        stats['total_market_value'] = overall_stats.get('market_value', Decimal('0.0'))
        if stats['total_market_value']: stats['total_market_value_crore'] = stats['total_market_value'] / CRORE
        stats['total_profit_loss'] = overall_stats.get('total_profit', Decimal('0.0'))
        if stats['total_profit_loss']: stats['total_profit_loss_crore'] = stats['total_profit_loss'] / CRORE
        stats['realized_pl'] = overall_stats.get('realized_pl', Decimal('0.0'))
        stats['unrealized_pl'] = overall_stats.get('unrealized_pl', Decimal('0.0'))
        stats['available_shares'] = sum(h['closing_kitta'] for h in holdings_summary_list)
        stats['holdings_count'] = len(holdings_summary_list)
        stats['top_investments'] = sorted(enriched_holdings, key=lambda x: x['book_value'], reverse=True)[:10]
        gainers = [h for h in enriched_holdings if h['total_pl'] >= 0]
        stats['top_gainers'] = sorted(gainers, key=lambda x: x['total_pl'], reverse=True)[:5]
        losers = [h for h in enriched_holdings if h['total_pl'] < 0]
        stats['top_losers'] = sorted(losers, key=lambda x: x['total_pl'])[:10]

    except Exception as e:
        messages.error(request, f"Could not load portfolio statistics: {e}")
    
    return render(request, 'my_portfolio/dashboard.html', {'stats': stats})

@login_required
def transaction_list_and_add(request):
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
                date=date, symbol=company, transaction_type=transaction_type,
                kitta=kitta, billed_amount=billed_amount, broker=broker
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
            txn.date = date; txn.symbol = company; txn.transaction_type = transaction_type
            txn.kitta = kitta; txn.billed_amount = billed_amount; txn.broker = broker
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
    success_count = 0; error_count = 0; errors = []
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
    sample_data = [{'Date': '2025-07-16', 'Symbol': 'CGH', 'Transaction Type': 'Balance b/d', 'Kitta': 7570, 'Billed Amount': '8062958.40', 'Broker': '35'}]
    fieldnames = ['Date', 'Symbol', 'Transaction Type', 'Kitta', 'Billed Amount', 'Broker']
    if file_type == 'csv':
        output = TextIOWrapper(BytesIO(), encoding='utf-8', newline='')
        writer = csv.DictWriter(output, fieldnames=fieldnames)
        writer.writeheader(); writer.writerows(sample_data); output.flush()
        response = HttpResponse(output.buffer.getvalue(), content_type='text/csv')
        response['Content-Disposition'] = 'attachment; filename="transaction_template.csv"'
        return response
    elif file_type == 'excel':
        output = BytesIO()
        df = pd.DataFrame(sample_data, columns=fieldnames)
        with pd.ExcelWriter(output, engine='openpyxl') as writer: df.to_excel(writer, index=False, sheet_name='Transactions')
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

# --- VALUATION VIEWS ---

@login_required
def valuation_report(request):
    end_date_str = request.GET.get('end_date')
    start_date_str = request.GET.get('start_date')
    if end_date_str: end_date = datetime.strptime(end_date_str, '%Y-%m-%d').date()
    else: 
        latest_price = StockPrices.objects.order_by('-business_date').first()
        end_date = latest_price.business_date if latest_price else timezone.now().date()
    if start_date_str: start_date = datetime.strptime(start_date_str, '%Y-%m-%d').date()
    else:
        first_txn = Transaction.objects.order_by('date').first()
        start_date = first_txn.date if first_txn else date(2000, 1, 1)

    raw_data, raw_grand_totals = _get_valuation_data(start_date, end_date)

    # Formatting for Web
    formatted_data = {}
    for sector, content in raw_data.items():
        new_rows = []
        for r in content['rows']:
            nr = r.copy()
            nr['op_amt'] = fmt_currency_short(r['op_amt'])
            nr['buy_amt'] = fmt_currency_short(r['buy_amt'])
            nr['bonus_amt'] = fmt_currency_short(r['bonus_amt'])
            nr['sale_amt'] = fmt_currency_short(r['sale_amt'])
            nr['consumption'] = fmt_currency_short(r['consumption'])
            nr['realized_pl'] = fmt_currency_short(r['realized_pl'])
            nr['cl_cost'] = fmt_currency_short(r['cl_cost'])
            nr['market_val'] = fmt_currency_short(r['market_val'])
            nr['unrealized_pl'] = fmt_currency_short(r['unrealized_pl'])
            new_rows.append(nr)
        
        t = content['totals']
        new_totals = t.copy()
        new_totals['op_amt'] = fmt_currency_short(t['op_amt'])
        new_totals['buy_amt'] = fmt_currency_short(t['buy_amt'])
        new_totals['bonus_amt'] = fmt_currency_short(t['bonus_amt'])
        new_totals['sale_amt'] = fmt_currency_short(t['sale_amt'])
        new_totals['consumption'] = fmt_currency_short(t['consumption'])
        new_totals['realized_pl'] = fmt_currency_short(t['realized_pl'])
        new_totals['cl_cost'] = fmt_currency_short(t['cl_cost'])
        new_totals['market_val'] = fmt_currency_short(t['market_val'])
        new_totals['unrealized_pl'] = fmt_currency_short(t['unrealized_pl'])
        
        formatted_data[sector] = {'rows': new_rows, 'totals': new_totals}

    formatted_grand_totals = {}
    for k, v in raw_grand_totals.items():
        formatted_grand_totals[k] = fmt_currency_short(v)

    context = {
        'valuation_data': formatted_data,
        'start_date': start_date,
        'end_date': end_date,
        'grand_totals': formatted_grand_totals,
    }
    return render(request, 'my_portfolio/valuation_report.html', context)

@login_required
def download_valuation_report(request):
    # 1. Date Logic
    end_date_str = request.GET.get('end_date')
    start_date_str = request.GET.get('start_date')
    if end_date_str: end_date = datetime.strptime(end_date_str, '%Y-%m-%d').date()
    else: 
        latest_price = StockPrices.objects.order_by('-business_date').first()
        end_date = latest_price.business_date if latest_price else timezone.now().date()
    if start_date_str: start_date = datetime.strptime(start_date_str, '%Y-%m-%d').date()
    else:
        first_txn = Transaction.objects.order_by('date').first()
        start_date = first_txn.date if first_txn else date(2000, 1, 1)

    # 2. Get Data
    data, totals = _get_valuation_data(start_date, end_date)

    # 3. Create Workbook
    wb = openpyxl.Workbook()
    ws = wb.active
    ws.title = "Valuation Report"
    
    # --- 2. GLOBAL SETTINGS ---
    ws.sheet_view.showGridLines = False
    ws.freeze_panes = "B3"
    ws.sheet_properties.outlinePr.summaryBelow = False
    
    # --- 3. STYLES ---
    font_header = Font(name='Calibri', size=9, bold=True)
    font_body = Font(name='Calibri', size=9)
    font_subtotal = Font(name='Calibri', size=9, bold=True)
    font_grand = Font(name='Calibri', size=10, bold=True, color="FFFFFF")
    
    align_center = Alignment(horizontal='center', vertical='center', wrap_text=True)
    align_right = Alignment(horizontal='right', vertical='center')
    align_left = Alignment(horizontal='left', vertical='center')
    
    fill_header = PatternFill(start_color="F8F9FA", end_color="F8F9FA", fill_type="solid")
    fill_subtotal = PatternFill(start_color="DFE1E5", end_color="DFE1E5", fill_type="solid") 
    fill_grand = PatternFill(start_color="000000", end_color="000000", fill_type="solid")
    fill_profit = PatternFill(start_color="D1E7DD", end_color="D1E7DD", fill_type="solid")
    fill_loss = PatternFill(start_color="F8D7DA", end_color="F8D7DA", fill_type="solid")

    thin_side = Side(style='thin', color="E2E2E2")
    thick_side = Side(style='medium', color="999999")
    
    num_fmt = '#,##0'
    dec_fmt = '#,##0.00'

    # --- 4. HEADERS ---
    ltp_header = f"LTP\n{end_date.strftime('%Y-%m-%d')}"
    
    headers_cat = [
        ("S.N.", 1), ("Symbol", 1), ("Company Name", 1),
        ("Opening", 3), ("Purchase", 3), ("Bonus", 3), ("Sales", 3), 
        ("Performance", 2), ("Closing (Cost)", 3), ("Market Valuation", 3), 
        ("Net P/L", 1)
    ]
    
    col = 1
    for title, span in headers_cat:
        cell = ws.cell(row=1, column=col, value=title)
        cell.font = font_header
        cell.alignment = align_center
        cell.fill = fill_header
        if col > 3: 
            cell.border = Border(left=thick_side, bottom=thin_side, top=thin_side, right=thin_side)
        else:
            cell.border = Border(bottom=thin_side, top=thin_side, right=thin_side, left=thin_side)
        if span > 1:
            ws.merge_cells(start_row=1, start_column=col, end_row=1, end_column=col+span-1)
        col += span

    headers_det = [
        "S.N.", "Symbol", "Company",
        "Qty", "Rate", "Amt", "Qty", "Rate", "Amt", "Qty", "Rate", "Amt", 
        "Qty", "Rate", "Amt", "Consump", "Real. P/L", "Qty", "WACC", "Cost", 
        ltp_header, "Value", "Unreal P/L", "Total Profit"
    ]
    section_starts = {4, 7, 10, 13, 16, 18, 21, 24}

    for c_idx, title in enumerate(headers_det, 1):
        cell = ws.cell(row=2, column=c_idx, value=title)
        cell.font = font_header
        cell.alignment = align_center
        cell.fill = fill_header
        left_style = thick_side if c_idx in section_starts else thin_side
        cell.border = Border(left=left_style, bottom=thick_side, right=thin_side)

    # --- 5. DATA ROWS ---
    current_row = 3
    
    for sector, content in data.items():
        # SECTOR HEADER (SUB TOTAL)
        ws.cell(row=current_row, column=2, value=sector).font = font_subtotal
        ws.cell(row=current_row, column=3, value="Sub Total").font = font_subtotal
        
        for c in range(1, 25):
            cell = ws.cell(row=current_row, column=c)
            cell.fill = fill_subtotal
            cell.font = font_subtotal
            left_s = thick_side if c in section_starts else None
            cell.border = Border(left=left_s, bottom=thin_side, top=thin_side)

        def write_sub(col, val, is_pl=False):
            c = ws.cell(row=current_row, column=col, value=val)
            c.number_format = num_fmt
            c.alignment = align_right
            left_s = thick_side if col in section_starts else None
            c.border = Border(left=left_s, bottom=thin_side, top=thin_side)
            if is_pl and val:
                if val < 0: c.font = Font(name='Calibri', size=9, bold=True, color="9C0006")
                elif val > 0: c.font = Font(name='Calibri', size=9, bold=True, color="006100")

        write_sub(4, content['totals']['op_kitta']); write_sub(6, content['totals']['op_amt'])
        write_sub(7, content['totals']['buy_kitta']); write_sub(9, content['totals']['buy_amt'])
        write_sub(10, content['totals']['bonus_kitta']); write_sub(12, content['totals']['bonus_amt'])
        write_sub(13, content['totals']['sale_kitta']); write_sub(15, content['totals']['sale_amt'])
        write_sub(16, content['totals']['consumption'])
        write_sub(17, content['totals']['realized_pl'], is_pl=True)
        write_sub(18, content['totals']['cl_kitta']); write_sub(20, content['totals']['cl_cost'])
        write_sub(22, content['totals']['market_val'])
        write_sub(23, content['totals']['unrealized_pl'], is_pl=True)
        write_sub(24, content['totals']['total_pl'], is_pl=True)
        
        current_row += 1

        # DATA ROWS
        # Grouping
        num_rows = len(content['rows'])
        if num_rows > 0:
            for r_idx in range(current_row, current_row + num_rows):
                ws.row_dimensions[r_idx].outlineLevel = 1

        for r in content['rows']:
            c1 = ws.cell(row=current_row, column=1, value=r['sn'])
            c1.alignment = align_center; c1.font = font_body; c1.border = Border(bottom=thin_side)
            
            c2 = ws.cell(row=current_row, column=2, value=r['company'])
            c2.alignment = align_left; c2.font = Font(name='Calibri', size=9, bold=True); c2.border = Border(bottom=thin_side)
            
            c3 = ws.cell(row=current_row, column=3, value=r['company_name'])
            c3.alignment = align_left; c3.font = font_body; c3.border = Border(bottom=thin_side)

            def write_val(col, val, fmt=num_fmt, is_pl=False):
                c = ws.cell(row=current_row, column=col, value=val)
                c.font = font_body
                c.number_format = fmt
                c.alignment = align_right
                left_s = thick_side if col in section_starts else None
                c.border = Border(left=left_s, bottom=thin_side)
                if is_pl and val:
                    if val < 0: c.fill = fill_loss
                    elif val > 0: c.fill = fill_profit

            write_val(4, r['op_kitta'] or 0); write_val(5, r['op_rate'], dec_fmt); write_val(6, r['op_amt'])
            write_val(7, r['buy_kitta'] or 0); write_val(8, r['buy_rate'], dec_fmt); write_val(9, r['buy_amt'])
            write_val(10, r['bonus_kitta'] or 0); write_val(11, r['bonus_rate'], dec_fmt); write_val(12, r['bonus_amt'])
            write_val(13, r['sale_kitta'] or 0); write_val(14, r['sale_rate'], dec_fmt); write_val(15, r['sale_amt'])
            write_val(16, r['consumption']); 
            write_val(17, r['realized_pl'], is_pl=True)
            write_val(18, r['cl_kitta']); write_val(19, r['cl_rate'], dec_fmt); write_val(20, r['cl_cost'])
            write_val(21, r['ltp'], dec_fmt); write_val(22, r['market_val'])
            write_val(23, r['unrealized_pl'], is_pl=True)
            write_val(24, r['total_pl'], is_pl=True)
            
            current_row += 1

    # --- 6. GRAND TOTAL ROW ---
    ws.cell(row=current_row, column=2, value="GRAND TOTAL").font = font_grand
    ws.cell(row=current_row, column=3, value="GRAND TOTAL").font = font_grand
    
    for c in range(1, 25):
        cell = ws.cell(row=current_row, column=c)
        cell.fill = fill_grand
        cell.border = Border(top=Side(style='medium'), bottom=Side(style='medium'))
        if c not in [2,3]: cell.value = ""

    def write_grand(col, val, is_pl=False):
        c = ws.cell(row=current_row, column=col, value=val)
        c.font = font_grand
        c.number_format = num_fmt
        c.alignment = align_right
        c.fill = fill_grand
        left_s = Side(style='medium', color="FFFFFF") if col in section_starts else None
        c.border = Border(left=left_s, top=thick_side, bottom=thick_side)
        if is_pl and val:
            if val < 0: c.font = Font(name='Calibri', size=10, bold=True, color="FF9999")
            elif val > 0: c.font = Font(name='Calibri', size=10, bold=True, color="99FF99")

    write_grand(6, totals['op_amt'])
    write_grand(9, totals['buy_amt'])
    write_grand(15, totals['sale_amt'])
    write_grand(17, totals['realized_pl'], True)
    write_grand(20, totals['cl_cost'])
    write_grand(22, totals['market_val'])
    write_grand(23, totals['unrealized_pl'], True)
    write_grand(24, totals['total_pl'], True)

    # --- 7. COLUMN WIDTHS ---
    ws.column_dimensions['A'].width = 5
    ws.column_dimensions['B'].width = 8
    ws.column_dimensions['C'].width = 20
    for c in range(4, 25):
        ws.column_dimensions[get_column_letter(c)].width = 12

    response = HttpResponse(content_type='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet')
    response['Content-Disposition'] = f'attachment; filename=Valuation_Report_{end_date}.xlsx'
    wb.save(response)
    return response
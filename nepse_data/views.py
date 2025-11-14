# nepse_data/views.py
from django.shortcuts import render, redirect
from django.db import connection
from django.db.models import Q, Max
from listed_companies.models import Companies
import datetime
from django.http import HttpResponse
from django.contrib import messages
from django.views.decorators.http import require_POST
from adjustments_stock_price.models import StockPricesAdj
import io
import csv
import pandas as pd  # <-- THIS WAS THE MISSING IMPORT
from decimal import Decimal, InvalidOperation
from django.core.paginator import Paginator
from django.db.models import Q, Max, Sum
from .models import StockPrices, Indices, Marcap, FloorsheetRaw, DividendHistory
from django.http import JsonResponse
from django.db.models import Value
from django.db.models.functions import Concat


# ==================================
# --- CONSOLIDATED HELPER FUNCTIONS ---
# ==================================

def clean_decimal(value):
    """
    Safely converts any input (str, float, int) with commas 
    to a Decimal or None.
    """
    if value is None: return None
    if not isinstance(value, str):
        value = str(value)
    if value.strip() in ('', 'N/A', '-'):
        return None
    try:
        return Decimal(value.replace(',', ''))
    except (InvalidOperation, ValueError, TypeError):
        print(f"Could not convert '{value}' to Decimal")
        return None

def clean_int(value):
    """
    Safely converts any input (str, float, int) with commas 
    and .00 decimals to an Integer or None.
    """
    if value is None: return None
    if not isinstance(value, str):
        value = str(value)
    if value.strip() in ('', 'N/A', '-'):
        return None
    try:
        # Use float first to handle "40,591.00"
        return int(float(value.replace(',', '')))
    except (InvalidOperation, ValueError, TypeError):
        print(f"Could not convert '{value}' to Integer")
        return None

# A helper function to fetch raw SQL as a dictionary
def dictfetchall(cursor):
    "Return all rows from a cursor as a dict"
    columns = [col[0] for col in cursor.description]
    return [
        dict(zip(columns, row))
        for row in cursor.fetchall()
    ]

def clean_date(value):
    """
    Safely converts any input (str, datetime, etc.) to a Date object or None.
    Handles NaT, None, empty strings, or common Excel null dates like '1900-01-00'.
    """
    if not value or pd.isna(value):
        return None
    
    # Convert to string to handle various inputs and check for known bad dates
    value_str = str(value).strip()
    
    # Pre-emptively catch empty strings or common "bad null" dates
    if value_str in ('', '1900-01-00', 'NaT'):
        return None
        
    try:
        # pd.to_datetime is very flexible with input formats
        return pd.to_datetime(value_str).date()
    except (ValueError, TypeError):
        # This will now only catch *truly* unexpected date formats
        print(f"Could not convert '{value_str}' to Date")
        return None


def buyer_seller_to_int(value):
    """Converts buyer/seller value, handling 'D01'/'D02' specifically."""
    if str(value).strip() == 'D01':
        return 60
    elif str(value).strip() == 'D02':
        return 77
    try:
        return int(value)
    except (ValueError, TypeError):
        return None
# ==================================
# --- ALL YOUR VIEWS (Corrected) ---
# ==================================

def todays_price_view(request):
    # (This view is unchanged and correct)
    view = request.GET.get('view', 'date')
    selected_date_str = request.GET.get('selected_date')
    search_term = request.GET.get('search_term', '').strip()
    stock_prices = []
    query_date = None
    title = "Stock Prices"
    all_companies = Companies.objects.values('script_ticker', 'company_name').order_by('script_ticker')
    if not selected_date_str:
        latest_date_result = StockPrices.objects.aggregate(max_date=Max('business_date'))
        if latest_date_result['max_date']:
            query_date = latest_date_result['max_date']
            selected_date_str = query_date.isoformat()
    else:
        try:
            query_date = datetime.date.fromisoformat(selected_date_str)
        except ValueError:
            query_date = None
    if view == 'date' and query_date:
        title = f"Stock Prices by Date"
        stock_prices = StockPrices.objects.filter(
            business_date=query_date
        ).order_by('symbol')
    elif view == 'company' and search_term:
        title = f"Price History for {search_term}"
        stock_prices = StockPrices.objects.filter(
            Q(symbol=search_term) | Q(security_name=search_term)
        ).order_by('-business_date', 'symbol')
    elif view == 'adjusted' and query_date:
        title = "Adjusted Stock Prices by Date"
        query = """
        WITH PricesToday AS (
            SELECT 
                p.id, p.business_date, p.symbol, p.security_name,
                p.total_traded_quantity, p.total_trades, p.market_capitalization, p.close_price,
                p.fifty_two_week_high, p.fifty_two_week_low,
                COALESCE(adj.open_price_adj, p.open_price) as open_price_adj,
                COALESCE(adj.high_price_adj, p.high_price) as high_price_adj,
                COALESCE(adj.low_price_adj, p.low_price) as low_price_adj,
                COALESCE(adj.close_price_adj, p.close_price) as close_price_adj,
                COALESCE(adj.average_traded_price_adj, p.average_traded_price) as average_traded_price_adj
            FROM stock_prices p
            LEFT JOIN stock_prices_adj adj ON p.id = adj.id
            WHERE p.business_date = %s
        ),
        WindowStats AS (
            SELECT
                symbol,
                MAX(high_price_adj) as calculated_52w_high,
                MIN(low_price_adj) as calculated_52w_low
            FROM stock_prices_adj
            WHERE business_date BETWEEN DATE_SUB(%s, INTERVAL 364 DAY) AND %s
            GROUP BY symbol
        )
        SELECT 
            pt.*,
            COALESCE(ws.calculated_52w_high, pt.fifty_two_week_high) as fifty_two_week_high_adj,
            COALESCE(ws.calculated_52w_low, pt.fifty_two_week_low) as fifty_two_week_low_adj
        FROM PricesToday pt
        LEFT JOIN WindowStats ws ON pt.symbol = ws.symbol
        ORDER BY pt.symbol
        """
        with connection.cursor() as cursor:
            cursor.execute(query, [query_date, query_date, query_date])
            stock_prices = dictfetchall(cursor)
    elif view == 'corporate' and search_term:
        title = f"Adjusted Price History for {search_term}"
        matching_symbols = list(Companies.objects.filter(
            Q(script_ticker=search_term) | Q(company_name=search_term)
        ).values_list('script_ticker', flat=True))
        if matching_symbols:
            format_strings = ','.join(['%s'] * len(matching_symbols))
            query = f"""
            SELECT
                p.id, p.business_date, p.symbol,
                COALESCE(adj.open_price_adj, p.open_price) as open_price_adj,
                COALESCE(adj.high_price_adj, p.high_price) as high_price_adj,
                COALESCE(adj.low_price_adj, p.low_price) as low_price_adj,
                COALESCE(adj.close_price_adj, p.close_price) as close_price_adj,
                COALESCE(adj.average_traded_price_adj, p.average_traded_price) as average_traded_price_adj,
                MAX(COALESCE(adj.high_price_adj, p.high_price)) OVER (
                    PARTITION BY p.symbol ORDER BY p.business_date
                    ROWS BETWEEN 364 PRECEDING AND CURRENT ROW
                ) as fifty_two_week_high_adj,
                MIN(COALESCE(adj.low_price_adj, p.low_price)) OVER (
                    PARTITION BY p.symbol ORDER BY p.business_date
                    ROWS BETWEEN 364 PRECEDING AND CURRENT ROW
                ) as fifty_two_week_low_adj,
                p.total_traded_quantity, p.total_trades,
                p.market_capitalization, p.close_price
            FROM
                stock_prices p
            LEFT JOIN
                stock_prices_adj adj ON p.id = adj.id
            WHERE
                p.symbol IN ({format_strings})
            ORDER BY
                p.symbol, p.business_date DESC
            """
            with connection.cursor() as cursor:
                cursor.execute(query, matching_symbols)
                stock_prices = dictfetchall(cursor)
    if view == 'adjusted' or view == 'corporate':
        for price in stock_prices:
            no_of_shares = None
            try:
                market_cap = price.get('market_capitalization')
                close_price = price.get('close_price')
                if not market_cap: 
                    market_cap = price.market_capitalization
                    close_price = price.close_price
                if market_cap is not None and close_price is not None and close_price > 0:
                    no_of_shares = round((market_cap * 1000000) / close_price)
            except Exception:
                pass 
            if isinstance(price, dict):
                price['no_of_shares'] = no_of_shares
            else:
                price.no_of_shares = no_of_shares
    context = {
        'stock_prices': stock_prices,
        'title': title,
        'selected_date_str': selected_date_str,
        'view': view,
        'search_term': search_term,
        'all_companies': all_companies,
    }
    return render(request, 'nepse_data/todays_price.html', context)

def download_stock_prices_view(request):
    # (This view is unchanged and correct)
    view = request.GET.get('view', 'date')
    selected_date_str = request.GET.get('selected_date')
    search_term = request.GET.get('search_term', '').strip()
    stock_prices = []
    query_date = None
    download_name = "stock_prices.csv"
    if not selected_date_str:
        latest_date_result = StockPrices.objects.aggregate(max_date=Max('business_date'))
        if latest_date_result['max_date']:
            query_date = latest_date_result['max_date']
            selected_date_str = query_date.isoformat()
    else:
        try:
            query_date = datetime.date.fromisoformat(selected_date_str)
        except ValueError:
            query_date = None
    if view == 'date' and query_date:
        stock_prices = StockPrices.objects.filter(business_date=query_date).order_by('symbol')
        download_name = f'stock_prices_unadjusted_{query_date}.csv'
    elif view == 'company' and search_term:
        stock_prices = StockPrices.objects.filter(
            Q(symbol=search_term) | Q(security_name=search_term)
        ).order_by('-business_date', 'symbol')
        download_name = f"stock_prices_unadjusted_{search_term.replace(' ', '_')}.csv"
    elif view == 'adjusted' and query_date:
        query = """
        WITH PricesToday AS (
            SELECT 
                p.id, p.business_date, p.symbol, p.security_name,
                p.total_traded_quantity, p.total_trades, p.market_capitalization, p.close_price,
                p.fifty_two_week_high, p.fifty_two_week_low,
                COALESCE(adj.open_price_adj, p.open_price) as open_price_adj,
                COALESCE(adj.high_price_adj, p.high_price) as high_price_adj,
                COALESCE(adj.low_price_adj, p.low_price) as low_price_adj,
                COALESCE(adj.close_price_adj, p.close_price) as close_price_adj,
                COALESCE(adj.average_traded_price_adj, p.average_traded_price) as average_traded_price_adj
            FROM stock_prices p LEFT JOIN stock_prices_adj adj ON p.id = adj.id
            WHERE p.business_date = %s
        ),
        WindowStats AS (
            SELECT symbol, MAX(high_price_adj) as calculated_52w_high, MIN(low_price_adj) as calculated_52w_low
            FROM stock_prices_adj
            WHERE business_date BETWEEN DATE_SUB(%s, INTERVAL 364 DAY) AND %s
            GROUP BY symbol
        )
        SELECT 
            pt.id, pt.business_date, pt.symbol, pt.security_name,
            pt.open_price_adj, pt.high_price_adj, pt.low_price_adj, pt.close_price_adj,
            pt.total_traded_quantity, pt.total_trades, pt.market_capitalization,
            pt.average_traded_price_adj,
            COALESCE(ws.calculated_52w_high, pt.fifty_two_week_high) as fifty_two_week_high_adj,
            COALESCE(ws.calculated_52w_low, pt.fifty_two_week_low) as fifty_two_week_low_adj
        FROM PricesToday pt LEFT JOIN WindowStats ws ON pt.symbol = ws.symbol
        ORDER BY pt.symbol
        """
        with connection.cursor() as cursor:
            cursor.execute(query, [query_date, query_date, query_date])
            stock_prices = dictfetchall(cursor)
        download_name = f'stock_prices_adjusted_{query_date}.csv'
    elif view == 'corporate' and search_term:
        matching_symbols = list(Companies.objects.filter(
            Q(script_ticker=search_term) | Q(company_name=search_term)
        ).values_list('script_ticker', flat=True))
        if matching_symbols:
            format_strings = ','.join(['%s'] * len(matching_symbols))
            query = f"""
            SELECT
                p.id, p.business_date, p.symbol,
                COALESCE(adj.open_price_adj, p.open_price) as open_price_adj,
                COALESCE(adj.high_price_adj, p.high_price) as high_price_adj,
                COALESCE(adj.low_price_adj, p.low_price) as low_price_adj,
                COALESCE(adj.close_price_adj, p.close_price) as close_price_adj,
                COALESCE(adj.average_traded_price_adj, p.average_traded_price) as average_traded_price_adj,
                MAX(COALESCE(adj.high_price_adj, p.high_price)) OVER (
                    PARTITION BY p.symbol ORDER BY p.business_date ROWS BETWEEN 364 PRECEDING AND CURRENT ROW
                ) as fifty_two_week_high_adj,
                MIN(COALESCE(adj.low_price_adj, p.low_price)) OVER (
                    PARTITION BY p.symbol ORDER BY p.business_date ROWS BETWEEN 364 PRECEDING AND CURRENT ROW
                ) as fifty_two_week_low_adj,
                p.total_traded_quantity, p.total_trades,
                p.market_capitalization, p.close_price
            FROM stock_prices p LEFT JOIN stock_prices_adj adj ON p.id = adj.id
            WHERE p.symbol IN ({format_strings})
            ORDER BY p.symbol, p.business_date DESC
            """
            with connection.cursor() as cursor:
                cursor.execute(query, matching_symbols)
                stock_prices = dictfetchall(cursor)
        download_name = f"stock_prices_adjusted_{search_term.replace(' ', '_')}.csv"
    if not stock_prices:
        return HttpResponse("No data found for this query.", status=404)
    response = HttpResponse(content_type='text/csv')
    response['Content-Disposition'] = f'attachment; filename="{download_name}"'
    if isinstance(stock_prices, list):
        headers = stock_prices[0].keys()
        writer = csv.DictWriter(response, fieldnames=headers)
        writer.writeheader()
        writer.writerows(stock_prices)
    else:
        headers = [field.name for field in stock_prices.model._meta.fields]
        writer = csv.writer(response)
        writer.writerow(headers)
        for row in stock_prices:
            writer.writerow([getattr(row, field) for field in headers])
    return response

# --- Main View for Data Entry Page (CORRECTED) ---
def data_entry_view(request):
    if request.method == 'POST':
        if request.POST.get('action') == 'upload_price':
            # --- HANDLE PRICE UPLOAD (NOW USES CLEAN HELPERS) ---
            price_file = request.FILES.get('price_file')
            if not price_file:
                messages.error(request, "No file selected for uploading.")
                return redirect('nepse_data:data_entry')
            if not price_file.name.endswith('.csv'):
                messages.error(request, "Invalid file type. Please upload a .csv file.")
                return redirect('nepse_data:data_entry')
            try:
                csv_file = io.TextIOWrapper(price_file.file, encoding='utf-8')
                reader = csv.reader(csv_file)
                header = next(reader)
                expected_columns = len(header)
                if expected_columns < 19:
                    messages.error(request, f"CSV format error. Expected 19+ columns, found {expected_columns}.")
                    return redirect('nepse_data:data_entry')
                all_rows_raw = list(reader)
                if not all_rows_raw:
                    messages.error(request, "CSV file is empty or contains only a header.")
                    return redirect('nepse_data:data_entry')
                business_date_str = all_rows_raw[0][1].strip()
                if StockPrices.objects.filter(business_date=business_date_str).exists():
                    messages.error(request, f"Error: Data for date {business_date_str} (from file) already exists.")
                    return redirect('nepse_data:data_entry')
                corrected_rows = []
                for row in all_rows_raw:
                    if len(row) > expected_columns:
                        extra_col_count = len(row) - expected_columns
                        merged_name = ' '.join(row[4 : 4 + extra_col_count + 1])
                        corrected_row = row[:4] + [merged_name] + row[4 + extra_col_count + 1:]
                        corrected_rows.append(corrected_row)
                    elif len(row) == expected_columns:
                        corrected_rows.append(row)
                inserted_rows, failed_rows = 0, 0
                for row in corrected_rows:
                    if len(row) < 19:
                        failed_rows += 1
                        continue
                    try:
                        StockPrices.objects.create(
                            business_date=business_date_str,
                            security_id=row[2].strip(),
                            symbol=row[3].strip(),
                            security_name=row[4].strip(),
                            open_price=clean_decimal(row[5]),
                            high_price=clean_decimal(row[6]),
                            low_price=clean_decimal(row[7]),
                            close_price=clean_decimal(row[8]),
                            total_traded_quantity=clean_int(row[9]),
                            total_traded_value=clean_decimal(row[10]),
                            previous_close=clean_decimal(row[11]),
                            fifty_two_week_high=clean_decimal(row[12]),
                            fifty_two_week_low=clean_decimal(row[13]),
                            last_updated_time=row[14].strip() or None,
                            last_updated_price=clean_decimal(row[15]),
                            total_trades=clean_int(row[16]),
                            average_traded_price=clean_decimal(row[17]),
                            market_capitalization=clean_decimal(row[18])
                        )
                        inserted_rows += 1
                    except Exception as e:
                        print(f"Error inserting row: {e}")
                        failed_rows += 1
                messages.success(request, f"Upload successful! Inserted {inserted_rows} price records for {business_date_str}. Skipped {failed_rows} rows.")
            except Exception as e:
                messages.error(request, f"An error occurred: {e}")
            return redirect('nepse_data:data_entry')

        elif request.POST.get('action') == 'upload_indices':
            # --- HANDLE INDICES UPLOAD (NOW USES CLEAN HELPERS & DECIMAL MODEL) ---
            indices_file = request.FILES.get('indices_file')
            if not indices_file:
                messages.error(request, "No indices file selected.")
                return redirect('nepse_data:data_entry')
            if not indices_file.name.endswith('.csv'):
                messages.error(request, "Invalid file type. Please upload a .csv file.")
                return redirect('nepse_data:data_entry')
            try:
                csv_file = io.TextIOWrapper(indices_file.file, encoding='utf-8')
                reader = csv.reader(csv_file)
                header = next(reader)
                inserted_rows, skipped_rows, failed_rows = 0, 0, 0
                for row in reader:
                    try:
                        row_date = datetime.datetime.strptime(row[1], '%Y-%m-%d').date()
                        row_sector = row[13].strip()
                        if Indices.objects.filter(date=row_date, sector=row_sector).exists():
                            skipped_rows += 1
                            continue
                        
                        Indices.objects.create(
                            sn=clean_int(row[0]),
                            date=row_date,
                            open=clean_decimal(row[2]),
                            high=clean_decimal(row[3]),
                            low=clean_decimal(row[4]),
                            close=clean_decimal(row[5]),
                            absolute_change=clean_decimal(row[6]),
                            percentage_change=row[7] or None,
                            number_52_weeks_high=clean_decimal(row[8]),
                            number_52_weeks_low=clean_decimal(row[9]),
                            turnover_values=clean_decimal(row[10]),
                            turnover_volume=clean_int(row[11]),
                            total_transaction=clean_int(row[12]),
                            sector=row_sector
                        )
                        inserted_rows += 1
                    except Exception as e:
                        print(f"Error inserting index row: {e}")
                        failed_rows += 1
                messages.success(request, f"Indices upload successful! Inserted {inserted_rows} new records. Skipped {skipped_rows} duplicate rows. Failed {failed_rows} rows.")
            except Exception as e:
                messages.error(request, f"An error occurred during indices upload: {e}")
            return redirect('nepse_data:data_entry')

        elif request.POST.get('action') == 'upload_marcap':
            # --- HANDLE MARKET CAP UPLOAD (THIS IS THE CORRECTED LOGIC) ---
            marcap_file = request.FILES.get('marcap_file')
            if not marcap_file:
                messages.error(request, "No market cap file selected.")
                return redirect('nepse_data:data_entry')
            if not marcap_file.name.endswith('.csv'):
                messages.error(request, "Invalid file type. Please upload a .csv file.")
                return redirect('nepse_data:data_entry')
            
            try:
                df = pd.read_csv(marcap_file)
                
                df.columns = df.columns.str.strip().str.lower().str.replace(' ', '_')
                
                if 'bussiness_date' in df.columns:
                    df.rename(columns={'bussiness_date': 'business_date'}, inplace=True)

                required_cols = ['business_date', 'market_capitalization', 'total_turnover']
                if not all(col in df.columns for col in required_cols):
                    missing = [col for col in required_cols if col not in df.columns]
                    messages.error(request, f"CSV is missing required columns. Must include: {', '.join(missing)}")
                    return redirect('nepse_data:data_entry')

                inserted_rows, updated_rows, failed_rows = 0, 0, 0
                
                for index, row in df.iterrows():
                    try:
                        row_date = pd.to_datetime(row['business_date']).date()

                        data_to_insert = {
                            'sn': clean_int(row.get('s.n')),
                            'market_capitalization': clean_decimal(row.get('market_capitalization')),
                            'sensitive_market_capitalization': clean_decimal(row.get('sensitive_market_capitalization')),
                            'float_market_capitalization': clean_decimal(row.get('float_market_capitalization')),
                            'sensitive_float_market_capitalization': clean_decimal(row.get('sensitive_float_market_capitalization')),
                            'total_turnover': clean_decimal(row.get('total_turnover')),
                            'total_traded_shares': clean_int(row.get('total_traded_shares')),
                            'total_transactions': clean_int(row.get('total_transactions')),
                            'total_scrips_traded': clean_int(row.get('total_scrips_traded')),
                        }
                        
                        obj, created = Marcap.objects.update_or_create(
                            business_date=row_date,
                            defaults=data_to_insert
                        )

                        if created:
                            inserted_rows += 1
                        else:
                            updated_rows += 1
                    
                    except Exception as e:
                        print(f"Error inserting marcap row {index}: {e}")
                        failed_rows += 1

                messages.success(request, f"Market Cap upload successful! Created {inserted_rows} new records. Updated {updated_rows} records. Failed {failed_rows} rows.")
            except Exception as e:
                messages.error(request, f"An error occurred during market cap upload: {e}")
            return redirect('nepse_data:data_entry')
        
        # --- NEW FLOORSHEET UPLOAD LOGIC ---
        elif request.POST.get('action') == 'upload_floorsheet':
            floorsheet_file = request.FILES.get('floorsheet_file')
            date_str = request.POST.get('floorsheet_date')

            if not floorsheet_file:
                messages.error(request, "No floorsheet file selected.")
                return redirect('nepse_data:data_entry')
            if not date_str:
                messages.error(request, "No calculation date selected.")
                return redirect('nepse_data:data_entry')

            try:
                calculation_date = datetime.date.fromisoformat(date_str)
            except ValueError:
                messages.error(request, "Invalid date format.")
                return redirect('nepse_data:data_entry')

            # --- Read file using Pandas (handles both Excel and CSV) ---
            try:
                if floorsheet_file.name.endswith('.csv'):
                    df = pd.read_csv(floorsheet_file)
                else:
                    df = pd.read_excel(floorsheet_file)
                df.columns = [col.upper().strip() for col in df.columns]
            except Exception as e:
                messages.error(request, f"Error reading file: {e}")
                return redirect('nepse_data:data_entry')

            # --- Validate required columns ---
            required_cols = ['SN', 'CONTRACT NO.', 'STOCK SYMBOL', 'BUYER', 'SELLER', 'QUANTITY', 'RATE (RS)', 'AMOUNT (RS)']
            if not all(col in df.columns for col in required_cols):
                missing = [col for col in required_cols if col not in df.columns]
                messages.error(request, f"File is missing required columns: {', '.join(missing)}")
                return redirect('nepse_data:data_entry')

            # --- Get the Sector Map (using Django ORM) ---
            sector_map = {
                k.upper(): v for k, v in
                Companies.objects.values_list('script_ticker', 'sector')
                if k
            }
            
            # --- Delete existing data for this date (from SQL_Importer logic) ---
            with connection.cursor() as cursor:
                print(f"Deleting existing records for {calculation_date}")
                cursor.execute("DELETE FROM floorsheet_raw WHERE calculation_date = %s", [calculation_date])
                cursor.execute("DELETE FROM buyer_summary WHERE calculation_date = %s", [calculation_date])
                cursor.execute("DELETE FROM seller_summary WHERE calculation_date = %s", [calculation_date])
                cursor.execute("DELETE FROM sector_buyer_summary WHERE calculation_date = %s", [calculation_date])
                cursor.execute("DELETE FROM sector_seller_summary WHERE calculation_date = %s", [calculation_date])

            # --- Prepare records for bulk creation ---
            records_to_insert = []
            failed_rows = 0
            for index, row in df.iterrows():
                try:
                    original_id = int(row['SN'])
                    stock_symbol = str(row['STOCK SYMBOL']).upper()
                    sector = sector_map.get(stock_symbol, None)

                    # Replicate the unique ID logic from SQL_Importer.py
                    new_id = int(f"{calculation_date.strftime('%Y%m%d')}{original_id:06d}")

                    records_to_insert.append(FloorsheetRaw(
                        id=new_id,
                        contract_no=str(row['CONTRACT NO.']),
                        stock_symbol=stock_symbol,
                        buyer=buyer_seller_to_int(row['BUYER']),
                        seller=buyer_seller_to_int(row['SELLER']),
                        quantity=clean_int(row['QUANTITY']),
                        rate=clean_decimal(row['RATE (RS)']),
                        amount=clean_decimal(row['AMOUNT (RS)']),
                        calculation_date=calculation_date,
                        sector=sector
                    ))
                except Exception as e:
                    print(f"Skipping row {index}: {e}")
                    failed_rows += 1

            # --- Insert all records in a single batch ---
            FloorsheetRaw.objects.bulk_create(records_to_insert, ignore_conflicts=True)
            inserted_count = len(records_to_insert)

            # --- Now, repopulate the summary tables (Raw SQL from SQL_Importer.py) ---
            with connection.cursor() as cursor:
                print(f"Populating summary tables for {calculation_date}...")
                
                # BUYER SUMMARY - PER COMPANY
                cursor.execute("""
                    INSERT INTO buyer_summary (calculation_date, stock_symbol, buyer, sector, total_quantity, total_amount, average_rate)
                    SELECT calculation_date, stock_symbol, buyer, sector, SUM(quantity), SUM(amount), SUM(amount) / SUM(quantity)
                    FROM floorsheet_raw WHERE calculation_date = %s GROUP BY calculation_date, stock_symbol, buyer, sector
                    ON DUPLICATE KEY UPDATE
                        sector = VALUES(sector), total_quantity = VALUES(total_quantity),
                        total_amount = VALUES(total_amount), average_rate = VALUES(average_rate);
                """, [calculation_date])

                # SELLER SUMMARY - PER COMPANY
                cursor.execute("""
                    INSERT INTO seller_summary (calculation_date, stock_symbol, seller, sector, total_quantity, total_amount, average_rate)
                    SELECT calculation_date, stock_symbol, seller, sector, SUM(quantity), SUM(amount), SUM(amount) / SUM(quantity)
                    FROM floorsheet_raw WHERE calculation_date = %s GROUP BY calculation_date, stock_symbol, seller, sector
                    ON DUPLICATE KEY UPDATE
                        sector = VALUES(sector), total_quantity = VALUES(total_quantity),
                        total_amount = VALUES(total_amount), average_rate = VALUES(average_rate);
                """, [calculation_date])

                # SECTOR BUYER SUMMARY - PER SECTOR
                cursor.execute("""
                    INSERT INTO sector_buyer_summary (calculation_date, sector, buyer, total_quantity, total_amount, average_rate)
                    SELECT calculation_date, sector, buyer, SUM(quantity), SUM(amount), SUM(amount) / SUM(quantity)
                    FROM floorsheet_raw WHERE calculation_date = %s AND sector IS NOT NULL GROUP BY calculation_date, sector, buyer
                    ON DUPLICATE KEY UPDATE
                        total_quantity = VALUES(total_quantity), total_amount = VALUES(total_amount), average_rate = VALUES(average_rate);
                """, [calculation_date])

                # SECTOR SELLER SUMMARY - PER SECTOR
                cursor.execute("""
                    INSERT INTO sector_seller_summary (calculation_date, sector, seller, total_quantity, total_amount, average_rate)
                    SELECT calculation_date, sector, seller, SUM(quantity), SUM(amount), SUM(amount) / SUM(quantity)
                    FROM floorsheet_raw WHERE calculation_date = %s AND sector IS NOT NULL GROUP BY calculation_date, sector, seller
                    ON DUPLICATE KEY UPDATE
                        total_quantity = VALUES(total_quantity), total_amount = VALUES(total_amount), average_rate = VALUES(average_rate);
                """, [calculation_date])

            messages.success(request, f"Floorsheet upload for {calculation_date} successful! Inserted {inserted_count} records. Skipped {failed_rows} rows. Summary tables updated.")
            return redirect('nepse_data:data_entry')
    
    # ... (inside data_entry_view, after the 'upload_floorsheet' block)

# --- NEW DIVIDEND HISTORY UPLOAD LOGIC ---
        elif request.POST.get('action') == 'upload_dividend':
            dividend_file = request.FILES.get('dividend_file')
            if not dividend_file:
                messages.error(request, "No dividend history file selected.")
                return redirect('nepse_data:data_entry')

            # --- Read file using Pandas (handles both Excel and CSV) ---
            try:
                if dividend_file.name.endswith('.csv'):
                    df = pd.read_csv(dividend_file)
                else:
                    df = pd.read_excel(dividend_file)
                
                # --- THIS IS THE CORRECTED STANDARDIZATION LOGIC ---
                df.columns = (
                    df.columns.str.strip()
                    .str.lower()
                    # 1. Handle "Bonus (%)" -> "bonus_percent"
                    .str.replace(' (%)', '_percent', regex=False)
                    # 2. Handle "Bonus(%)" (no space) -> "bonus_percent"
                    .str.replace('(%', '_percent', regex=False)
                    # 3. Handle "Bonus %" -> "bonus_percent"
                    .str.replace(' %', '_percent', regex=False)
                    # 4. Handle "Fiscal Year" -> "fiscal_year"
                    .str.replace(' ', '_', regex=False)
                    # 5. Clean up any remaining characters
                    .str.replace('.', '', regex=False)
                    .str.replace('/', '_', regex=False)
                    .str.replace(')', '', regex=False)
                )
                # --- END OF FIX ---

            except Exception as e:
                messages.error(request, f"Error reading file: {e}")
                return redirect('nepse_data:data_entry')

            # --- Get Company Map for names ---
            company_map = {
                k: v for k, v in
                Companies.objects.values_list('script_ticker', 'company_name')
                if k
            }
            
            inserted_rows, updated_rows, failed_rows = 0, 0, 0

            # --- Iterate through DataFrame rows ---
            for index, row in df.iterrows():
                try:
                    symbol = str(row['symbol']).strip().upper()
                    if not symbol:
                        failed_rows += 1
                        continue

                    fiscal_year = str(row.get('fiscal_year', '')).strip() or None
                    
                    # 1. Define what makes a record unique
                    unique_key = {
                        'symbol': symbol,
                        'fiscal_year': fiscal_year,
                    }

                    # 2. Get all date fields
                    announcement_date = clean_date(row.get('announcement_date'))
                    book_closure_date = clean_date(row.get('book_closure_date'))
                    distribution_date = clean_date(row.get('distribution_date'))
                    bonus_listing_date = clean_date(row.get('bonus_listing_date'))
                    
                    # 3. Define ALL data fields to be inserted or updated
                    data_to_insert = {
                        'company_name': company_map.get(symbol, row.get('company_name', None)),
                        
                        # This will now correctly find 'bonus_percent', etc.
                        'bonus_percent': clean_decimal(row.get('bonus_percent')),
                        'cash_percent': clean_decimal(row.get('cash_percent')),
                        'tax_percent': clean_decimal(row.get('tax_percent')),
                        'total_percent': clean_decimal(row.get('total_percent')),
                        
                        'announcement_date': announcement_date,
                        'book_closure_date': book_closure_date,
                        'book_closure_status': str(row.get('book_closure_status', '')).strip() or None,
                        'distribution_date': distribution_date,
                        'bonus_listing_date': bonus_listing_date,
                    }
                    
                    # 4. Call update_or_create
                    obj, created = DividendHistory.objects.update_or_create(
                        **unique_key,
                        defaults=data_to_insert
                    )

                    if created:
                        inserted_rows += 1
                    else:
                        updated_rows += 1

                except Exception as e:
                    print(f"Error processing row {index} ({symbol}): {e}")
                    failed_rows += 1

            messages.success(request, f"Dividend History upload successful! Created {inserted_rows} new records. Updated {updated_rows} records. Failed {failed_rows} rows.")
            return redirect('nepse_data:data_entry')

    else:
        # --- HANDLE GET REQUEST (Unchanged) ---
        available_dates = StockPrices.objects.values('business_date').distinct().order_by('-business_date')

        available_floorsheet_dates = FloorsheetRaw.objects.values('calculation_date').distinct().order_by('-calculation_date')

        context = {
            'title': 'Data Entry Portal',
            'available_dates': available_dates,
            'available_floorsheet_dates': available_floorsheet_dates
        }
        return render(request, 'nepse_data/data_entry.html', context)
    


@require_POST
def delete_floorsheet_data_view(request):
    dates_to_delete = request.POST.getlist('dates_to_delete')
    if not dates_to_delete:
        messages.warning(request, "No dates were selected for deletion.")
        return redirect('nepse_data:data_entry')
    
    try:
        with connection.cursor() as cursor:
            # Create a string of placeholders (%s, %s, %s)
            placeholders = ','.join(['%s'] * len(dates_to_delete))
            
            # Delete from summary tables first
            cursor.execute(f"DELETE FROM sector_seller_summary WHERE calculation_date IN ({placeholders})", dates_to_delete)
            cursor.execute(f"DELETE FROM sector_buyer_summary WHERE calculation_date IN ({placeholders})", dates_to_delete)
            cursor.execute(f"DELETE FROM seller_summary WHERE calculation_date IN ({placeholders})", dates_to_delete)
            cursor.execute(f"DELETE FROM buyer_summary WHERE calculation_date IN ({placeholders})", dates_to_delete)
            
            # Finally, delete from the raw floorsheet table
            cursor.execute(f"DELETE FROM floorsheet_raw WHERE calculation_date IN ({placeholders})", dates_to_delete)

        messages.success(request, f"Successfully deleted all floorsheet and summary data for {len(dates_to_delete)} selected date(s).")
    except Exception as e:
        messages.error(request, f"An error occurred while deleting: {e}")
    
    return redirect('nepse_data:data_entry')



@require_POST
def delete_price_data_view(request):
    # (This view is unchanged and correct)
    dates_to_delete = request.POST.getlist('dates_to_delete')
    if not dates_to_delete:
        messages.warning(request, "No dates were selected for deletion.")
        return redirect('nepse_data:data_entry')
    try:
        StockPricesAdj.objects.filter(business_date__in=dates_to_delete).delete()
        count, _ = StockPrices.objects.filter(business_date__in=dates_to_delete).delete()
        messages.success(request, f"Successfully deleted all price data for {len(dates_to_delete)} selected date(s).")
    except Exception as e:
        messages.error(request, f"An error occurred while deleting: {e}")
    return redirect('nepse_data:data_entry')

def indices_view(request):
    # (This view is unchanged and correct)
    indices_data = []
    view = request.GET.get('view', 'date')
    selected_date_str = request.GET.get('selected_date')
    search_term = request.GET.get('search_term', '').strip()
    page = request.GET.get('page', 1)
    per_page_str = request.GET.get('per_page', '20')
    query_date = None
    title = "Indices Data"
    all_indices_list = Indices.objects.values('sector').distinct().order_by('sector')
    if view == 'date':
        title = "Indices by Date"
        if selected_date_str:
            try:
                query_date = datetime.date.fromisoformat(selected_date_str)
            except ValueError:
                query_date = None
        else:
            latest_date_result = Indices.objects.aggregate(max_date=Max('date'))
            if latest_date_result['max_date']:
                query_date = latest_date_result['max_date']
        if query_date:
            indices_data = Indices.objects.filter(date=query_date).order_by('sector')
            selected_date_str = query_date.isoformat()
        context = {
            'indices_data': indices_data, 'title': title, 'selected_date_str': selected_date_str,
            'view': view, 'all_indices_list': all_indices_list, 'total_pages': 1, 'page': 1,
            'per_page': per_page_str,
        }
        return render(request, 'nepse_data/indices.html', context)
    elif view == 'indices':
        base_query = Indices.objects.all() 
        if search_term:
            title = f"History for {search_term}"
            base_query = base_query.filter(sector=search_term)
        else:
            title = "Full Index History"
        base_query = base_query.order_by('-date', 'sector')
        if per_page_str == 'All':
            paginator = None
            page_obj = base_query
        else:
            paginator = Paginator(base_query, int(per_page_str), allow_empty_first_page=True)
            page_obj = paginator.get_page(page)
        context = {
            'indices_data': page_obj, 'title': title, 'view': view, 'search_term': search_term,
            'all_indices_list': all_indices_list, 'page_obj': page_obj if paginator else None,
            'paginator': paginator, 'per_page': per_page_str, 'page': page,
        }
        return render(request, 'nepse_data/indices.html', context)
    return render(request, 'nepse_data/indices.html', {'title': 'Indices', 'view': view})

def download_indices_view(request):
    # (This view is unchanged and correct)
    view = request.GET.get('view', 'date')
    search_term = request.GET.get('search_term', '').strip()
    selected_date_str = request.GET.get('selected_date')
    query = Indices.objects.all()
    download_name = "indices_data.csv"
    if view == 'date':
        query_date = None
        if selected_date_str:
            try:
                query_date = datetime.date.fromisoformat(selected_date_str)
            except ValueError:
                query_date = None
        else:
            latest_date_result = Indices.objects.aggregate(max_date=Max('date'))
            if latest_date_result['max_date']:
                query_date = latest_date_result['max_date']
        if query_date:
            query = query.filter(date=query_date).order_by('sector')
            download_name = f'indices_by_date_{query_date}.csv'
    elif view == 'indices':
        if search_term:
            query = query.filter(sector=search_term)
            download_name = f'indices_history_{search_term.replace(" ", "_")}.csv'
        else:
            download_name = 'indices_history_all.csv'
        query = query.order_by('-date', 'sector')
    indices_data = query.values_list(
        'id', 'sn', 'date', 'sector', 'open', 'high', 'low', 'close',
        'absolute_change', 'percentage_change', 'number_52_weeks_high', 
        'number_52_weeks_low', 'turnover_values', 'turnover_volume', 'total_transaction'
    )
    if not indices_data.exists():
        return HttpResponse("No index data to download for that query.", status=404)
    response = HttpResponse(content_type='text/csv')
    response['Content-Disposition'] = f'attachment; filename="{download_name}"'
    writer = csv.writer(response)
    writer.writerow([
        'id', 'sn', 'date', 'sector', 'open', 'high', 'low', 'close',
        'absolute_change', 'percentage_change', '52_weeks_high', 
        '52_weeks_low', 'turnover_values', 'turnover_volume', 'total_transaction'
    ])
    for row in indices_data:
        writer.writerow(row)
    return response

def market_cap_view(request):
    # (This view is unchanged and correct)
    title = "Market Capitalization History"
    page = request.GET.get('page', 1)
    per_page_str = request.GET.get('per_page', '20')
    base_query = Marcap.objects.all().order_by('-business_date')
    if per_page_str == 'All':
        paginator = None
        page_obj = base_query
    else:
        paginator = Paginator(base_query, int(per_page_str), allow_empty_first_page=True)
        page_obj = paginator.get_page(page)
    context = {
        'marcap_data': page_obj,
        'title': title,
        'page_obj': page_obj if paginator else None,
        'paginator': paginator,
        'per_page': per_page_str,
        'page': page,
    }
    return render(request, 'nepse_data/market_cap.html', context)

def download_marcap_view(request):
    # (This view is unchanged and correct)
    marcap_data = Marcap.objects.all().order_by('-business_date').values_list(
        'id', 'sn', 'business_date', 'market_capitalization',
        'sensitive_market_capitalization', 'float_market_capitalization',
        'sensitive_float_market_capitalization',
        'total_turnover', 'total_traded_shares', 'total_transactions', 'total_scrips_traded',
        'created_at'
    )
    if not marcap_data.exists():
        return HttpResponse("No market cap data to download.", status=404)
    response = HttpResponse(content_type='text/csv')
    response['Content-Disposition'] = 'attachment; filename="market_cap_history.csv"'
    writer = csv.writer(response)
    writer.writerow([
        'id', 'sn', 'business_date', 'market_capitalization',
        'sensitive_market_capitalization', 'float_market_capitalization',
        'sensitive_float_market_capitalization',
        'total_turnover', 'total_traded_shares', 'total_transactions', 'total_scrips_traded',
        'created_at'
    ])
    for row in marcap_data:
        writer.writerow(row)
    return response

def floorsheet_view(request):
    title = "Floorsheet History"

    # --- Get filter parameters ---
    selected_date_str = request.GET.get('selected_date')
    contract_no = request.GET.get('contract_no', '').strip()
    stock_symbol = request.GET.get('stock_symbol', '').strip()
    buyer = request.GET.get('buyer', '').strip()
    seller = request.GET.get('seller', '').strip()

    # --- Get pagination parameters ---
    page = request.GET.get('page', 1)
    per_page_str = request.GET.get('per_page', '20')

    # --- NEW: Get sorting parameters ---
    sort_by = request.GET.get('sort', 'contract_no')
    direction = request.GET.get('dir', 'desc')

    # --- NEW: Whitelist allowed sort fields to prevent errors/injection ---
    allowed_sort_fields = ['contract_no', 'quantity', 'amount']
    if sort_by not in allowed_sort_fields:
        sort_by = 'contract_no'
    if direction not in ['asc', 'desc']:
        direction = 'desc'

    # --- Base query ---
    base_query = FloorsheetRaw.objects.all()

    # --- 1. Date Filter ---
    query_date = None
    if selected_date_str:
        try:
            query_date = datetime.date.fromisoformat(selected_date_str)
        except ValueError:
            query_date = None
    else:
        latest_date_result = FloorsheetRaw.objects.aggregate(max_date=Max('calculation_date'))
        if latest_date_result['max_date']:
            query_date = latest_date_result['max_date']

    if query_date:
        base_query = base_query.filter(calculation_date=query_date)
        selected_date_str = query_date.isoformat()

    # --- 2. Other Filters ---
    if contract_no:
        base_query = base_query.filter(contract_no=contract_no)
    if stock_symbol:
        base_query = base_query.filter(stock_symbol__icontains=stock_symbol)
    if buyer:
        base_query = base_query.filter(buyer=buyer)
    if seller:
        base_query = base_query.filter(seller=seller)

    # --- Calculate totals (before pagination) ---
    totals = base_query.aggregate(
        total_quantity=Sum('quantity'),
        total_amount=Sum('amount')
    )

    # --- NEW: Apply dynamic server-side sorting ---
    sort_param = sort_by
    if direction == 'desc':
        sort_param = f"-{sort_by}"
    
    # We add '-id' as a secondary sort to ensure a stable,
    # predictable order if two values are identical.
    base_query = base_query.order_by(sort_param, '-id')

    # --- 3. Pagination ---
    paginator = Paginator(base_query, int(per_page_str), allow_empty_first_page=True)
    page_obj = paginator.get_page(page)

    context = {
        'floorsheet_data': page_obj,
        'title': title,

        # Pagination context
        'page_obj': page_obj,
        'paginator': paginator,
        'per_page': per_page_str,
        'page': page,

        # Filter values
        'selected_date': selected_date_str,
        'contract_no': contract_no,
        'stock_symbol': stock_symbol,
        'buyer': buyer,
        'seller': seller,

        # Totals
        'total_quantity': totals['total_quantity'],
        'total_amount': totals['total_amount'],
        
        # --- NEW: Pass sorting state to template ---
        'current_sort': sort_by,
        'current_dir': direction,
    }
    return render(request, 'nepse_data/floorsheet.html', context)


def download_floorsheet_view(request):
    # --- Get all the same filter parameters ---
    selected_date_str = request.GET.get('selected_date')
    contract_no = request.GET.get('contract_no', '').strip()
    stock_symbol = request.GET.get('stock_symbol', '').strip()
    buyer = request.GET.get('buyer', '').strip()
    seller = request.GET.get('seller', '').strip()

    base_query = FloorsheetRaw.objects.all()

    # --- Apply Date Filter ---
    query_date = None
    if selected_date_str:
        try:
            query_date = datetime.date.fromisoformat(selected_date_str)
        except ValueError:
            query_date = None
    else:
        latest_date_result = FloorsheetRaw.objects.aggregate(max_date=Max('calculation_date'))
        if latest_date_result['max_date']:
            query_date = latest_date_result['max_date']

    if query_date:
        base_query = base_query.filter(calculation_date=query_date)

    # --- Apply Other Filters ---
    if contract_no:
        base_query = base_query.filter(contract_no=contract_no)
    if stock_symbol:
        base_query = base_query.filter(stock_symbol__icontains=stock_symbol)
    if buyer:
        base_query = base_query.filter(buyer=buyer)
    if seller:
        base_query = base_query.filter(seller=seller)

    floorsheet_data = base_query.order_by('-id').values_list(
        'calculation_date', 'contract_no', 'stock_symbol', 'buyer', 
        'seller', 'quantity', 'rate', 'amount', 'sector'
    )

    if not floorsheet_data.exists():
        return HttpResponse("No floorsheet data to download for that query.", status=404)

    response = HttpResponse(content_type='text/csv')
    download_name = f"floorsheet_{query_date or 'all_dates'}.csv"
    response['Content-Disposition'] = f'attachment; filename="{download_name}"'

    writer = csv.writer(response)
    writer.writerow([
        'Date', 'Contract No', 'Symbol', 'Buyer', 
        'Seller', 'Quantity', 'Rate', 'Amount', 'Sector'
    ])

    for row in floorsheet_data:
        writer.writerow(row)

    return response


def dividend_history_view(request):
    title = "Dividend History"

    # --- Get filter parameters ---
    symbol = request.GET.get('symbol', '').strip().upper()
    fiscal_year = request.GET.get('fiscal_year', '').strip()
    
    # --- Get pagination parameters ---
    page = request.GET.get('page', 1)
    per_page_str = request.GET.get('per_page', '20')

    # --- Base query ---
    base_query = DividendHistory.objects.all()

    # --- 1. Apply Filters ---
    if symbol:
        base_query = base_query.filter(symbol=symbol)
    if fiscal_year:
        base_query = base_query.filter(fiscal_year=fiscal_year)

    # --- 2. Apply Sorting ---
    # The default ordering is set in the model's Meta class
    base_query = base_query.order_by('-announcement_date', 'symbol')

    # --- 3. Pagination ---
    if per_page_str == 'All':
        paginator = None
        page_obj = base_query
    else:
        try:
            paginator = Paginator(base_query, int(per_page_str), allow_empty_first_page=True)
            page_obj = paginator.get_page(page)
        except ValueError:
            paginator = Paginator(base_query, 20, allow_empty_first_page=True)
            page_obj = paginator.get_page(1)
            per_page_str = '20'

    # --- Get distinct values for filter dropdowns ---
    filter_options = {
        'symbols': list(DividendHistory.objects.values_list('symbol', flat=True)
                        .distinct().order_by('symbol')),
        'fiscal_years': list(DividendHistory.objects.values_list('fiscal_year', flat=True)
                             .distinct().order_by('-fiscal_year'))
    }

    context = {
        'title': title,
        'dividend_data': page_obj,

        # Pagination context
        'page_obj': page_obj if paginator else None,
        'paginator': paginator,
        'per_page': per_page_str,
        'page': page,

        # Filter values
        'symbol': symbol,
        'fiscal_year': fiscal_year,
        'filter_options': filter_options,
    }
    return render(request, 'nepse_data/dividend_history.html', context)


def download_dividend_history_view(request):
    # --- Get all the same filter parameters ---
    symbol = request.GET.get('symbol', '').strip().upper()
    fiscal_year = request.GET.get('fiscal_year', '').strip()

    base_query = DividendHistory.objects.all()

    # --- Apply Filters ---
    if symbol:
        base_query = base_query.filter(symbol=symbol)
    if fiscal_year:
        base_query = base_query.filter(fiscal_year=fiscal_year)

    # --- Get data ---
    dividend_data = base_query.order_by('-announcement_date', 'symbol').values_list(
        'fiscal_year', 'symbol', 'company_name',
        'bonus_percent', 'cash_percent', 'tax_percent', 'total_percent',
        'announcement_date', 'book_closure_date', 'book_closure_status',
        'distribution_date', 'bonus_listing_date'
    )

    if not dividend_data.exists():
        return HttpResponse("No dividend history to download for that query.", status=404)

    response = HttpResponse(content_type='text/csv')
    download_name = f"dividend_history.csv"
    response['Content-Disposition'] = f'attachment; filename="{download_name}"'

    writer = csv.writer(response)
    # Write header
    writer.writerow([
        'Fiscal Year', 'Symbol', 'Company Name',
        'Bonus %', 'Cash %', 'Tax %', 'Total %',
        'Announcement Date', 'Book Closure Date', 'BC Status',
        'Distribution Date', 'Bonus Listing Date'
    ])

    # Write data rows
    for row in dividend_data:
        writer.writerow(row)

    return response


def download_dividend_sample_view(request):
    """
    Generates and serves a sample CSV file for dividend history uploads.
    """
    response = HttpResponse(content_type='text/csv')
    response['Content-Disposition'] = 'attachment; filename="sample_dividend_history.csv"'

    writer = csv.writer(response)
    
    # Write Header
    writer.writerow([
        'Fiscal Year', 'Symbol', 'Bonus (%)', 'Cash (%)', 'Tax (%)', 
        'Total (%)', 'Announcement Date', 'Book Closure Date', 
        'Book Closure Status', 'Distribution Date', 'Bonus Listing Date'
    ])
    
    # Write Sample Rows
    writer.writerow([
        '2079/80', 'NIFRA', '5', '0', '0', '5', '2024-09-15', '2024-10-01', 'Posted', '2024-11-10', '2024-12-05'
    ])
    writer.writerow([
        '2079/80', 'NABIL', '11', '22.5', '1.18', '33.5', '2024-08-20', '2024-09-05', 'Posted', '2024-10-15', '2024-11-20'
    ])
    writer.writerow([
        '2078/79', 'HDL', '20', '5', '1.32', '25', '2023-11-01', '2023-11-20', 'Posted', '', ''
    ])
    writer.writerow([
        '2078/79', 'CIT', '10', '1.5', '0', '11.5', '2023-10-05', '', 'Not Posted', '', ''
    ])

    return response


@require_POST
def edit_dividend_view(request, pk):
    """
    Handles editing a single dividend history entry via a modal form.
    """
    try:
        dividend = DividendHistory.objects.get(pk=pk)
    except DividendHistory.DoesNotExist:
        messages.error(request, "Dividend entry not found.")
        return redirect('nepse_data:dividend_history')

    # Get data from the POST request
    dividend.fiscal_year = request.POST.get('fiscal_year')
    dividend.symbol = request.POST.get('symbol', '').upper()
    dividend.company_name = request.POST.get('company_name')
    
    dividend.bonus_percent = clean_decimal(request.POST.get('bonus_percent'))
    dividend.cash_percent = clean_decimal(request.POST.get('cash_percent'))
    dividend.tax_percent = clean_decimal(request.POST.get('tax_percent'))
    dividend.total_percent = clean_decimal(request.POST.get('total_percent'))
    
    # Use clean_date for all date fields
    announcement_date = clean_date(request.POST.get('announcement_date'))
    book_closure_date = clean_date(request.POST.get('book_closure_date'))
    distribution_date = clean_date(request.POST.get('distribution_date'))
    bonus_listing_date = clean_date(request.POST.get('bonus_listing_date'))

    dividend.announcement_date = announcement_date if announcement_date else None
    dividend.book_closure_date = book_closure_date if book_closure_date else None
    dividend.distribution_date = distribution_date if distribution_date else None
    dividend.bonus_listing_date = bonus_listing_date if bonus_listing_date else None
    
    dividend.book_closure_status = request.POST.get('book_closure_status')

    try:
        dividend.save()
        messages.success(request, f"Successfully updated dividend for {dividend.symbol} ({dividend.fiscal_year}).")
    except Exception as e:
        messages.error(request, f"Error updating dividend: {e}")

    # Redirect back to the same page (including filters)
    return redirect(request.META.get('HTTP_REFERER', 'nepse_data:dividend_history'))


@require_POST
def delete_dividend_view(request, pk):
    """
    Handles deleting a single dividend history entry.
    """
    try:
        dividend = DividendHistory.objects.get(pk=pk)
        symbol = dividend.symbol
        fy = dividend.fiscal_year
        dividend.delete()
        messages.success(request, f"Successfully deleted dividend for {symbol} ({fy}).")
    except DividendHistory.DoesNotExist:
        messages.error(request, "Dividend entry not found.")
    except Exception as e:
        messages.error(request, f"Error deleting dividend: {e}")
        
    return redirect(request.META.get('HTTP_REFERER', 'nepse_data:dividend_history'))


@require_POST
def delete_all_dividends_view(request):
    """
    Handles deleting ALL dividend history entries.
    """
    try:
        count, _ = DividendHistory.objects.all().delete()
        messages.success(request, f"Successfully deleted all {count} dividend history records.")
    except Exception as e:
        messages.error(request, f"An error occurred while deleting all records: {e}")
        
    return redirect('nepse_data:data_entry')


@require_POST
def add_dividend_view(request):
    """
    Handles adding OR UPDATING a new single dividend entry via a modal form.
    This now uses update_or_create to match the bulk upload logic.
    """
    try:
        # Get the unique keys
        symbol = request.POST.get('symbol', '').upper()
        fiscal_year = request.POST.get('fiscal_year')

        if not symbol or not fiscal_year:
            messages.error(request, "Symbol and Fiscal Year are required.")
            return redirect('nepse_data:data_entry')

        # Get all the data for the 'defaults' dictionary
        data_to_insert = {
            'company_name': request.POST.get('company_name'),
            'bonus_percent': clean_decimal(request.POST.get('bonus_percent')),
            'cash_percent': clean_decimal(request.POST.get('cash_percent')),
            'tax_percent': clean_decimal(request.POST.get('tax_percent')),
            'total_percent': clean_decimal(request.POST.get('total_percent')),
            
            'announcement_date': clean_date(request.POST.get('announcement_date')),
            'book_closure_date': clean_date(request.POST.get('book_closure_date')),
            'distribution_date': clean_date(request.POST.get('distribution_date')),
            'bonus_listing_date': clean_date(request.POST.get('bonus_listing_date')),
            'book_closure_status': request.POST.get('book_closure_status'),
        }

        # Use update_or_create to find a match on (symbol, fiscal_year)
        # or create a new one.
        obj, created = DividendHistory.objects.update_or_create(
            symbol=symbol,
            fiscal_year=fiscal_year,
            defaults=data_to_insert
        )

        if created:
            messages.success(request, f"Successfully added new dividend for {symbol} ({fiscal_year}).")
        else:
            messages.success(request, f"Successfully updated dividend for {symbol} ({fiscal_year}).")
    
    except Exception as e:
        messages.error(request, f"Error adding/updating dividend: {e}")

    # Redirect back to the data entry page
    return redirect('nepse_data:data_entry')

def search_dividends_json_view(request):
    """
    Returns a JSON list of dividend entries matching a search term.
    """
    term = request.GET.get('term', '').strip()
    if not term:
        return JsonResponse({'error': 'No search term provided'}, status=400)

    # Search by symbol or fiscal year
    # We use Concat to make searching "NIFRA 2079/80" possible
    search_results = DividendHistory.objects.annotate(
        search_field=Concat('symbol', Value(' '), 'fiscal_year')
    ).filter(
        Q(search_field__icontains=term) | Q(symbol__icontains=term)
    ).order_by('-fiscal_year', 'symbol')[:50] # Limit to 50 results

    # Serialize all data needed by the edit modal
    data = [
        {
            'id': item.id,
            'symbol': item.symbol,
            'fiscal_year': item.fiscal_year,
            'company_name': item.company_name,
            'bonus_percent': item.bonus_percent,
            'cash_percent': item.cash_percent,
            'tax_percent': item.tax_percent,
            'total_percent': item.total_percent,
            'announcement_date': item.announcement_date.strftime('%Y-%m-%d') if item.announcement_date else '',
            'book_closure_date': item.book_closure_date.strftime('%Y-%m-%d') if item.book_closure_date else '',
            'book_closure_status': item.book_closure_status,
            'distribution_date': item.distribution_date.strftime('%Y-%m-%d') if item.distribution_date else '',
            'bonus_listing_date': item.bonus_listing_date.strftime('%Y-%m-%d') if item.bonus_listing_date else '',
        } 
        for item in search_results
    ]
    
    return JsonResponse({'results': data})

def company_lookup_json_view(request):
    """
    Looks up a company name by its symbol (script_ticker).
    """
    symbol = request.GET.get('symbol', '').strip().upper()
    if not symbol:
        return JsonResponse({'error': 'No symbol provided'}, status=400)

    try:
        # We query the Companies model from your other app
        company = Companies.objects.get(script_ticker=symbol)
        return JsonResponse({'company_name': company.company_name})
    except Companies.DoesNotExist:
        # If not found, return an empty string
        return JsonResponse({'company_name': ''})
    except Exception as e:
        return JsonResponse({'error': str(e)}, status=500)
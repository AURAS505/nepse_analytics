# nepse_data/views.py
from django.shortcuts import render, redirect
from django.db import connection
from django.db.models import Q, Max
from listed_companies.models import Companies
from .models import StockPrices, Indices # Make sure Indices is imported
import datetime
from django.http import HttpResponse
from django.contrib import messages
from django.views.decorators.http import require_POST
from adjustments_stock_price.models import StockPricesAdj # Import from other app
import io
import csv
from decimal import Decimal, InvalidOperation
from django.core.paginator import Paginator
from .models import StockPrices, Indices, Marcap
import pandas as pd

# A helper function to fetch raw SQL as a dictionary
def dictfetchall(cursor):
    "Return all rows from a cursor as a dict"
    columns = [col[0] for col in cursor.description]
    return [
        dict(zip(columns, row))
        for row in cursor.fetchall()
    ]

def todays_price_view(request):
    # (This view is unchanged)
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
    # (This view is unchanged)
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

# --- Helper functions ---
def convert_float(value):
    """ Safely convert a string (with commas) to a Decimal """
    if not isinstance(value, str):
        value = str(value)
    if not value or value.strip() in ('', 'N/A', '-'):
        return None
    try:
        return Decimal(value.replace(',', ''))
    except InvalidOperation:
        return None

def convert_int(value):
    """ Safely convert a string (with commas) to an Integer """
    if not isinstance(value, str):
        value = str(value)
    if not value or value.strip() in ('', 'N/A', '-'):
        return None
    try:
        # Use float first to handle decimals (e.g., 40591.00)
        return int(float(value.replace(',', '')))
    except (ValueError, InvalidOperation, TypeError):
        return None

def to_float(value):
    """ Safely convert a string (with commas) to a Float """
    if not isinstance(value, str):
        value = str(value)
    if not value or value.strip() in ('', 'N/A', '-'):
        return None
    try:
        return float(value.replace(',', ''))
    except (ValueError, TypeError):
        return None

def to_int(value):
    """ Safely convert a string (with commas) to an Integer """
    if not isinstance(value, str):
        value = str(value)
    if not value or value.strip() in ('', 'N/A', '-'):
        return None
    try:
        return int(float(value.replace(',', '')))
    except (ValueError, TypeError):
        return None

# --- Main View for Data Entry Page ---
def data_entry_view(request):
    if request.method == 'POST':
        if request.POST.get('action') == 'upload_price':
            # --- HANDLE PRICE UPLOAD ---
            price_file = request.FILES.get('price_file')
            
            if not price_file:
                messages.error(request, "No file selected for uploading.")
                return redirect('nepse_data:data_entry')
            
            if not price_file.name.endswith('.csv'):
                messages.error(request, "Invalid file type. Please upload a .csv file.")
                return redirect('nepse_data:data_entry')

            try:
                # (Your existing price upload logic is unchanged)
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
                            open_price=convert_float(row[5]),
                            high_price=convert_float(row[6]),
                            low_price=convert_float(row[7]),
                            close_price=convert_float(row[8]),
                            total_traded_quantity=convert_int(row[9]),
                            total_traded_value=convert_float(row[10]),
                            previous_close=convert_float(row[11]),
                            fifty_two_week_high=convert_float(row[12]),
                            fifty_two_week_low=convert_float(row[13]),
                            last_updated_time=row[14].strip() or None,
                            last_updated_price=convert_float(row[15]),
                            total_trades=convert_int(row[16]),
                            average_traded_price=convert_float(row[17]),
                            market_capitalization=convert_float(row[18])
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
            # --- HANDLE INDICES UPLOAD (Unchanged) ---
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
                            sn=to_int(row[0]),
                            date=row_date,
                            open=to_float(row[2]),
                            high=to_float(row[3]),
                            low=to_float(row[4]),
                            close=to_float(row[5]),
                            absolute_change=to_float(row[6]),
                            percentage_change=row[7] or None,
                            number_52_weeks_high=to_float(row[8]),
                            number_52_weeks_low=to_float(row[9]),
                            turnover_values=to_float(row[10]),
                            turnover_volume=to_int(row[11]),
                            total_transaction=to_int(row[12]),
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

        # --- THIS IS THE NEW BLOCK ---
        elif request.POST.get('action') == 'upload_marcap':
            marcap_file = request.FILES.get('marcap_file')

            if not marcap_file:
                messages.error(request, "No market cap file selected.")
                return redirect('nepse_data:data_entry')
            
            if not marcap_file.name.endswith('.csv'):
                messages.error(request, "Invalid file type. Please upload a .csv file.")
                return redirect('nepse_data:data_entry')
            
            try:
                # Use pandas to read the CSV, it handles the comma-separators in numbers well
                df = pd.read_csv(marcap_file)
                
                # Clean column names (remove spaces, fix spelling, make lowercase)
                df.columns = df.columns.str.strip().str.lower().str.replace(' ', '_')
                
                # Fix 'bussiness_date' typo from CSV
                if 'bussiness_date' in df.columns:
                    df.rename(columns={'bussiness_date': 'business_date'}, inplace=True)

                # Check for required columns
                required_cols = ['business_date', 'market_capitalization', 'total_turnover']
                if not all(col in df.columns for col in df.columns):
                    missing = [col for col in required_cols if col not in df.columns]
                    messages.error(request, f"CSV is missing required columns. Must include: {', '.join(missing)}")
                    return redirect('nepse_data:data_entry')

                inserted_rows = 0
                updated_rows = 0
                failed_rows = 0
                
                for index, row in df.iterrows():
                    try:
                        # Parse date
                        row_date = pd.to_datetime(row['business_date']).date()

                        # Create data dict for update_or_create
                        data_to_insert = {
                            'sn': to_int(row.get('s.n')),
                            'market_capitalization': convert_float(row.get('market_capitalization')),
                            'sensitive_market_capitalization': convert_float(row.get('sensitive_market_capitalization')),
                            'float_market_capitalization': convert_float(row.get('float_market_capitalization')),
                            'sensitive_float_market_capitalization': convert_float(row.get('sensitive_float_market_capitalization')),
                            'total_turnover': convert_float(row.get('total_turnover')),
                            'total_traded_shares': to_int(row.get('total_traded_shares')),
                            'total_transactions': to_int(row.get('total_transactions')),
                            'total_scrips_traded': to_int(row.get('total_scrips_traded')),
                        }
                        
                        # Use update_or_create to prevent duplicates
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
        # --- END OF NEW BLOCK ---

    else:
        # --- HANDLE GET REQUEST (Unchanged) ---
        available_dates = StockPrices.objects.values('business_date').distinct().order_by('-business_date')
        context = {
            'title': 'Data Entry Portal',
            'available_dates': available_dates
        }
        return render(request, 'nepse_data/data_entry.html', context)

@require_POST
def delete_price_data_view(request):
    # (This view is unchanged)
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

# --- THIS IS THE CORRECTED VIEW ---
def indices_view(request):
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
            # FIX: 'date' is now a DateField, so .date() is not needed
            latest_date_result = Indices.objects.aggregate(max_date=Max('date'))
            if latest_date_result['max_date']:
                query_date = latest_date_result['max_date']

        if query_date:
            # FIX: We don't need '__date' anymore, just filter on 'date'
            indices_data = Indices.objects.filter(date=query_date).order_by('sector')
            selected_date_str = query_date.isoformat()

        context = {
            'indices_data': indices_data,
            'title': title,
            'selected_date_str': selected_date_str,
            'view': view,
            'all_indices_list': all_indices_list,
            'total_pages': 1,
            'page': 1,
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
            'indices_data': page_obj,
            'title': title,
            'view': view,
            'search_term': search_term,
            'all_indices_list': all_indices_list,
            'page_obj': page_obj if paginator else None,
            'paginator': paginator,
            'per_page': per_page_str,
            'page': page,
        }
        return render(request, 'nepse_data/indices.html', context)

    return render(request, 'nepse_data/indices.html', {'title': 'Indices', 'view': view})
# --- END OF CORRECTED VIEW ---


def download_indices_view(request):
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
            # FIX: 'date' is now a DateField, so .date() is not needed
            latest_date_result = Indices.objects.aggregate(max_date=Max('date'))
            if latest_date_result['max_date']:
                query_date = latest_date_result['max_date']

        if query_date:
            # FIX: We don't need '__date' anymore
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

# Add these two new functions
def to_float(value):
    if not value or value.strip() in ('', 'N/A', '-'):
        return None
    try:
        return float(value.replace(',', ''))
    except (ValueError, TypeError):
        return None

def to_int(value):
    if not value or value.strip() in ('', 'N/A', '-'):
        return None
    try:
        return int(float(value.replace(',', '')))
    except (ValueError, TypeError):
        return None
    

def market_cap_view(request):
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
    # Updated to include new fields
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
    # Write updated headers
    writer.writerow([
        'id', 'sn', 'business_date', 'market_capitalization',
        'sensitive_market_capitalization', 'float_market_capitalization',
        'sensitive_float_market_capitalization',
        'total_turnover', 'total_traded_shares', 'total_transactions', 'total_scrips_traded',
        'created_at'
    ])

    # Write data
    for row in marcap_data:
        writer.writerow(row)

    return response


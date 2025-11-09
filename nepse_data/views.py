# nepse_data/views.py
from django.shortcuts import render
from django.db import connection
from django.db.models import Q, Max
from listed_companies.models import Companies
from .models import StockPrices
import datetime
from django.http import HttpResponse
from django.shortcuts import redirect
from django.contrib import messages
from django.views.decorators.http import require_POST
from adjustments_stock_price.models import StockPricesAdj # Import from other app
import io
import csv
from decimal import Decimal, InvalidOperation

# A helper function to fetch raw SQL as a dictionary
def dictfetchall(cursor):
    "Return all rows from a cursor as a dict"
    columns = [col[0] for col in cursor.description]
    return [
        dict(zip(columns, row))
        for row in cursor.fetchall()
    ]

def todays_price_view(request):
    # Get parameters from URL (e.g., /data/todays-price/?view=date)
    view = request.GET.get('view', 'date')
    selected_date_str = request.GET.get('selected_date')
    search_term = request.GET.get('search_term', '').strip()

    stock_prices = []
    query_date = None
    title = "Stock Prices"

    # Fetch all companies for the search datalist
    all_companies = Companies.objects.values('script_ticker', 'company_name').order_by('script_ticker')

    # --- Get the latest date as a default ---
    if not selected_date_str:
        latest_date_result = StockPrices.objects.aggregate(max_date=Max('business_date'))
        if latest_date_result['max_date']:
            query_date = latest_date_result['max_date']
            selected_date_str = query_date.isoformat()
    else:
        try:
            query_date = datetime.date.fromisoformat(selected_date_str)
        except ValueError:
            query_date = None # Will show "no data" if date is invalid

    # --- Logic for each view type ---

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
        # This complex query is best handled with raw SQL,
        # just like in your optimized Flask app.

        # This is the exact same query from your Flask app
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

        # 1. Find all matching symbols
        matching_symbols = list(Companies.objects.filter(
            Q(script_ticker=search_term) | Q(company_name=search_term)
        ).values_list('script_ticker', flat=True))

        if matching_symbols:
            # 2. Build a format string for the IN clause (e.g., "%s, %s, %s")
            format_strings = ','.join(['%s'] * len(matching_symbols))

            # This is the exact same query from your Flask app
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

    # --- Handle "No. of Shares" calculation ---
    # Django templates can't do math, so we do it here.
    if view == 'adjusted' or view == 'corporate':
        for price in stock_prices:
            no_of_shares = None
            try:
                # We use .get() for dicts, . for objects
                market_cap = price.get('market_capitalization')
                close_price = price.get('close_price')

                if not market_cap: # Handle object fallback
                    market_cap = price.market_capitalization
                    close_price = price.close_price

                if market_cap is not None and close_price is not None and close_price > 0:
                    no_of_shares = round((market_cap * 1000000) / close_price)
            except Exception:
                pass # Silently fail if data is bad

            # Add the new value to the dict/object
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
    view = request.GET.get('view', 'date')
    selected_date_str = request.GET.get('selected_date')
    search_term = request.GET.get('search_term', '').strip()

    stock_prices = []
    query_date = None
    download_name = "stock_prices.csv"

    # --- Get the latest date as a default ---
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
    
    # --- Logic for each view type (same as your other view) ---

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
            stock_prices = dictfetchall(cursor) # This returns a list of dicts
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
                stock_prices = dictfetchall(cursor) # This returns a list of dicts
        download_name = f"stock_prices_adjusted_{search_term.replace(' ', '_')}.csv"

    # --- Create the CSV response ---
    if not stock_prices:
        return HttpResponse("No data found for this query.", status=404)

    response = HttpResponse(content_type='text/csv')
    response['Content-Disposition'] = f'attachment; filename="{download_name}"'
    
    # Check if we have a list of dicts (from raw SQL) or a QuerySet (from ORM)
    if isinstance(stock_prices, list):
        # Data is from dictfetchall
        headers = stock_prices[0].keys()
        writer = csv.DictWriter(response, fieldnames=headers)
        writer.writeheader()
        writer.writerows(stock_prices)
    else:
        # Data is a QuerySet
        headers = [field.name for field in stock_prices.model._meta.fields]
        writer = csv.writer(response)
        writer.writerow(headers)
        for row in stock_prices:
            writer.writerow([getattr(row, field) for field in headers])

    return response

# --- Helper functions (from your Flask app) ---
def convert_float(value):
    if not value or value.strip() in ('', 'N/A', '-'):
        return None
    try:
        return Decimal(value.replace(',', ''))
    except InvalidOperation:
        return None

def convert_int(value):
    if not value or value.strip() in ('', 'N/A', '-'):
        return None
    try:
        return int(value.replace(',', ''))
    except (ValueError, InvalidOperation):
        return None

# --- Main View for Data Entry Page ---
def data_entry_view(request):
    # This view handles both GET (showing the page) and POST (uploading the file)

    if request.method == 'POST' and request.POST.get('action') == 'upload_price':
        # --- HANDLE FILE UPLOAD ---
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

            # Auto-detect date from first row
            business_date_str = all_rows_raw[0][1].strip()

            # Check for duplicates
            if StockPrices.objects.filter(business_date=business_date_str).exists():
                messages.error(request, f"Error: Data for date {business_date_str} (from file) already exists.")
                return redirect('nepse_data:data_entry')

            # Handle "extra comma in name" logic
            corrected_rows = []
            for row in all_rows_raw:
                if len(row) > expected_columns:
                    extra_col_count = len(row) - expected_columns
                    merged_name = ' '.join(row[4 : 4 + extra_col_count + 1])
                    corrected_row = row[:4] + [merged_name] + row[4 + extra_col_count + 1:]
                    corrected_rows.append(corrected_row)
                elif len(row) == expected_columns:
                    corrected_rows.append(row)

            inserted_rows = 0
            failed_rows = 0

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

            messages.success(request, f"Upload successful! Inserted {inserted_rows} records for {business_date_str}. Skipped {failed_rows} rows.")

        except Exception as e:
            messages.error(request, f"An error occurred: {e}")

        return redirect('nepse_data:data_entry')

    else:
        # --- HANDLE GET REQUEST ---
        # Fetch available dates for the delete modal
        available_dates = StockPrices.objects.values('business_date').distinct().order_by('-business_date')
        context = {
            'title': 'Data Entry Portal',
            'available_dates': available_dates
        }
        return render(request, 'nepse_data/data_entry.html', context)

@require_POST  # Ensures this view only accepts POST
def delete_price_data_view(request):
    dates_to_delete = request.POST.getlist('dates_to_delete')

    if not dates_to_delete:
        messages.warning(request, "No dates were selected for deletion.")
        return redirect('nepse_data:data_entry')

    try:
        # Delete from 'stock_prices_adj' first (from other app)
        StockPricesAdj.objects.filter(business_date__in=dates_to_delete).delete()

        # Then delete from 'stock_prices'
        count, _ = StockPrices.objects.filter(business_date__in=dates_to_delete).delete()

        messages.success(request, f"Successfully deleted all price data for {len(dates_to_delete)} selected date(s).")

    except Exception as e:
        messages.error(request, f"An error occurred while deleting: {e}")

    return redirect('nepse_data:data_entry')
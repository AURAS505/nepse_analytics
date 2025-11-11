# In: floorsheet_analysis/views.py

import mysql.connector
from mysql.connector import Error
from datetime import datetime, timedelta, date
from decimal import Decimal
import json

from django.shortcuts import render
from django.http import JsonResponse
from django.conf import settings

# --- Database Connection ---
# This reads from your main Django settings.py DATABASES config
db_settings = settings.DATABASES['default']

DB_CONFIG = {
    'host': db_settings.get('HOST', '127.0.0.1'),
    'user': db_settings.get('USER', 'root'),
    'password': db_settings.get('PASSWORD', ''),
    'database': db_settings.get('NAME', 'nepse_data'),
    'port': db_settings.get('PORT', 3306)
}

def create_connection():
    """Create a database connection using the config."""
    try:
        connection = mysql.connector.connect(**DB_CONFIG)
        return connection
    except Error as e:
        print(f"Error while connecting to MySQL: {e}")
        return None

# --- Data Fetching Functions (Ported from Flask) ---

def find_valid_trading_date(connection, target_date, available_dates, direction='closest'):
    """
    Finds the closest available trading date in the provided list of available_dates.
    """
    if not available_dates:
        return None

    if isinstance(target_date, datetime):
        target_date = target_date.date()

    available_dates_set = {d for d in available_dates}

    if target_date in available_dates_set:
        return target_date

    sorted_available_dates = sorted(available_dates)

    if direction == 'closest':
        closest_date = min(sorted_available_dates, key=lambda d: abs((d - target_date).days))
        return closest_date
    elif direction == 'previous':
        suitable_dates = [d for d in sorted_available_dates if d <= target_date]
        if suitable_dates:
            return max(suitable_dates)
        return sorted_available_dates[0]
    elif direction == 'next':
        suitable_dates = [d for d in sorted_available_dates if d >= target_date]
        if suitable_dates:
            return min(suitable_dates)
        return sorted_available_dates[-1]

    return None

def get_broker_settlement_data(start_date, end_date):
    """
    Fetches and calculates broker settlement data including buy, sell, total, difference,
    and internal matching amounts for a given date range.
    """
    connection = create_connection()
    if connection is None:
        return []

    cursor = connection.cursor(dictionary=True)
    
    # Query: Get Buy/Sell amounts from summary tables
    settlement_query = """
    SELECT
        b.broker_no,
        b.name AS broker_name,
        COALESCE(SUM(T.buy_amount), 0) AS buy_amount,
        COALESCE(SUM(T.sell_amount), 0) AS sell_amount
    FROM
        brokers b
    LEFT JOIN (
        SELECT buyer AS broker_no, total_amount AS buy_amount, 0 AS sell_amount
        FROM sector_buyer_summary
        WHERE calculation_date BETWEEN %s AND %s
        UNION ALL
        SELECT seller AS broker_no, 0 AS buy_amount, total_amount AS sell_amount
        FROM sector_seller_summary
        WHERE calculation_date BETWEEN %s AND %s
    ) AS T ON b.broker_no = T.broker_no
    GROUP BY b.broker_no, b.name
    HAVING buy_amount > 0 OR sell_amount > 0
    """
    
    # Query: Get internal matching amounts from raw floorsheet data
    matching_query = """
    SELECT
        buyer AS broker_no,
        SUM(amount) AS matching_amount
    FROM floorsheet_raw
    WHERE buyer = seller AND calculation_date BETWEEN %s AND %s
    GROUP BY buyer
    """
    
    try:
        cursor.execute(settlement_query, (start_date, end_date, start_date, end_date))
        settlement_data = cursor.fetchall()
        
        cursor.execute(matching_query, (start_date, end_date))
        matching_data = cursor.fetchall()
        
        matching_map = {item['broker_no']: item['matching_amount'] for item in matching_data}

        for row in settlement_data:
            row['total_amount'] = row['buy_amount'] + row['sell_amount']
            row['difference'] = row['buy_amount'] - row['sell_amount']
            row['matching_amount'] = matching_map.get(row['broker_no'], 0)

        sorted_data = sorted(settlement_data, key=lambda x: x['total_amount'], reverse=True)
        return sorted_data

    except Error as e:
        print(f"Error fetching broker settlement data: {e}")
        return []
    finally:
        cursor.close()
        connection.close()

def get_daywise_broker_settlement_data(broker_no, start_date, end_date):
    """
    Fetches and calculates day-wise settlement data for a SINGLE broker.
    """
    connection = create_connection()
    if connection is None:
        return []

    cursor = connection.cursor(dictionary=True)
    
    settlement_query = """
    SELECT
        b.broker_no,
        b.name AS broker_name,
        T.calculation_date,
        COALESCE(SUM(T.buy_amount), 0) AS buy_amount,
        COALESCE(SUM(T.sell_amount), 0) AS sell_amount
    FROM
        brokers b
    LEFT JOIN (
        SELECT buyer AS broker_no, total_amount AS buy_amount, 0 AS sell_amount, calculation_date
        FROM sector_buyer_summary
        WHERE calculation_date BETWEEN %s AND %s AND buyer = %s
        UNION ALL
        SELECT seller AS broker_no, 0 AS buy_amount, total_amount AS sell_amount, calculation_date
        FROM sector_seller_summary
        WHERE calculation_date BETWEEN %s AND %s AND seller = %s
    ) AS T ON b.broker_no = T.broker_no
    WHERE b.broker_no = %s
    GROUP BY b.broker_no, b.name, T.calculation_date
    HAVING T.calculation_date IS NOT NULL
    ORDER BY T.calculation_date DESC
    """
    
    matching_query = """
    SELECT
        calculation_date,
        SUM(amount) AS matching_amount
    FROM floorsheet_raw
    WHERE buyer = seller AND buyer = %s AND calculation_date BETWEEN %s AND %s
    GROUP BY calculation_date
    """
    
    try:
        cursor.execute(settlement_query, (start_date, end_date, broker_no, start_date, end_date, broker_no, broker_no))
        settlement_data = cursor.fetchall()
        
        cursor.execute(matching_query, (broker_no, start_date, end_date))
        matching_data = cursor.fetchall()
        matching_map = {item['calculation_date']: item['matching_amount'] for item in matching_data}

        for row in settlement_data:
            row['total_amount'] = row['buy_amount'] + row['sell_amount']
            row['difference'] = row['buy_amount'] - row['sell_amount']
            row['matching_amount'] = matching_map.get(row['calculation_date'], 0)

        return settlement_data

    except Error as e:
        print(f"Error fetching day-wise broker settlement data: {e}")
        return []
    finally:
        cursor.close()
        connection.close()

def get_broker_sector_details(broker_no, start_date, end_date):
    """ Fetches sector-wise details for the modal """
    connection = create_connection()
    if not connection:
        return {"error": "Database connection failed"}

    cursor = connection.cursor(dictionary=True)
    try:
        sector_query = """
        SELECT sector, SUM(buy_amount) AS sector_buy, SUM(sell_amount) AS sector_sell
        FROM (
            SELECT sector, total_amount AS buy_amount, 0 AS sell_amount FROM sector_buyer_summary WHERE buyer = %s AND calculation_date BETWEEN %s AND %s
            UNION ALL
            SELECT sector, 0 AS buy_amount, total_amount AS sell_amount FROM sector_seller_summary WHERE seller = %s AND calculation_date BETWEEN %s AND %s
        ) AS combined GROUP BY sector
        """
        cursor.execute(sector_query, (broker_no, start_date, end_date, broker_no, start_date, end_date))
        sector_data = cursor.fetchall()

        broker_totals_query = """
        SELECT 
            (SELECT COALESCE(SUM(total_amount), 0) FROM sector_buyer_summary WHERE buyer = %s AND calculation_date BETWEEN %s AND %s) as total_buy,
            (SELECT COALESCE(SUM(total_amount), 0) FROM sector_seller_summary WHERE seller = %s AND calculation_date BETWEEN %s AND %s) as total_sell
        """
        cursor.execute(broker_totals_query, (broker_no, start_date, end_date, broker_no, start_date, end_date))
        broker_totals = cursor.fetchone()

        grand_total_query = "SELECT COALESCE(SUM(total_amount), 0) AS total FROM sector_buyer_summary WHERE calculation_date BETWEEN %s AND %s"
        cursor.execute(grand_total_query, (start_date, end_date))
        grand_total_turnover = cursor.fetchone()['total']

        return {
            "sector_data": sector_data,
            "broker_totals": broker_totals,
            "grand_total_turnover": grand_total_turnover
        }
    except Error as e:
        print(f"Error fetching broker sector details: {e}")
        return {"error": "Failed to fetch data"}
    finally:
        cursor.close()
        connection.close()

def get_broker_script_details(broker_no, sector, start_date, end_date, broker_total_buy, broker_total_sell):
    """ Fetches script-wise details for the modal """
    connection = create_connection()
    if not connection:
        return {"error": "Database connection failed"}

    cursor = connection.cursor(dictionary=True)
    try:
        script_query = """
        SELECT stock_symbol, SUM(buy_amount) as script_buy, SUM(sell_amount) as script_sell
        FROM (
            SELECT bs.stock_symbol, bs.total_amount AS buy_amount, 0 AS sell_amount FROM buyer_summary bs JOIN companies c ON bs.stock_symbol = c.script_ticker WHERE bs.buyer = %s AND c.sector = %s AND bs.calculation_date BETWEEN %s AND %s
            UNION ALL
            SELECT ss.stock_symbol, 0 AS buy_amount, ss.total_amount AS sell_amount FROM seller_summary ss JOIN companies c ON ss.stock_symbol = c.script_ticker WHERE ss.seller = %s AND c.sector = %s AND ss.calculation_date BETWEEN %s AND %s
        ) AS combined_scripts GROUP BY stock_symbol
        """
        cursor.execute(script_query, (broker_no, sector, start_date, end_date, broker_no, sector, start_date, end_date))
        script_data = cursor.fetchall()

        sector_totals_query = """
        SELECT
            (SELECT COALESCE(SUM(bs.total_amount), 0) FROM buyer_summary bs JOIN companies c ON bs.stock_symbol = c.script_ticker WHERE bs.buyer = %s AND c.sector = %s AND bs.calculation_date BETWEEN %s AND %s) as total_buy,
            (SELECT COALESCE(SUM(ss.total_amount), 0) FROM seller_summary ss JOIN companies c ON ss.stock_symbol = c.script_ticker WHERE ss.seller = %s AND c.sector = %s AND ss.calculation_date BETWEEN %s AND %s) as total_sell
        """
        cursor.execute(sector_totals_query, (broker_no, sector, start_date, end_date, broker_no, sector, start_date, end_date))
        sector_totals = cursor.fetchone()

        return {
            "script_data": script_data,
            "sector_totals": sector_totals,
            "broker_totals": {"total_buy": broker_total_buy, "total_sell": broker_total_sell}
        }
    except Error as e:
        print(f"Error fetching broker script details: {e}")
        return {"error": "Failed to fetch data"}
    finally:
        cursor.close()
        connection.close()


# --- Django Views ---

def settlement_report(request):
    """
    Main view for the Broker Settlement Report page.
    """
    connection = create_connection()
    if connection is None:
        return render(request, 'floorsheet_analysis/settlement_report.html', {'error': 'Database connection failed.'})

    cursor = connection.cursor(dictionary=True)
    cursor.execute("""
        SELECT DISTINCT calculation_date FROM sector_buyer_summary
        UNION
        SELECT DISTINCT calculation_date FROM sector_seller_summary
        ORDER BY calculation_date DESC
    """)
    available_dates_db = sorted([d['calculation_date'] for d in cursor.fetchall()], reverse=True)
    latest_available_date = available_dates_db[0] if available_dates_db else date.today()

    # Handle form data from POST or GET
    form_data = request.POST if request.method == 'POST' else request.GET
    search_broker_no = form_data.get('search_broker_no')
    selected_date_range_type = form_data.get('date_range_type', 'current_day')

    if request.method == 'POST' and search_broker_no and search_broker_no.strip().isdigit() and selected_date_range_type == 'current_day':
        selected_date_range_type = 'custom'
        end_date = latest_available_date
        potential_start_date = end_date - timedelta(days=60)
        start_date = find_valid_trading_date(connection, potential_start_date, available_dates_db, direction='next') or latest_available_date
    else:
        start_date = latest_available_date
        end_date = latest_available_date

        end_date_str_form = form_data.get('end_date')
        if end_date_str_form:
            try:
                potential_end_date = datetime.strptime(end_date_str_form, '%Y-%m-%d').date()
                end_date = find_valid_trading_date(connection, potential_end_date, available_dates_db, direction='previous') or latest_available_date
            except (ValueError, TypeError):
                pass
        
        range_map = {
            '2_days': 2, '3_days': 3, '4_days': 4, '5_days': 5,
            '1_week': 7, 'fortnight': 15, 'monthly': 30
        }
        if selected_date_range_type in range_map:
            count = 0
            temp_start_date = end_date
            for d in available_dates_db:
                if d <= end_date:
                    count += 1
                    temp_start_date = d
                    if count >= range_map[selected_date_range_type]:
                        break
            start_date = temp_start_date
        elif selected_date_range_type == 'custom':
            start_date_str_form = form_data.get('start_date')
            if start_date_str_form:
                try:
                    potential_start_date = datetime.strptime(start_date_str_form, '%Y-%m-%d').date()
                    start_date = find_valid_trading_date(connection, potential_start_date, available_dates_db, direction='next') or latest_available_date
                except (ValueError, TypeError):
                    pass
    
    if start_date is None: start_date = latest_available_date
    if end_date is None: end_date = latest_available_date

    if start_date > end_date:
        start_date, end_date = end_date, start_date

    settlement_data = []
    is_daywise_view = False
    
    if search_broker_no and search_broker_no.strip().isdigit():
        settlement_data = get_daywise_broker_settlement_data(int(search_broker_no.strip()), start_date, end_date)
        is_daywise_view = True
    else:
        settlement_data = get_broker_settlement_data(start_date, end_date)

    cursor.close()
    connection.close()

    # Helper function to serialize data for JavaScript
    def json_default(obj):
        if isinstance(obj, (date, datetime)):
            return obj.isoformat()
        if isinstance(obj, Decimal):
            return float(obj)
        raise TypeError(f"Type {type(obj)} not serializable")

    context = {
        'settlement_data': settlement_data,
        'settlement_data_json': json.dumps(settlement_data, default=json_default),
        'start_date': start_date.strftime('%Y-%m-%d'),
        'end_date': end_date.strftime('%Y-%m-%d'),
        'selected_date_range_type': selected_date_range_type,
        'available_dates_db': json.dumps([d.strftime('%Y-%m-%d') for d in available_dates_db]),
        'search_broker_no': search_broker_no,
        'is_daywise_view': is_daywise_view
    }
    
    return render(request, 'floorsheet_analysis/settlement_report.html', context)


# --- JSON API Views for Modals ---

def broker_sector_details(request):
    """
    Fetches sector-wise buy/sell for the modal.
    """
    broker_no = request.GET.get('broker_no')
    start_date = request.GET.get('start_date')
    end_date = request.GET.get('end_date')

    if not all([broker_no, start_date, end_date]):
        return JsonResponse({"error": "Missing required parameters"}, status=400)

    data = get_broker_sector_details(broker_no, start_date, end_date)
    
    # Convert Decimal to float for JSON serialization
    for row in data.get('sector_data', []):
        row['sector_buy'] = float(row['sector_buy'])
        row['sector_sell'] = float(row['sector_sell'])
    if 'broker_totals' in data:
        data['broker_totals']['total_buy'] = float(data['broker_totals']['total_buy'])
        data['broker_totals']['total_sell'] = float(data['broker_totals']['total_sell'])
    if 'grand_total_turnover' in data:
        data['grand_total_turnover'] = float(data['grand_total_turnover'])

    return JsonResponse(data)


def broker_script_details(request):
    """
    Fetches script-wise buy/sell for the modal.
    """
    broker_no = request.GET.get('broker_no')
    sector = request.GET.get('sector')
    start_date = request.GET.get('start_date')
    end_date = request.GET.get('end_date')
    broker_total_buy = float(request.GET.get('broker_total_buy', 0))
    broker_total_sell = float(request.GET.get('broker_total_sell', 0))

    if not all([broker_no, sector, start_date, end_date]):
        return JsonResponse({"error": "Missing required parameters"}, status=400)

    data = get_broker_script_details(broker_no, sector, start_date, end_date, broker_total_buy, broker_total_sell)
    
    # Convert Decimal to float for JSON serialization
    for row in data.get('script_data', []):
        row['script_buy'] = float(row['script_buy'])
        row['script_sell'] = float(row['script_sell'])
    if 'sector_totals' in data:
        data['sector_totals']['total_buy'] = float(data['sector_totals']['total_buy'])
        data['sector_totals']['total_sell'] = float(data['sector_totals']['total_sell'])

    return JsonResponse(data)

# In: floorsheet_analysis/views.py
# --- ADD ALL THE CODE BELOW TO THE END OF THE FILE ---

def get_broker_name_map():
    """ Fetches all broker numbers and names. """
    connection = create_connection()
    if connection is None:
        return {}
    
    cursor = connection.cursor(dictionary=True)
    broker_name_map = {}
    try:
        cursor.execute("SELECT broker_no, name FROM brokers")
        all_brokers = cursor.fetchall()
        broker_name_map = {str(broker['broker_no']): broker['name'] for broker in all_brokers}
    except Error as e:
        print(f"Error fetching broker map: {e}")
    finally:
        cursor.close()
        connection.close()
    return broker_name_map


def get_summary_data_for_range(stock_symbol, start_date, end_date):
    """
    Fetches buyer and seller summary data for a given stock and date range.
    Calculates range-specific overall summary statistics.
    """
    connection = create_connection()
    if connection is None:
        return [], [], {'total_traded_kitta': 0, 'total_amount': 0, 'atr': 0}

    cursor = connection.cursor(dictionary=True)
    buyer_data = []
    seller_data = []
    overall_summary = {'total_traded_kitta': 0, 'total_amount': 0, 'atr': 0}

    try:
        # Fetch buyer data
        query_buyer = """
        SELECT buyer, SUM(total_quantity) as total_quantity, SUM(total_amount) as total_amount
        FROM buyer_summary
        WHERE stock_symbol = %s AND calculation_date BETWEEN %s AND %s
        GROUP BY buyer
        ORDER BY total_amount DESC
        """
        cursor.execute(query_buyer, (stock_symbol, start_date, end_date))
        buyer_data = cursor.fetchall()
        for row in buyer_data:
            row['average_rate'] = (row['total_amount'] / row['total_quantity']) if row['total_quantity'] else 0

        # Fetch seller data
        query_seller = """
        SELECT seller, SUM(total_quantity) as total_quantity, SUM(total_amount) as total_amount
        FROM seller_summary
        WHERE stock_symbol = %s AND calculation_date BETWEEN %s AND %s
        GROUP BY seller
        ORDER BY total_amount DESC
        """
        cursor.execute(query_seller, (stock_symbol, start_date, end_date))
        seller_data = cursor.fetchall()
        for row in seller_data:
            row['average_rate'] = (row['total_amount'] / row['total_quantity']) if row['total_quantity'] else 0

        # Calculate range-specific totals
        query_buy_totals = """
        SELECT
            SUM(total_quantity) AS total_buy_quantity,
            SUM(total_amount) AS total_buy_amount
        FROM buyer_summary
        WHERE stock_symbol = %s AND calculation_date BETWEEN %s AND %s
        """
        cursor.execute(query_buy_totals, (stock_symbol, start_date, end_date))
        buy_totals = cursor.fetchone()

        total_traded_kitta = buy_totals['total_buy_quantity'] or 0
        total_amount = buy_totals['total_buy_amount'] or 0
        atr = total_amount / total_traded_kitta if total_traded_kitta else 0

        overall_summary = {
            'total_traded_kitta': total_traded_kitta,
            'total_amount': total_amount,
            'atr': atr
        }

    except Error as e:
        print(f"Error in get_summary_data_for_range: {e}")
    finally:
        cursor.close()
        connection.close()

    return buyer_data, seller_data, overall_summary


def get_broker_net_data(stock_symbol, start_date, end_date):
    """Calculates the net quantity (buy - sell) for each broker for a given stock and date range."""
    connection = create_connection()
    if connection is None:
        return []

    cursor = connection.cursor(dictionary=True)
    broker_net_data = []
    
    try:
        query_net_data = """
        SELECT
            broker,
            SUM(CASE WHEN type = 'buy' THEN quantity ELSE 0 END) as total_buy_quantity,
            SUM(CASE WHEN type = 'sell' THEN quantity ELSE 0 END) as total_sell_quantity,
            SUM(CASE WHEN type = 'buy' THEN quantity ELSE 0 END) - SUM(CASE WHEN type = 'sell' THEN quantity ELSE 0 END) as net_quantity
        FROM (
            SELECT buyer as broker, total_quantity as quantity, 'buy' as type
            FROM buyer_summary
            WHERE stock_symbol = %s AND calculation_date BETWEEN %s AND %s
            UNION ALL
            SELECT seller as broker, total_quantity as quantity, 'sell' as type
            FROM seller_summary
            WHERE stock_symbol = %s AND calculation_date BETWEEN %s AND %s
        ) as combined_data
        GROUP BY broker
        """
        cursor.execute(query_net_data, (stock_symbol, start_date, end_date, stock_symbol, start_date, end_date))
        broker_net_data = cursor.fetchall()

    except Error as e:
        print(f"Error in get_broker_net_data: {e}")
    finally:
        cursor.close()
        connection.close()
    
    return broker_net_data


def company_trades_report(request):
    """
    Main view for the Company-wise Historical Analysis page.
    """
    connection = create_connection()
    if connection is None:
        return render(request, 'floorsheet_analysis/company_trades_report.html', {'error': 'Database connection failed.'})

    cursor = connection.cursor(dictionary=True)
    
    try:
        cursor.execute("SELECT DISTINCT stock_symbol FROM buyer_summary ORDER BY stock_symbol")
        stocks = cursor.fetchall()
        
        broker_name_map = get_broker_name_map()

        cursor.execute("""
            SELECT DISTINCT calculation_date FROM buyer_summary
            UNION
            SELECT DISTINCT calculation_date FROM seller_summary
            ORDER BY calculation_date DESC
        """)
        available_dates_db = sorted([d['calculation_date'] for d in cursor.fetchall()], reverse=True)
        
    except Error as e:
        print(f"Error fetching initial data: {e}")
        stocks = []
        broker_name_map = {}
        available_dates_db = []
    
    finally:
        cursor.close()
        # Don't close connection yet, find_valid_trading_date might need it (oh, wait, it doesn't)
        # But the date logic below does. Let's re-open if needed or just keep it.
        # Re-reading my code: find_valid_trading_date does not need the connection.
        # So we can close it.
        if connection.is_connected():
            connection.close()

    latest_available_date = available_dates_db[0] if available_dates_db else date.today()

    # Handle form data from POST or GET
    form_data = request.POST if request.method == 'POST' else request.GET
    
    selected_stock = form_data.get('stock_symbol', stocks[0]['stock_symbol'] if stocks else None)
    selected_date_range_type = form_data.get('date_range_type', 'current_day')

    start_date = latest_available_date
    end_date = latest_available_date
    end_date_str_form = form_data.get('end_date')

    if end_date_str_form:
        try:
            potential_end_date = datetime.strptime(end_date_str_form, '%Y-%m-%d').date()
            end_date = find_valid_trading_date(None, potential_end_date, available_dates_db, direction='previous') or latest_available_date
        except (ValueError, TypeError):
            pass

    if selected_date_range_type == 'current_day':
        start_date = latest_available_date
        end_date = latest_available_date
    elif selected_date_range_type == '1_week':
        days_to_find = 7
    elif selected_date_range_type == 'fortnight':
        days_to_find = 15
    elif selected_date_range_type == 'monthly':
        days_to_find = 30
    elif selected_date_range_type == 'quarterly':
        days_to_find = 90
    elif selected_date_range_type == 'semi_annually':
        days_to_find = 180
    elif selected_date_range_type == 'yearly':
        days_to_find = 360
    elif selected_date_range_type == 'custom':
        days_to_find = 0
        start_date_str_form = form_data.get('start_date')
        try:
            if start_date_str_form:
                potential_start_date = datetime.strptime(start_date_str_form, '%Y-%m-%d').date()
                start_date = find_valid_trading_date(None, potential_start_date, available_dates_db, direction='next') or latest_available_date
        except (ValueError, TypeError):
            pass
    
    if selected_date_range_type not in ['custom', 'current_day']:
        count = 0
        temp_start_date = end_date
        for d in available_dates_db:
            if d <= end_date:
                count += 1
                temp_start_date = d
                if count >= days_to_find:
                    break
        start_date = temp_start_date

    if start_date > end_date:
        start_date, end_date = end_date, start_date
    
    if start_date is None: start_date = latest_available_date
    if end_date is None: end_date = latest_available_date

    buyer_data, seller_data, overall_summary = get_summary_data_for_range(selected_stock, start_date, end_date)
    broker_net_data = get_broker_net_data(selected_stock, start_date, end_date)

    broker_net_data_asc = sorted(broker_net_data, key=lambda x: x['net_quantity'])
    broker_net_data_desc = sorted(broker_net_data, key=lambda x: x['net_quantity'], reverse=True)

    # Helper function to serialize data for JavaScript
    def json_default_decimal(obj):
        if isinstance(obj, Decimal):
            return float(obj)
        raise TypeError

    context = {
        'buyer_data': buyer_data,
        'seller_data': seller_data,
        'stocks': stocks,
        'start_date': start_date.strftime('%Y-%m-%d'),
        'end_date': end_date.strftime('%Y-%m-%d'),
        'selected_stock': selected_stock,
        'selected_date_range_type': selected_date_range_type,
        'overall_summary': overall_summary,
        'broker_net_data_asc': broker_net_data_asc,
        'broker_net_data_desc': broker_net_data_desc,
        'available_dates_db_json': json.dumps([d.strftime('%Y-%m-%d') for d in available_dates_db]),
        'broker_name_map_json': json.dumps(broker_name_map, default=json_default_decimal)
    }

    return render(request, 'floorsheet_analysis/company_trades_report.html', context)

def get_floorsheet_details(request):
    """
    Fetches detailed floorsheet transactions for a specific buyer, seller, or broker.
    This is an API endpoint called by JavaScript.
    """
    
    if request.method != 'POST':
        return JsonResponse({"error": "Only POST method is allowed"}, status=405)

    try:
        data = json.loads(request.body)
        stock_symbol = data.get('stock_symbol')
        start_date_str = data.get('start_date')
        end_date_str = data.get('end_date')
        detail_type = data.get('type')
        detail_name = data.get('name')
    except json.JSONDecodeError:
        return JsonResponse({"error": "Invalid JSON"}, status=400)

    if not all([stock_symbol, start_date_str, end_date_str, detail_type, detail_name]):
        return JsonResponse({"error": "Missing parameters"}, status=400)

    conn = create_connection()
    detailed_data = []

    if conn:
        cursor = conn.cursor(dictionary=True)
        try:
            start_date_obj = datetime.strptime(start_date_str, '%Y-%m-%d').date()
            end_date_obj = datetime.strptime(end_date_str, '%Y-%m-%d').date()

            query = """
                SELECT id, contract_no, stock_symbol, buyer, seller, quantity, rate, amount, sector, calculation_date
                FROM floorsheet_raw
                WHERE stock_symbol = %s AND calculation_date BETWEEN %s AND %s
            """
            params = [stock_symbol, start_date_obj, end_date_obj]

            if detail_type == 'buyer':
                query += " AND buyer = %s"
                params.append(detail_name)
            elif detail_type == 'seller':
                query += " AND seller = %s"
                params.append(detail_name)
            elif detail_type == 'broker':
                query += " AND (buyer = %s OR seller = %s)"
                params.append(detail_name)
                params.append(detail_name)

            cursor.execute(query, tuple(params))
            detailed_data = cursor.fetchall()

        except mysql.connector.Error as err:
            print(f"Error fetching detailed data: {err}")
            return JsonResponse({"error": "Database error"}, status=500)
        except ValueError as verr:
            print(f"Date parsing error: {verr}")
            return JsonResponse({"error": "Invalid date format"}, status=400)
        finally:
            cursor.close()
            conn.close()

    # --- THIS IS THE FIX ---
    # The 'default' argument and the helper function are not needed.
    # Django's JsonResponse handles Decimal and date objects automatically.
    
    return JsonResponse(detailed_data, safe=False)


def get_broker_transaction_summary(broker_no, start_date, end_date):
    """
    Fetches a summary of all stocks bought and sold by a specific broker
    within a given date range.
    """
    connection = create_connection()
    if connection is None:
        return [], []  # Return empty lists for buy and sell data

    cursor = connection.cursor(dictionary=True)
    buy_data = []
    sell_data = []

    try:
        # Query for all stocks BOUGHT by the broker in the date range
        query_buyer = """
        SELECT
            stock_symbol,
            SUM(total_quantity) as total_quantity,
            SUM(total_amount) as total_amount
        FROM buyer_summary
        WHERE buyer = %s AND calculation_date BETWEEN %s AND %s
        GROUP BY stock_symbol
        ORDER BY total_amount DESC
        """
        cursor.execute(query_buyer, (broker_no, start_date, end_date))
        buy_data = cursor.fetchall()

        # Query for all stocks SOLD by the broker in the date range
        query_seller = """
        SELECT
            stock_symbol,
            SUM(total_quantity) as total_quantity,
            SUM(total_amount) as total_amount
        FROM seller_summary
        WHERE seller = %s AND calculation_date BETWEEN %s AND %s
        GROUP BY stock_symbol
        ORDER BY total_amount DESC
        """
        cursor.execute(query_seller, (broker_no, start_date, end_date))
        sell_data = cursor.fetchall()

    except Error as e:
        print(f"Error fetching broker transaction summary: {e}")
    finally:
        cursor.close()
        connection.close()

    return buy_data, sell_data


def calculate_net_summary(buy_data, sell_data):
    """Combines buy and sell data to calculate net positions for each stock."""
    net_summary = {}

    # Process buy data
    for item in buy_data:
        symbol = item['stock_symbol']
        if symbol not in net_summary:
            net_summary[symbol] = {'buy_quantity': 0, 'sell_quantity': 0}
        net_summary[symbol]['buy_quantity'] += item['total_quantity']

    # Process sell data
    for item in sell_data:
        symbol = item['stock_symbol']
        if symbol not in net_summary:
            net_summary[symbol] = {'buy_quantity': 0, 'sell_quantity': 0}
        net_summary[symbol]['sell_quantity'] += item['total_quantity']

    # Calculate net values and format the output list
    net_data = []
    for symbol, data in net_summary.items():
        net_quantity = data['buy_quantity'] - data['sell_quantity']
        if net_quantity != 0:  # Only include if there is a net position
            net_data.append({
                'stock_symbol': symbol,
                'net_quantity': net_quantity,
            })

    # Sort by absolute net quantity, descending
    net_data.sort(key=lambda x: abs(x['net_quantity']), reverse=True)
    return net_data

def get_stock_holding_history(stock_symbol, broker_nos, start_date, end_date):
    """
    Calculates the day-wise buy, sell, net, and cumulative holding history for
    a specific stock and a LIST of brokers within the given date range.
    """
    connection = create_connection()
    if not connection: return []
    if not broker_nos: return []

    cursor = connection.cursor(dictionary=True)
    history_data = []
    try:
        broker_placeholders = ', '.join(['%s'] * len(broker_nos))
        daily_summary_query = f"""
        SELECT
            calculation_date,
            SUM(CASE WHEN buyer IN ({broker_placeholders}) THEN quantity ELSE 0 END) as buy_quantity,
            SUM(CASE WHEN buyer IN ({broker_placeholders}) THEN amount ELSE 0 END) as buy_amount,
            SUM(CASE WHEN seller IN ({broker_placeholders}) THEN quantity ELSE 0 END) as sell_quantity,
            SUM(CASE WHEN seller IN ({broker_placeholders}) THEN amount ELSE 0 END) as sell_amount
        FROM floorsheet_raw
        WHERE
            stock_symbol = %s
            AND (buyer IN ({broker_placeholders}) OR seller IN ({broker_placeholders}))
            AND calculation_date BETWEEN %s AND %s
        GROUP BY calculation_date
        ORDER BY calculation_date ASC
        """
        params_list = []
        params_list.extend(broker_nos) # for buy_quantity
        params_list.extend(broker_nos) # for buy_amount
        params_list.extend(broker_nos) # for sell_quantity
        params_list.extend(broker_nos) # for sell_amount
        params_list.append(stock_symbol)
        params_list.extend(broker_nos) # for WHERE buyer IN (...)
        params_list.extend(broker_nos) # for WHERE seller IN (...)
        params_list.append(start_date)
        params_list.append(end_date)
        params = tuple(params_list)

        cursor.execute(daily_summary_query, params)
        daily_transactions = cursor.fetchall()
        cumulative_qty = 0
        
        for day in daily_transactions:
            day['buy_rate'] = (day['buy_amount'] / day['buy_quantity']) if day['buy_quantity'] > 0 else 0
            day['sell_rate'] = (day['sell_amount'] / day['sell_quantity']) if day['sell_quantity'] > 0 else 0
            day['net_quantity'] = day['buy_quantity'] - day['sell_quantity']
            cumulative_qty += day['net_quantity']
            day['cumulative_qty'] = cumulative_qty
            history_data.append(day)
    except Error as e:
        print(f"Error fetching stock holding history: {e}")
    finally:
        cursor.close()
        connection.close()
    return history_data[::-1]




def broker_trades_report(request):
    """
    Main view for the Broker-wise Historical Analysis page.
    """
    connection = create_connection()
    if connection is None:
        return render(request, 'floorsheet_analysis/broker_trades_report.html', {'error': 'Database connection failed.'})

    cursor = connection.cursor(dictionary=True)
    
    try:
        cursor.execute("SELECT broker_no, name FROM brokers ORDER BY broker_no")
        all_brokers = cursor.fetchall()

        cursor.execute("""
            SELECT DISTINCT calculation_date FROM buyer_summary
            UNION
            SELECT DISTINCT calculation_date FROM seller_summary
            ORDER BY calculation_date DESC
        """)
        available_dates_db = sorted([d['calculation_date'] for d in cursor.fetchall()], reverse=True)
        
        # --- NEW: Get the company name map ---
        cursor.execute("SELECT script_ticker, company_name FROM companies")
        company_name_map = {row['script_ticker']: row['company_name'] for row in cursor.fetchall()}
        # --- END NEW ---
    
    except Error as e:
        print(f"Error fetching initial data: {e}")
        all_brokers = []
        available_dates_db = []
        company_name_map = {} # <-- Add this
    
    finally:
        if connection.is_connected():
            cursor.close()
            connection.close()

    latest_available_date = available_dates_db[0] if available_dates_db else date.today()

    form_data = request.POST if request.method == 'POST' else request.GET
    
    selected_broker = form_data.get('broker_no', all_brokers[0]['broker_no'] if all_brokers else None)
    selected_date_range_type = form_data.get('date_range_type', 'current_day')
    
    start_date = latest_available_date
    end_date = latest_available_date

    end_date_str_form = form_data.get('end_date')
    if end_date_str_form:
        try:
            potential_end_date = datetime.strptime(end_date_str_form, '%Y-%m-%d').date()
            end_date = find_valid_trading_date(None, potential_end_date, available_dates_db, direction='previous') or latest_available_date
        except (ValueError, TypeError):
            pass
            
    days_to_find = 0
    if selected_date_range_type == 'current_day':
        days_to_find = 1
    elif selected_date_range_type == '1_week':
        days_to_find = 7
    elif selected_date_range_type == 'fortnight':
        days_to_find = 15
    elif selected_date_range_type == 'monthly':
        days_to_find = 30
    elif selected_date_range_type == 'quarterly':
        days_to_find = 90
    elif selected_date_range_type == 'semi_annually':
        days_to_find = 180
    elif selected_date_range_type == 'yearly':
        days_to_find = 360
    elif selected_date_range_type == 'custom':
        start_date_str_form = form_data.get('start_date')
        if start_date_str_form:
            try:
                potential_start_date = datetime.strptime(start_date_str_form, '%Y-%m-%d').date()
                start_date = find_valid_trading_date(None, potential_start_date, available_dates_db, direction='next') or latest_available_date
            except (ValueError, TypeError):
                pass
    
    if days_to_find > 0:
        count = 0
        temp_start_date = end_date
        for d in available_dates_db:
            if d <= end_date:
                count += 1
                temp_start_date = d
                if count >= days_to_find:
                    break
        start_date = temp_start_date
    
    if start_date > end_date:
        start_date, end_date = end_date, start_date

    buy_data, sell_data = [], []
    if selected_broker:
        buy_data, sell_data = get_broker_transaction_summary(selected_broker, start_date, end_date)

    net_data = calculate_net_summary(buy_data, sell_data)
    total_buy_amount = sum(item['total_amount'] for item in buy_data)
    total_sell_amount = sum(item['total_amount'] for item in sell_data)
    
    for item in buy_data:
        item['avg_rate'] = (item['total_amount'] / item['total_quantity']) if item['total_quantity'] > 0 else 0
        item['ratio'] = (item['total_amount'] / total_buy_amount * 100) if total_buy_amount > 0 else 0
        
    for item in sell_data:
        item['avg_rate'] = (item['total_amount'] / item['total_quantity']) if item['total_quantity'] > 0 else 0
        item['ratio'] = (item['total_amount'] / total_sell_amount * 100) if total_sell_amount > 0 else 0

    context = {
        'brokers': all_brokers,
        'buy_data': buy_data,
        'sell_data': sell_data,
        'net_data': net_data,
        'selected_broker': int(selected_broker) if selected_broker else None,
        'start_date': start_date.strftime('%Y-%m-%d'),
        'end_date': end_date.strftime('%Y-%m-%d'),
        'total_buy_amount': total_buy_amount,
        'total_sell_amount': total_sell_amount,
        'available_dates_db_json': json.dumps([d.strftime('%Y-%m-%d') for d in available_dates_db]),
        'selected_date_range_type': selected_date_range_type,
        'company_name_map_json': json.dumps(company_name_map) # <-- ADD THIS LINE
    }
    
    return render(request, 'floorsheet_analysis/broker_trades_report.html', context)

# Code for stock_holding_history.html
# --- ADD ALL THE CODE BELOW TO THE END OF THE FILE ---

# In: floorsheet_analysis/views.py

# In: floorsheet_analysis/views.py

# ... (keep all your other functions above this one) ...

def stock_holding_history_report(request):
    """
    Renders the Stock Daily Holding History page.
    """
    connection = create_connection()
    if not connection:
        return render(request, 'floorsheet_analysis/stock_holding_history_report.html', {'error': 'Database connection failed.'})

    cursor = connection.cursor(dictionary=True)
    
    try:
        cursor.execute("SELECT DISTINCT stock_symbol FROM floorsheet_raw ORDER BY stock_symbol")
        stocks = cursor.fetchall()
        cursor.execute("SELECT broker_no, name FROM brokers ORDER BY broker_no")
        brokers = cursor.fetchall()
        
        cursor.execute("""
            SELECT DISTINCT calculation_date FROM floorsheet_raw
            ORDER BY calculation_date DESC
        """)
        available_dates_db = [d['calculation_date'] for d in cursor.fetchall()]
    
    except Error as e:
        print(f"Error fetching initial data: {e}")
        stocks = []
        brokers = []
        available_dates_db = []

    finally:
        if connection.is_connected():
            cursor.close()
            connection.close()

    history_data = []
    
    form_data = request.POST if request.method == 'POST' else request.GET
    selected_stock = form_data.get('stock_symbol', stocks[0]['stock_symbol'] if stocks else None)
    
    # Get the list of broker numbers as strings from the form
    if request.method == 'POST':
        selected_broker_nos_str = request.POST.getlist('broker_no') 
    else:
        selected_broker_nos_str = request.GET.getlist('broker_no')

    if not selected_broker_nos_str and brokers:
        selected_broker_nos_str = [str(brokers[0]['broker_no'])]
    
    # --- FIX 1: Convert strings to integers ---
    try:
        selected_broker_nos_int = [int(b) for b in selected_broker_nos_str]
    except ValueError:
        selected_broker_nos_int = []
    # --- END FIX 1 ---
    
    selected_period = form_data.get('time_period', '1M')

    # --- FIX 2: Define the list of periods here ---
    time_periods = ['1W', '1M', '3M', '6M', '1Y', '2Y', '3Y', '4Y', '5Y']
    # --- END FIX 2 ---

    trading_day_count_map = {
        '1W': 7, '1M': 30, '3M': 90, '6M': 180, '1Y': 365,
        '2Y': 730, '3Y': 1095, '4Y': 1460, '5Y': 1825
    }
    
    num_trading_days = trading_day_count_map.get(selected_period, 30)
    
    if available_dates_db:
        end_date = available_dates_db[0]
        start_date_index = min(num_trading_days - 1, len(available_dates_db) - 1)
        start_date = available_dates_db[start_date_index]
    else:
        end_date = date.today()
        start_date = end_date - timedelta(days=num_trading_days)

    if request.method == 'POST' or 'stock_symbol' in request.GET:
        history_data = get_stock_holding_history(selected_stock, selected_broker_nos_int, start_date, end_date)

    context = {
        'stocks': stocks,
        'brokers': brokers,
        'history_data': history_data,
        'selected_stock': selected_stock,
        'selected_broker_nos': selected_broker_nos_int, # <-- Pass the INTEGER list
        'selected_period': selected_period,
        'time_periods': time_periods, # <-- Pass the new list
        'start_date': start_date.strftime('%Y-%m-%d'),
        'end_date': end_date.strftime('%Y-%m-%d'),
        'broker_name_map_json': json.dumps(get_broker_name_map())
    }

    return render(request, 'floorsheet_analysis/stock_holding_history_report.html', context)


def get_top_brokers_for_stock(stock_symbol, start_date, end_date, limit=10):
    """
    Fetches top 10 net buyers and net sellers for a stock, including their average buy and sell rates.
    """
    connection = create_connection()
    if not connection:
        return {'net_buyers': [], 'net_sellers': []}

    cursor = connection.cursor(dictionary=True)
    
    query = """
    SELECT
        broker,
        SUM(buy_qty) as total_buy_quantity,
        SUM(buy_amount) as total_buy_amount,
        SUM(sell_qty) as total_sell_quantity,
        SUM(sell_amount) as total_sell_amount,
        SUM(buy_qty) - SUM(sell_qty) as net_quantity
    FROM (
        SELECT buyer as broker, quantity as buy_qty, amount as buy_amount, 0 as sell_qty, 0 as sell_amount FROM floorsheet_raw
        WHERE stock_symbol = %s AND calculation_date BETWEEN %s AND %s AND buyer IS NOT NULL
        UNION ALL
        SELECT seller as broker, 0 as buy_qty, 0 as buy_amount, quantity as sell_qty, amount as sell_amount FROM floorsheet_raw
        WHERE stock_symbol = %s AND calculation_date BETWEEN %s AND %s AND seller IS NOT NULL
    ) as combined_data
    GROUP BY broker
    HAVING net_quantity != 0
    """
    
    try:
        cursor.execute(query, (stock_symbol, start_date, end_date, stock_symbol, start_date, end_date))
        all_brokers_net = cursor.fetchall()
        
        for row in all_brokers_net:
            for key, value in row.items():
                if isinstance(value, Decimal):
                    row[key] = float(value)
            
            row['avg_buy_rate'] = row['total_buy_amount'] / row['total_buy_quantity'] if row['total_buy_quantity'] > 0 else 0
            row['avg_sell_rate'] = row['total_sell_amount'] / row['total_sell_quantity'] if row['total_sell_quantity'] > 0 else 0

        net_buyers = sorted([b for b in all_brokers_net if b['net_quantity'] > 0], key=lambda x: x['net_quantity'], reverse=True)
        net_sellers = sorted([b for b in all_brokers_net if b['net_quantity'] < 0], key=lambda x: x['net_quantity'])

        return {
            'net_buyers': net_buyers[:limit],
            'net_sellers': net_sellers[:limit]
        }

    except Error as e:
        print(f"Error fetching top brokers net summary: {e}")
        return {'net_buyers': [], 'net_sellers': []}
    finally:
        cursor.close()
        connection.close()


def stock_holding_history_report(request):
    """
    Renders the Stock Daily Holding History page.
    """
    connection = create_connection()
    if not connection:
        return render(request, 'floorsheet_analysis/stock_holding_history_report.html', {'error': 'Database connection failed.'})

    cursor = connection.cursor(dictionary=True)
    
    try:
        cursor.execute("SELECT DISTINCT stock_symbol FROM floorsheet_raw ORDER BY stock_symbol")
        stocks = cursor.fetchall()
        cursor.execute("SELECT broker_no, name FROM brokers ORDER BY broker_no")
        brokers = cursor.fetchall()
        
        cursor.execute("""
            SELECT DISTINCT calculation_date FROM floorsheet_raw
            ORDER BY calculation_date DESC
        """)
        available_dates_db = [d['calculation_date'] for d in cursor.fetchall()]
    
    except Error as e:
        print(f"Error fetching initial data: {e}")
        stocks = []
        brokers = []
        available_dates_db = []

    finally:
        if connection.is_connected():
            cursor.close()
            connection.close()

    history_data = []
    
    form_data = request.POST if request.method == 'POST' else request.GET
    selected_stock = form_data.get('stock_symbol', stocks[0]['stock_symbol'] if stocks else None)
    
    # Get the list of broker numbers as strings from the form
    if request.method == 'POST':
        selected_broker_nos_str = request.POST.getlist('broker_no') 
    else:
        selected_broker_nos_str = request.GET.getlist('broker_no')

    if not selected_broker_nos_str and brokers:
        selected_broker_nos_str = [str(brokers[0]['broker_no'])]
    
    # --- FIX 1: Convert strings to integers ---
    try:
        selected_broker_nos_int = [int(b) for b in selected_broker_nos_str]
    except ValueError:
        selected_broker_nos_int = []
    # --- END FIX 1 ---
    
    selected_period = form_data.get('time_period', '1M')

    # --- FIX 2: Define the list of periods here ---
    time_periods = ['1W', '1M', '3M', '6M', '1Y', '2Y', '3Y', '4Y', '5Y']
    # --- END FIX 2 ---

    trading_day_count_map = {
        '1W': 7, '1M': 30, '3M': 90, '6M': 180, '1Y': 365,
        '2Y': 730, '3Y': 1095, '4Y': 1460, '5Y': 1825
    }
    
    num_trading_days = trading_day_count_map.get(selected_period, 30)
    
    if available_dates_db:
        end_date = available_dates_db[0]
        start_date_index = min(num_trading_days - 1, len(available_dates_db) - 1)
        start_date = available_dates_db[start_date_index]
    else:
        end_date = date.today()
        start_date = end_date - timedelta(days=num_trading_days)

    # Check if form was submitted (by checking for stock_symbol in GET)
    if request.method == 'POST' or 'stock_symbol' in request.GET:
        history_data = get_stock_holding_history(selected_stock, selected_broker_nos_int, start_date, end_date)

    context = {
        'stocks': stocks,
        'brokers': brokers,
        'history_data': history_data,
        'selected_stock': selected_stock,
        'selected_broker_nos': selected_broker_nos_int, # <-- Pass the INTEGER list
        'selected_period': selected_period,
        'time_periods': time_periods, # <-- Pass the new list
        'start_date': start_date.strftime('%Y-%m-%d'),
        'end_date': end_date.strftime('%Y-%m-%d'),
        'broker_name_map_json': json.dumps(get_broker_name_map())
    }

    return render(request, 'floorsheet_analysis/stock_holding_history_report.html', context)


def api_stock_summary_report(request):
    """
    API endpoint for the "Top Brokers" modal on the stock holding history page.
    """
    stock_symbol = request.GET.get('stock_symbol')
    start_date = request.GET.get('start_date')
    end_date = request.GET.get('end_date')

    if not all([stock_symbol, start_date, end_date]):
        return JsonResponse({"error": "Missing required parameters"}, status=400)

    data = get_top_brokers_for_stock(stock_symbol, start_date, end_date)
    return JsonResponse(data)
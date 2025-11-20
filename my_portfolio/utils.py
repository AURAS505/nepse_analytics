# my_portfolio/utils.py
from collections import defaultdict
from decimal import Decimal, ROUND_HALF_UP
from django.db import connection
from .models import Transaction

# --- Helper Functions (Moved from views.py) ---

def dictfetchall(cursor):
    "Return all rows from a cursor as a dict"
    columns = [col[0] for col in cursor.description]
    return [dict(zip(columns, row)) for row in cursor.fetchall()]

def fmt_currency_short(value):
    if value is None: return "-"
    try:
        val = Decimal(str(value))
    except:
        return value
    if val == 0: return "-"
    abs_val = abs(val)
    if abs_val >= 10000000: return f"{val/10000000:.2f}C"
    elif abs_val >= 100000: return f"{val/100000:.2f}L"
    elif abs_val >= 1000:   return f"{val/1000:.2f}T"
    else: return f"{val:,.0f}"

# --- Valuation Logic (Moved from views.py) ---

def _get_valuation_data(start_date, end_date):
    """
    Calculates the full valuation report, including opening,
    movements (buy/sale/bonus), and closing balances.
    """
    
    # 1. Fetch ALL Transactions up to end_date
    transactions = Transaction.objects.filter(
        date__lte=end_date
    ).select_related('symbol').order_by('symbol__sector', 'symbol__script_ticker', 'date', 'created_at')

    # 2. Fetch Latest Prices using Raw SQL (Efficient for snapshots)
    latest_prices = {}
    try:
        with connection.cursor() as cursor:
            cursor.execute("""
                WITH RankedPrices AS (
                    SELECT symbol, close_price, business_date,
                        ROW_NUMBER() OVER(PARTITION BY symbol ORDER BY business_date DESC) as rn
                    FROM stock_prices
                    WHERE business_date <= %s
                )
                SELECT symbol, close_price, business_date FROM RankedPrices WHERE rn = 1;
            """, [end_date])
            for row in dictfetchall(cursor):
                latest_prices[row['symbol']] = {
                    'close_price': row.get('close_price') or Decimal('0.0'),
                    'business_date': row.get('business_date')
                }
    except Exception as e:
        print(f"Error fetching prices: {e}")

    # 3. Group transactions by symbol
    grouped_txns = defaultdict(list)
    for txn in transactions:
        grouped_txns[txn.symbol].append(txn)

    sector_grouped_data = defaultdict(list)
    sector_totals = defaultdict(lambda: defaultdict(Decimal))
    grand_totals = defaultdict(lambda: Decimal('0.0'))
    
    TYPE_OPENING = {'Balance b/d'}
    TYPE_SIMPLE_PURCHASE = {'BUY', 'CONVERSION(+)', 'SUSPENSE(+)', 'RIGHT', 'IPO'}
    TYPE_PROPORTIONAL = {'BONUS'} 
    TYPE_SALES = {'SALE', 'CONVERSION(-)', 'SUSPENSE(-)'}
    TYPE_CASH = {'CASH'}

    # 4. Main Logic Loop
    for symbol_obj, txns in grouped_txns.items():
        row = defaultdict(Decimal)
        row.update({
            'company': symbol_obj.script_ticker,
            'company_name': symbol_obj.company_name,
            'sector': symbol_obj.sector,
        })

        global_kitta = 0
        global_cost = Decimal('0.0')
        
        # --- A. Calculate Opening Balance (all txns *before* start_date) ---
        for txn in txns:
            if txn.date < start_date:
                t_type = txn.transaction_type
                kitta = int(txn.kitta or 0)
                amount = txn.billed_amount if txn.billed_amount else Decimal('0.0')
                
                if t_type in TYPE_OPENING or t_type in TYPE_SIMPLE_PURCHASE or t_type in TYPE_PROPORTIONAL:
                    global_kitta += kitta
                    global_cost += amount
                elif t_type in TYPE_SALES:
                    wacc = (global_cost / Decimal(global_kitta)) if global_kitta > 0 else Decimal('0.0')
                    cons = (Decimal(kitta) * wacc).quantize(Decimal('0.01'), rounding=ROUND_HALF_UP)
                    global_kitta -= kitta
                    global_cost -= cons
                elif t_type in TYPE_CASH:
                    row['cash_dividend'] += amount

        row['op_kitta'] = global_kitta
        row['op_amt'] = global_cost
        row['op_rate'] = (row['op_amt'] / row['op_kitta']) if row['op_kitta'] > 0 else Decimal('0.0')

        period_total_cost = row['op_amt']
        period_total_qty = row['op_kitta']
        period_sales = [] 
        
        # --- B. Calculate Movements (txns *within* date range) ---
        for txn in txns:
            if txn.date >= start_date:
                t_type = txn.transaction_type
                kitta = int(txn.kitta or 0)
                amount = txn.billed_amount if txn.billed_amount else Decimal('0.0')
                rate = txn.eff_rate if txn.eff_rate else Decimal('0.0')

                if t_type in TYPE_OPENING:
                    row['op_kitta'] += kitta; row['op_amt'] += amount
                    period_total_qty += kitta; period_total_cost += amount
                
                elif t_type in TYPE_SIMPLE_PURCHASE:
                    row['buy_kitta'] += kitta; row['buy_amt'] += amount
                    period_total_qty += kitta; period_total_cost += amount
                
                elif t_type in TYPE_PROPORTIONAL:
                    row['bonus_kitta'] += kitta 
                    if amount > 0: row['bonus_amt'] += amount
                    period_total_qty += kitta; period_total_cost += amount
                
                elif t_type in TYPE_SALES:
                    row['sale_kitta'] += kitta; row['sale_amt'] += amount
                    period_sales.append({'kitta': kitta, 'amount': amount, 'rate': rate})
                
                elif t_type in TYPE_CASH:
                    row['cash_dividend'] += amount

        # --- C. Process Sales ---
        if period_total_qty > 0:
            period_wacc_rate = period_total_cost / Decimal(period_total_qty)
        else:
            period_wacc_rate = Decimal('0.0')

        for sale in period_sales:
            sell_qty = sale['kitta']
            cons = (Decimal(sell_qty) * period_wacc_rate).quantize(Decimal('0.01'), rounding=ROUND_HALF_UP)
            profit = sale['amount'] - cons
            
            row['consumption'] += cons
            row['realized_pl'] += profit
            
            period_total_qty -= sell_qty
            period_total_cost -= cons
            if period_total_qty > 0:
                period_wacc_rate = period_total_cost / Decimal(period_total_qty)
            else:
                period_wacc_rate = Decimal('0.0')

        # --- D. Calculate Closing Balance ---
        row['cl_kitta'] = period_total_qty
        row['cl_cost'] = period_total_cost if period_total_qty > 0 else Decimal('0.0')
        row['cl_rate'] = (row['cl_cost'] / row['cl_kitta']) if row['cl_kitta'] > 0 else Decimal('0.0')

        # --- E. Calculate Rates & Market Value ---
        row['buy_rate'] = (row['buy_amt'] / row['buy_kitta']) if row['buy_kitta'] > 0 else 0
        row['bonus_rate'] = (row['bonus_amt'] / row['bonus_kitta']) if row['bonus_kitta'] > 0 else 0
        row['sale_rate'] = (row['sale_amt'] / row['sale_kitta']) if row['sale_kitta'] > 0 else 0

        price_info = latest_prices.get(symbol_obj.script_ticker, {})
        ltp = price_info.get('close_price', Decimal('0.0'))
        row['ltp'] = ltp
        row['market_val'] = (Decimal(row['cl_kitta']) * ltp).quantize(Decimal('0.01'), rounding=ROUND_HALF_UP)
        row['unrealized_pl'] = row['market_val'] - row['cl_cost']
        row['total_pl'] = row['realized_pl'] + row['unrealized_pl']
        
        row['realized_pl_calc'] = row['realized_pl']
        row['total_pl_incl_div'] = row['total_pl'] + row['cash_dividend']

        # --- F. Add to totals ---
        if any([row['op_kitta'], row['buy_kitta'], row['bonus_kitta'], row['sale_kitta'], row['cl_kitta'], row['cash_dividend']]):
            sector_grouped_data[row['sector']].append(row)
            st = sector_totals[row['sector']]
            for key, val in row.items():
                if isinstance(val, Decimal):
                    st[key] += val
                elif isinstance(val, (int, float)):
                    st[key] += Decimal(str(val))
            
            for key, val in row.items():
                if isinstance(val, Decimal):
                    grand_totals[key] += val
                elif isinstance(val, (int, float)):
                    grand_totals[key] += Decimal(str(val))

    # --- 6. Format Final Data ---
    sorted_sectors = sorted(sector_grouped_data.keys())
    sn_counter = 1
    final_data = {}
    for sector in sorted_sectors:
        rows = sector_grouped_data[sector]
        rows.sort(key=lambda x: x['company']) 
        for r in rows: r['sn'] = sn_counter; sn_counter += 1
        final_data[sector] = {'rows': rows, 'totals': sector_totals[sector]}
        
    return final_data, grand_totals


# --- Existing Functions (Consolidated) ---

def get_holdings_on_date(symbol_obj, target_date):
    txns = Transaction.objects.filter(
        symbol=symbol_obj, 
        date__lt=target_date
    ).order_by('date', 'created_at')

    current_kitta = 0
    for txn in txns:
        kitta = int(txn.kitta or 0)
        if txn.transaction_type in ('BUY', 'BONUS', 'IPO', 'RIGHT', 'Balance b/d', 'CONVERSION(+)', 'SUSPENSE(+)'):
            current_kitta += kitta
        elif txn.transaction_type in ('SALE', 'CONVERSION(-)', 'SUSPENSE(-)'):
            current_kitta -= kitta
            
    return current_kitta

def calculate_pma_details(transactions, latest_price_info):
    detailed_calculations = []
    current_kitta = 0
    current_total_cost = Decimal('0.0')
    total_realized_pl = Decimal('0.0')
    total_cash_dividend = Decimal('0.0')
    total_purchase_amount = Decimal('0.0')
    total_sales_amount = Decimal('0.0')
    total_purchase_kitta = 0
    total_sales_kitta = 0
    is_first_row = True

    for txn in transactions:
        op_qty = current_kitta
        op_amount = current_total_cost
        op_rate = (op_amount / Decimal(op_qty)).quantize(Decimal('0.01'), rounding=ROUND_HALF_UP) if op_qty > 0 else Decimal('0.0')

        p_qty, p_rate, p_amount = 0, Decimal('0.0'), Decimal('0.0')
        s_qty, s_rate, s_amount = 0, Decimal('0.0'), Decimal('0.0')
        consumption = Decimal('0.0')
        profit = Decimal('0.0')

        txn_type = txn['transaction_type']
        kitta = int(txn.get('kitta') or 0) 
        billed_amount_dec = txn.get('billed_amount') or Decimal('0.0') 
        txn_eff_rate = txn.get('eff_rate') or Decimal('0.0') 

        if txn_type == 'Balance b/d' and is_first_row:
            op_qty, op_amount, op_rate = kitta, billed_amount_dec, txn_eff_rate
            p_qty, p_rate, p_amount = kitta, txn_eff_rate, billed_amount_dec
            current_kitta, current_total_cost = kitta, billed_amount_dec
            total_purchase_amount += billed_amount_dec
            total_purchase_kitta += kitta
        else:
            if txn_type in ('BUY', 'IPO', 'RIGHT', 'CONVERSION(+)', 'SUSPENSE(+)', 'Balance b/d'):
                p_qty, p_rate, p_amount = kitta, txn_eff_rate, billed_amount_dec
                current_kitta += kitta
                current_total_cost += billed_amount_dec 
                total_purchase_kitta += kitta
                total_purchase_amount += billed_amount_dec

            elif txn_type == 'BONUS':
                p_qty, p_rate, p_amount = kitta, txn_eff_rate, billed_amount_dec
                current_kitta += kitta
                total_purchase_kitta += kitta
            
            elif txn_type in ('SALE', 'CONVERSION(-)', 'SUSPENSE(-)'):
                s_qty, s_rate, s_amount = kitta, txn_eff_rate, billed_amount_dec
                
                sell_kitta = min(kitta, current_kitta)
                
                if sell_kitta <= 0:
                    profit, consumption = Decimal('0.0'), Decimal('0.0')
                else:
                    precise_wacc = current_total_cost / Decimal(current_kitta)
                    consumption = (Decimal(sell_kitta) * precise_wacc).quantize(Decimal('0.01'), rounding=ROUND_HALF_UP)
                    profit = (billed_amount_dec - consumption).quantize(Decimal('0.01'), rounding=ROUND_HALF_UP)
                
                total_realized_pl += profit
                current_total_cost -= consumption
                current_kitta -= sell_kitta
                
                if current_kitta == 0: current_total_cost = Decimal('0.0')

                total_sales_amount += billed_amount_dec
                total_sales_kitta += kitta
            
            elif txn_type == 'CASH':
                profit = billed_amount_dec
                total_realized_pl += profit
                total_cash_dividend += profit 
                p_amount = billed_amount_dec 
                
        cl_qty = current_kitta
        cl_amount = current_total_cost
        cl_rate = (cl_amount / Decimal(cl_qty)).quantize(Decimal('0.01'), rounding=ROUND_HALF_UP) if cl_qty > 0 else Decimal('0.0')
        
        is_buy_type = txn_type in ('BUY', 'BONUS', 'IPO', 'RIGHT', 'CONVERSION(+)', 'Balance b/d', 'SUSPENSE(+)', 'CASH')
        is_sale_type = txn_type in ('SALE', 'CONVERSION(-)', 'SUSPENSE(-)')

        detailed_calculations.append({
            'unique_id': txn['unique_id'], 'date': txn['date'], 'broker': txn.get('broker'),
            'type': txn_type, 'p_qty': p_qty, 'p_rate': p_rate, 'p_amount': p_amount,
            's_qty': s_qty, 's_rate': s_rate, 's_amount': s_amount,
            'profit': profit, 'cl_qty': cl_qty, 'cl_rate': cl_rate, 'cl_amount': cl_amount,
            'op_qty': op_qty, 'op_rate': op_rate, 'op_amount': op_amount,
            'consumption': consumption,
            'is_buy': is_buy_type,
            'is_sale': is_sale_type,
        })
        is_first_row = False

    closing_balance = current_kitta
    closing_total_cost = current_total_cost if closing_balance > 0 else Decimal('0.0')
    closing_avg_rate = (closing_total_cost / Decimal(closing_balance)).quantize(Decimal('0.01'), rounding=ROUND_HALF_UP) if closing_balance > 0 else Decimal('0.0')

    if closing_balance > 0:
        bep_val = (closing_total_cost - total_realized_pl) / Decimal(closing_balance)
    else:
        bep_val = Decimal('0.0')

    total_purchase_rate = (total_purchase_amount / Decimal(total_purchase_kitta)).quantize(Decimal('0.01'), rounding=ROUND_HALF_UP) if total_purchase_kitta > 0 else Decimal('0.0')
    total_sales_rate = (total_sales_amount / Decimal(total_sales_kitta)).quantize(Decimal('0.01'), rounding=ROUND_HALF_UP) if total_sales_kitta > 0 else Decimal('0.0')

    summary_data = {
        'realized_pl': total_realized_pl.quantize(Decimal('0.01'), rounding=ROUND_HALF_UP),
        'cash_dividend': total_cash_dividend.quantize(Decimal('0.01'), rounding=ROUND_HALF_UP),
        'closing_qty': closing_balance,
        'closing_avg_rate': closing_avg_rate,
        'closing_total_cost': closing_total_cost,
        'bep': bep_val.quantize(Decimal('0.01'), rounding=ROUND_HALF_UP),
        'total_purchase': total_purchase_amount.quantize(Decimal('0.01'), rounding=ROUND_HALF_UP),
        'total_sales': total_sales_amount.quantize(Decimal('0.01'), rounding=ROUND_HALF_UP),
        'latest_close_price': latest_price_info.get('close_price'),
        'latest_price_date': latest_price_info.get('business_date'),
        'total_purchase_kitta': total_purchase_kitta,
        'total_purchase_rate': total_purchase_rate,
        'total_sales_kitta': total_sales_kitta,
        'total_sales_rate': total_sales_rate
    }
    
    return detailed_calculations, summary_data

def calculate_overall_portfolio(all_transactions, latest_prices):
    """
    Calculates the high-level stats for the entire portfolio.
    """
    holdings_summary_list = []
    overall_stats = {
        'book_value': Decimal('0.0'),
        'market_value': Decimal('0.0'),
        'realized_pl': Decimal('0.0'),
        'cash_dividend': Decimal('0.0')
    }
    
    grouped_txns = defaultdict(list)
    for txn in all_transactions:
        grouped_txns[txn['symbol_id']].append(txn)

    for symbol_id, txns in grouped_txns.items():
        current_kitta = 0
        current_total_cost = Decimal('0.0')
        total_realized_pl = Decimal('0.0')
        total_cash_dividend = Decimal('0.0')
        
        script_name = txns[0].get('script', symbol_id)
        sector_name = txns[0].get('sector', 'Unknown')

        for txn in txns:
            txn_type = txn['transaction_type']
            kitta = int(txn.get('kitta') or 0)
            billed_amount_dec = txn.get('billed_amount') or Decimal('0.0') 
            
            if txn_type in ('Balance b/d', 'BUY', 'IPO', 'RIGHT', 'CONVERSION(+)', 'BONUS', 'SUSPENSE(+)'):
                current_kitta += kitta
                if txn_type != 'BONUS':
                    current_total_cost += billed_amount_dec 
            
            elif txn_type in ('SALE', 'CONVERSION(-)', 'SUSPENSE(-)'):
                sell_kitta = min(kitta, current_kitta)
                
                if sell_kitta <= 0 or current_kitta <= 0:
                    cost_of_goods_sold = Decimal('0.0')
                    profit_loss = billed_amount_dec 
                else:
                    current_avg_rate = current_total_cost / Decimal(current_kitta)
                    cost_of_goods_sold = (Decimal(sell_kitta) * current_avg_rate).quantize(Decimal('0.01'), rounding=ROUND_HALF_UP)
                    profit_loss = (billed_amount_dec - cost_of_goods_sold).quantize(Decimal('0.01'), rounding=ROUND_HALF_UP)
                
                total_realized_pl += profit_loss
                current_total_cost -= cost_of_goods_sold
                current_kitta -= sell_kitta
                
                if current_kitta == 0:
                    current_total_cost = Decimal('0.0')

            elif txn_type == 'CASH':
                total_realized_pl += billed_amount_dec
                total_cash_dividend += billed_amount_dec
        
        overall_stats['realized_pl'] += total_realized_pl
        overall_stats['cash_dividend'] += total_cash_dividend
        
        if current_kitta > 0:
            book_value = current_total_cost.quantize(Decimal('0.01'), rounding=ROUND_HALF_UP)
            wacc = (current_total_cost / Decimal(current_kitta)).quantize(Decimal('0.01'), rounding=ROUND_HALF_UP)
            
            raw_bep = (current_total_cost - total_realized_pl) / Decimal(current_kitta)
            bep_rate = Decimal('0.00') if raw_bep < 0 else raw_bep.quantize(Decimal('0.01'), rounding=ROUND_HALF_UP)
            
            ltp = latest_prices.get(symbol_id, {}).get('close_price', Decimal('0.0'))
            market_value = (ltp * Decimal(current_kitta)).quantize(Decimal('0.01'), rounding=ROUND_HALF_UP)
            unrealized_pl = (market_value - book_value)
            
            holdings_summary_list.append({
                'symbol': symbol_id, 
                'script': script_name,
                'sector': sector_name,
                'closing_kitta': current_kitta,
                'book_value': book_value,
                'wacc': wacc,
                'bep': bep_rate,
                'ltp': ltp,
                'realized_pl': total_realized_pl,
                'unrealized_pl': unrealized_pl,
                'cash_dividend': total_cash_dividend
            })

            overall_stats['book_value'] += book_value
            overall_stats['market_value'] += market_value

    total_bv = overall_stats['book_value']
    for item in holdings_summary_list:
        if total_bv > 0:
            item['book_value_pct'] = (item['book_value'] / total_bv) * 100
        else:
            item['book_value_pct'] = Decimal('0.0')

    holdings_summary_list.sort(key=lambda x: x['book_value'], reverse=True)
    
    overall_stats['unrealized_pl'] = overall_stats['market_value'] - overall_stats['book_value']
    overall_stats['total_profit'] = overall_stats['realized_pl'] + overall_stats['unrealized_pl']
    
    return overall_stats, holdings_summary_list
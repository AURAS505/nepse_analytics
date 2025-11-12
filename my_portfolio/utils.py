# my_portfolio/utils.py
from collections import defaultdict
from decimal import Decimal, ROUND_HALF_UP

# This holds the logic from your Flask app's portfolio_dashboard and stock_details
#

def calculate_pma_details(transactions, latest_price_info):
    """
    Calculates the detailed PMA (Perpetual Moving Average) ledger and
    summary for a single stock's transactions.
    
    Args:
        transactions (list): A list of transaction dicts for ONE symbol.
        latest_price_info (dict): A dict with 'close_price' and 'business_date'.
    
    Returns:
        tuple: (detailed_calculations, summary_data)
    """
    
    detailed_calculations = []
    current_kitta = 0
    current_total_cost = Decimal('0.0')
    total_realized_pl = Decimal('0.0')
    total_purchase_amount = Decimal('0.0')
    total_sales_amount = Decimal('0.0')
    total_purchase_kitta = 0
    total_sales_kitta = 0
    is_first_row = True

    for txn in transactions:
        op_qty, op_rate, op_amount = 0, Decimal('0.0'), Decimal('0.0')
        p_qty, p_rate, p_amount = 0, Decimal('0.0'), Decimal('0.0')
        s_qty, s_rate, s_amount = 0, Decimal('0.0'), Decimal('0.0')
        consumption = Decimal('0.0')
        profit = Decimal('0.0')

        txn_type = txn['transaction_type']
        kitta = int(txn['kitta'])
        billed_amount_dec = txn.get('billed_amount') or Decimal('0.0')

        # Ensure rate is a Decimal
        txn_rate = txn.get('rate')
        if isinstance(txn_rate, Decimal):
            txn_rate = txn_rate.quantize(Decimal('0.01'), rounding=ROUND_HALF_UP)
        elif txn_rate is not None:
            txn_rate = Decimal(txn_rate).quantize(Decimal('0.01'), rounding=ROUND_HALF_UP)
        elif kitta > 0 and billed_amount_dec > 0:
            txn_rate = (billed_amount_dec / Decimal(kitta)).quantize(Decimal('0.01'), rounding=ROUND_HALF_UP)
        else:
            txn_rate = Decimal('0.0')

        if txn_type == 'Balance b/d' and is_first_row:
            op_qty, op_amount, op_rate = kitta, billed_amount_dec, txn_rate
            p_qty, p_rate, p_amount = kitta, txn_rate, billed_amount_dec
            current_kitta, current_total_cost = kitta, billed_amount_dec
            total_purchase_amount += billed_amount_dec
            total_purchase_kitta += kitta
        else:
            op_qty, op_amount = current_kitta, current_total_cost
            op_rate = (op_amount / Decimal(op_qty)).quantize(Decimal('0.01'), rounding=ROUND_HALF_UP) if op_qty > 0 else Decimal('0.0')

            if txn_type in ('BUY', 'BONUS', 'IPO', 'RIGHT', 'CONVERSION(+)', 'SUSPENSE(+)'):
                p_qty, p_rate, p_amount = kitta, txn_rate, billed_amount_dec
                current_kitta += kitta
                current_total_cost += billed_amount_dec
                total_purchase_kitta += kitta
                if txn_type != 'BONUS':
                    total_purchase_amount += billed_amount_dec
            elif txn_type in ('SALE', 'CONVERSION(-)', 'SUSPENSE(-)'):
                s_qty, s_rate, s_amount = kitta, txn_rate, billed_amount_dec
                current_avg_rate = op_rate
                sell_kitta = min(kitta, current_kitta)
                if sell_kitta <= 0:
                    profit, consumption = Decimal('0.0'), Decimal('0.0')
                else:
                    consumption = (Decimal(sell_kitta) * current_avg_rate).quantize(Decimal('0.01'), rounding=ROUND_HALF_UP)
                    profit = (billed_amount_dec - consumption).quantize(Decimal('0.01'), rounding=ROUND_HALF_UP)
                
                total_realized_pl += profit
                current_total_cost -= consumption
                current_kitta -= sell_kitta
                total_sales_amount += billed_amount_dec
                total_sales_kitta += kitta

        cl_qty = current_kitta
        cl_amount = current_total_cost
        cl_rate = (cl_amount / Decimal(cl_qty)).quantize(Decimal('0.01'), rounding=ROUND_HALF_UP) if cl_qty > 0 else Decimal('0.0')
        if cl_qty <= 0: cl_amount, cl_qty = Decimal('0.0'), 0
        
        # --- NEW: Define row type for template ---
        is_buy_type = txn_type in ('BUY', 'BONUS', 'IPO', 'RIGHT', 'CONVERSION(+)', 'Balance b/d', 'SUSPENSE(+)')
        is_sale_type = txn_type in ('SALE', 'CONVERSION(-)', 'SUSPENSE(-)')
        # --- END NEW ---

        detailed_calculations.append({
            'unique_id': txn['unique_id'], 'date': txn['date'], 'broker': txn.get('broker'),
            'type': txn_type, 'p_qty': p_qty, 'p_rate': p_rate, 'p_amount': p_amount,
            's_qty': s_qty, 's_rate': s_rate, 's_amount': s_amount,
            'profit': profit, 'cl_qty': cl_qty, 'cl_rate': cl_rate, 'cl_amount': cl_amount,
            'op_qty': op_qty, 'op_rate': op_rate, 'op_amount': op_amount,
            'consumption': consumption,
            # --- NEW: Add keys for the template ---
            'is_buy': is_buy_type,
            'is_sale': is_sale_type,
        })
        is_first_row = False

    # --- Final summary for single stock ---
    closing_balance = current_kitta
    closing_avg_rate = (current_total_cost / Decimal(closing_balance)).quantize(Decimal('0.01'), rounding=ROUND_HALF_UP) if closing_balance > 0 else Decimal('0.0')
    closing_total_cost = current_total_cost if closing_balance > 0 else Decimal('0.0')

    paid_purchase_kitta = 0
    for txn in transactions:
        if txn['transaction_type'] in ('Balance b/d', 'BUY', 'IPO', 'RIGHT', 'CONVERSION(+)') and (txn.get('billed_amount') or 0) > 0:
            paid_purchase_kitta += int(txn['kitta'])
    
    total_purchase_rate = (total_purchase_amount / Decimal(paid_purchase_kitta)).quantize(Decimal('0.01'), rounding=ROUND_HALF_UP) if paid_purchase_kitta > 0 else Decimal('0.0')
    total_sales_rate = (total_sales_amount / Decimal(total_sales_kitta)).quantize(Decimal('0.01'), rounding=ROUND_HALF_UP) if total_sales_kitta > 0 else Decimal('0.0')

    summary_data = {
        'realized_pl': total_realized_pl.quantize(Decimal('0.01'), rounding=ROUND_HALF_UP),
        'closing_qty': closing_balance,
        'closing_avg_rate': closing_avg_rate,
        'closing_total_cost': closing_total_cost,
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
    
    Args:
        all_transactions (list): A list of ALL transaction dicts.
        latest_prices (dict): A dict mapping symbols to their latest price info.
    
    Returns:
        tuple: (overall_stats, holdings_summary_list)
    """
    
    holdings_summary_list = []
    overall_stats = {
        'book_value': Decimal('0.0'),
        'market_value': Decimal('0.0'),
        'realized_pl': Decimal('0.0')
    }
    
    # Group transactions by symbol
    grouped_txns = defaultdict(list)
    for txn in all_transactions:
        # --- THIS IS THE FIX ---
        # Changed 'symbol_id' to 'symbol' to match your database query
        grouped_txns[txn['symbol']].append(txn)
        # --- END OF FIX ---

    # Iterate through each symbol to get its final state
    for symbol, txns in grouped_txns.items():
        current_kitta = 0
        current_total_cost = Decimal('0.0')
        total_realized_pl = Decimal('0.0')
        script_name = txns[0]['script']
        sector_name = txns[0]['sector']

        # Run PMA logic for this symbol
        for txn in txns:
            txn_type = txn['transaction_type']
            kitta = int(txn['kitta'])
            billed_amount_dec = txn.get('billed_amount') or Decimal('0.0')
            
            if txn_type in ('Balance b/d', 'BUY', 'IPO', 'RIGHT', 'CONVERSION(+)', 'BONUS', 'SUSPENSE(+)'):
                current_kitta += kitta
                current_total_cost += billed_amount_dec
            
            elif txn_type in ('SALE', 'CONVERSION(-)', 'SUSPENSE(-)'):
                current_avg_rate = Decimal('0.0')
                if current_kitta > 0:
                    current_avg_rate = current_total_cost / Decimal(current_kitta)
                
                sell_kitta = min(kitta, current_kitta)
                if sell_kitta <= 0:
                    cost_of_goods_sold = Decimal('0.0')
                    profit_loss = billed_amount_dec
                else:
                    cost_of_goods_sold = (Decimal(sell_kitta) * current_avg_rate).quantize(Decimal('0.01'), rounding=ROUND_HALF_UP)
                    profit_loss = (billed_amount_dec - cost_of_goods_sold).quantize(Decimal('0.01'), rounding=ROUND_HALF_UP)
                
                total_realized_pl += profit_loss
                current_total_cost -= cost_of_goods_sold
                current_kitta -= sell_kitta
        
        # Add to OVERALL stats
        overall_stats['realized_pl'] += total_realized_pl
        
        # If we still hold this stock, add to book/market value
        if current_kitta > 0:
            bep_rate = (current_total_cost / Decimal(current_kitta)).quantize(Decimal('0.01'), rounding=ROUND_HALF_UP)
            book_value = current_total_cost.quantize(Decimal('0.01'), rounding=ROUND_HALF_UP)
            
            ltp = latest_prices.get(symbol, {}).get('close_price', Decimal('0.0'))
            market_value = (ltp * Decimal(current_kitta)).quantize(Decimal('0.01'), rounding=ROUND_HALF_UP)
            unrealized_pl = (market_value - book_value)
            
            holdings_summary_list.append({
                'symbol': symbol,
                'script': script_name,
                'sector': sector_name,
                'closing_kitta': current_kitta,
                'book_value': book_value,
                'bep': bep_rate,
                'ltp': ltp,
                'realized_pl': total_realized_pl,
                'unrealized_pl': unrealized_pl
            })

            overall_stats['book_value'] += book_value
            overall_stats['market_value'] += market_value

    holdings_summary_list.sort(key=lambda x: x['symbol'])
    overall_stats['unrealized_pl'] = overall_stats['market_value'] - overall_stats['book_value']
    overall_stats['total_profit'] = overall_stats['realized_pl'] + overall_stats['unrealized_pl']
    
    return overall_stats, holdings_summary_list

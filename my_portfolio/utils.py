# my_portfolio/utils.py
from collections import defaultdict
from decimal import Decimal, ROUND_HALF_UP
from .models import Transaction # Import the Transaction model

# ### THIS IS THE FUNCTION THAT WAS MISSING ###
def get_holdings_on_date(symbol_obj, target_date):
    """
    Calculates the total kitta held for a specific symbol *before* a target date.
    This is used to determine holdings for dividend/right eligibility.
    """
    # Get all transactions for this symbol *before* the book closure date
    txns = Transaction.objects.filter(
        symbol=symbol_obj, 
        date__lt=target_date
    ).order_by('date', 'created_at')

    current_kitta = 0
    for txn in txns:
        kitta = int(txn.kitta or 0)
        
        # Add for purchases
        if txn.transaction_type in ('BUY', 'BONUS', 'IPO', 'RIGHT', 'Balance b/d', 'CONVERSION(+)', 'SUSPENSE(+)'):
            current_kitta += kitta
        # Subtract for sales
        elif txn.transaction_type in ('SALE', 'CONVERSION(-)', 'SUSPENSE(-)'):
            current_kitta -= kitta
        # CASH type does not affect kitta
            
    return current_kitta
# ### END NEW FUNCTION ###


def calculate_pma_details(transactions, latest_price_info):
    """
    Calculates the detailed PMA (Perpetual Moving Average) ledger and
    summary for a single stock's transactions.
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
        # Handle CASH type where kitta is None
        kitta = int(txn.get('kitta') or 0) 
        # billed_amount is the net cost/proceeds
        billed_amount_dec = txn.get('billed_amount') or Decimal('0.0') 
        # eff_rate is the (billed_amount / kitta)
        txn_eff_rate = txn.get('eff_rate') or Decimal('0.0') 

        if txn_type == 'Balance b/d' and is_first_row:
            op_qty, op_amount, op_rate = kitta, billed_amount_dec, txn_eff_rate
            p_qty, p_rate, p_amount = kitta, txn_eff_rate, billed_amount_dec
            current_kitta, current_total_cost = kitta, billed_amount_dec
            total_purchase_amount += billed_amount_dec
            total_purchase_kitta += kitta
        else:
            op_qty, op_amount = current_kitta, current_total_cost
            op_rate = (op_amount / Decimal(op_qty)).quantize(Decimal('0.01'), rounding=ROUND_HALF_UP) if op_qty > 0 else Decimal('0.0')

            if txn_type in ('BUY', 'BONUS', 'IPO', 'RIGHT', 'CONVERSION(+)', 'SUSPENSE(+)'):
                p_qty, p_rate, p_amount = kitta, txn_eff_rate, billed_amount_dec
                current_kitta += kitta
                current_total_cost += billed_amount_dec # Cost basis is the net billed amount
                total_purchase_kitta += kitta
                if txn_type != 'BONUS':
                    total_purchase_amount += billed_amount_dec
            
            elif txn_type in ('SALE', 'CONVERSION(-)', 'SUSPENSE(-)'):
                s_qty, s_rate, s_amount = kitta, txn_eff_rate, billed_amount_dec
                current_avg_rate = op_rate # This is the WACC
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
            
            elif txn_type == 'CASH':
                # Cash Dividend: Add to realized P/L, no effect on kitta or cost
                profit = billed_amount_dec
                total_realized_pl += profit
                # Set 'p_amount' so it appears in the ledger
                p_amount = billed_amount_dec 
                

        cl_qty = current_kitta
        cl_amount = current_total_cost
        cl_rate = (cl_amount / Decimal(cl_qty)).quantize(Decimal('0.01'), rounding=ROUND_HALF_UP) if cl_qty > 0 else Decimal('0.0')
        if cl_qty <= 0: cl_amount, cl_qty = Decimal('0.0'), 0
        
        # --- Define row type for template ---
        is_buy_type = txn_type in ('BUY', 'BONUS', 'IPO', 'RIGHT', 'CONVERSION(+)', 'Balance b/d', 'SUSPENSE(+)', 'CASH')
        is_sale_type = txn_type in ('SALE', 'CONVERSION(-)', 'SUSPENSE(-)')
        # --- END NEW ---

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

    # --- Final summary for single stock ---
    closing_balance = current_kitta
    closing_avg_rate = (current_total_cost / Decimal(closing_balance)).quantize(Decimal('0.01'), rounding=ROUND_HALF_UP) if closing_balance > 0 else Decimal('0.0')
    closing_total_cost = current_total_cost if closing_balance > 0 else Decimal('0.0')

    paid_purchase_kitta = 0
    for txn in transactions:
        if txn['transaction_type'] in ('Balance b/d', 'BUY', 'IPO', 'RIGHT', 'CONVERSION(+)') and (txn.get('billed_amount') or 0) > 0:
            paid_purchase_kitta += int(txn.get('kitta') or 0)
    
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


# ### MODIFIED FUNCTION: calculate_overall_portfolio ###
def calculate_overall_portfolio(all_transactions, latest_prices):
    """
    Calculates the high-level stats for the entire portfolio.
    """
    
    holdings_summary_list = []
    overall_stats = {
        'book_value': Decimal('0.0'),
        'market_value': Decimal('0.0'),
        'realized_pl': Decimal('0.0'),
        'cash_dividend': Decimal('0.0') # <-- ADD THIS
    }
    
    # Group transactions by symbol
    grouped_txns = defaultdict(list)
    for txn in all_transactions:
        # This uses the 'symbol_id' key from dictfetchall
        grouped_txns[txn['symbol_id']].append(txn)

    # Iterate through each symbol to get its final state
    for symbol_id, txns in grouped_txns.items():
        current_kitta = 0
        current_total_cost = Decimal('0.0')
        total_realized_pl = Decimal('0.0')
        total_cash_dividend = Decimal('0.0') # <-- ADD THIS
        script_name = txns[0]['script']
        sector_name = txns[0]['sector']

        # Run PMA logic for this symbol
        for txn in txns:
            txn_type = txn['transaction_type']
            kitta = int(txn.get('kitta') or 0)
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
            
            elif txn_type == 'CASH':
                # This is the change:
                total_realized_pl += billed_amount_dec  # Add to P/L
                total_cash_dividend += billed_amount_dec # Also track separately
        
        # Add to OVERALL stats
        overall_stats['realized_pl'] += total_realized_pl
        overall_stats['cash_dividend'] += total_cash_dividend # <-- ADD THIS
        
        # If we still hold this stock, add to book/market value
        if current_kitta > 0:
            bep_rate = (current_total_cost / Decimal(current_kitta)).quantize(Decimal('0.01'), rounding=ROUND_HALF_UP)
            book_value = current_total_cost.quantize(Decimal('0.01'), rounding=ROUND_HALF_UP)
            
            ltp = latest_prices.get(symbol_id, {}).get('close_price', Decimal('0.0'))
            market_value = (ltp * Decimal(current_kitta)).quantize(Decimal('0.01'), rounding=ROUND_HALF_UP)
            unrealized_pl = (market_value - book_value)
            
            holdings_summary_list.append({
                'symbol': symbol_id, 
                'script': script_name,
                'sector': sector_name,
                'closing_kitta': current_kitta,
                'book_value': book_value,
                'bep': bep_rate,
                'ltp': ltp,
                'realized_pl': total_realized_pl,
                'unrealized_pl': unrealized_pl,
                'cash_dividend': total_cash_dividend # <-- ADD THIS
            })

            overall_stats['book_value'] += book_value
            overall_stats['market_value'] += market_value

    holdings_summary_list.sort(key=lambda x: x['symbol'])
    overall_stats['unrealized_pl'] = overall_stats['market_value'] - overall_stats['book_value']
    overall_stats['total_profit'] = overall_stats['realized_pl'] + overall_stats['unrealized_pl']
    
    return overall_stats, holdings_summary_list
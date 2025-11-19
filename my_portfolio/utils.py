# my_portfolio/utils.py
from collections import defaultdict
from decimal import Decimal, ROUND_HALF_UP
from .models import Transaction 

# ... (keep get_holdings_on_date and calculate_pma_details as they were) ...

def get_holdings_on_date(symbol_obj, target_date):
    """
    Calculates the total kitta held for a specific symbol *before* a target date.
    """
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
    # ... (Previous code for calculate_pma_details remains exactly the same) ...
    """
    Calculates the detailed PMA ledger and summary for a single stock.
    """
    
    detailed_calculations = []
    current_kitta = 0
    current_total_cost = Decimal('0.0')
    total_realized_pl = Decimal('0.0')
    total_cash_dividend = Decimal('0.0') # Track separately for display
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
            op_qty, op_amount = current_kitta, current_total_cost
            op_rate = (op_amount / Decimal(op_qty)).quantize(Decimal('0.01'), rounding=ROUND_HALF_UP) if op_qty > 0 else Decimal('0.0')

            if txn_type in ('BUY', 'BONUS', 'IPO', 'RIGHT', 'CONVERSION(+)', 'SUSPENSE(+)'):
                p_qty, p_rate, p_amount = kitta, txn_eff_rate, billed_amount_dec
                current_kitta += kitta
                current_total_cost += billed_amount_dec 
                total_purchase_kitta += kitta
                if txn_type != 'BONUS':
                    total_purchase_amount += billed_amount_dec
            
            elif txn_type in ('SALE', 'CONVERSION(-)', 'SUSPENSE(-)'):
                s_qty, s_rate, s_amount = kitta, txn_eff_rate, billed_amount_dec
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
            
            elif txn_type == 'CASH':
                profit = billed_amount_dec
                total_realized_pl += profit
                total_cash_dividend += profit 
                p_amount = billed_amount_dec 
                

        cl_qty = current_kitta
        cl_amount = current_total_cost
        cl_rate = (cl_amount / Decimal(cl_qty)).quantize(Decimal('0.01'), rounding=ROUND_HALF_UP) if cl_qty > 0 else Decimal('0.0')
        if cl_qty <= 0: cl_amount, cl_qty = Decimal('0.0'), 0
        
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
    closing_avg_rate = (current_total_cost / Decimal(closing_balance)).quantize(Decimal('0.01'), rounding=ROUND_HALF_UP) if closing_balance > 0 else Decimal('0.0')
    closing_total_cost = current_total_cost if closing_balance > 0 else Decimal('0.0')

    if closing_balance > 0:
        # BEP = (Book Value - Realized Profit) / Remaining Qty
        bep_val = (closing_total_cost - total_realized_pl) / Decimal(closing_balance)
    else:
        bep_val = Decimal('0.0')

    paid_purchase_kitta = 0
    for txn in transactions:
        if txn['transaction_type'] in ('Balance b/d', 'BUY', 'IPO', 'RIGHT', 'CONVERSION(+)') and (txn.get('billed_amount') or 0) > 0:
            paid_purchase_kitta += int(txn.get('kitta') or 0)
    
    total_purchase_rate = (total_purchase_amount / Decimal(paid_purchase_kitta)).quantize(Decimal('0.01'), rounding=ROUND_HALF_UP) if paid_purchase_kitta > 0 else Decimal('0.0')
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

    # Iterate through each symbol to get its final state
    for symbol_id, txns in grouped_txns.items():
        current_kitta = 0
        current_total_cost = Decimal('0.0')
        total_realized_pl = Decimal('0.0')
        total_cash_dividend = Decimal('0.0')
        script_name = txns[0]['script']
        sector_name = txns[0]['sector']

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
                total_realized_pl += billed_amount_dec
                total_cash_dividend += billed_amount_dec
        
        overall_stats['realized_pl'] += total_realized_pl
        overall_stats['cash_dividend'] += total_cash_dividend
        
        if current_kitta > 0:
            # Book Value (Total Cost)
            book_value = current_total_cost.quantize(Decimal('0.01'), rounding=ROUND_HALF_UP)
            
            # Cost Price (WACC)
            wacc = (current_total_cost / Decimal(current_kitta)).quantize(Decimal('0.01'), rounding=ROUND_HALF_UP)
            
            # BEP Calculation: (Book Value - Realized Profit) / Kitta
            raw_bep = (current_total_cost - total_realized_pl) / Decimal(current_kitta)
            if raw_bep < 0:
                bep_rate = Decimal('0.00')
            else:
                bep_rate = raw_bep.quantize(Decimal('0.01'), rounding=ROUND_HALF_UP)
            
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
                # 'book_value_pct' will be added below
            })

            overall_stats['book_value'] += book_value
            overall_stats['market_value'] += market_value

    # --- ADDED: Calculate Percentage of Portfolio ---
    total_bv = overall_stats['book_value']
    for item in holdings_summary_list:
        if total_bv > 0:
            item['book_value_pct'] = (item['book_value'] / total_bv) * 100
        else:
            item['book_value_pct'] = Decimal('0.0')

    # --- Default Sort: Amount (Book Value) Descending ---
    holdings_summary_list.sort(key=lambda x: x['book_value'], reverse=True)
    
    overall_stats['unrealized_pl'] = overall_stats['market_value'] - overall_stats['book_value']
    overall_stats['total_profit'] = overall_stats['realized_pl'] + overall_stats['unrealized_pl']
    
    return overall_stats, holdings_summary_list
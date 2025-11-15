# adjustments_stock_price/tasks.py
import threading
import uuid
from decimal import Decimal
from datetime import date
from celery import shared_task
from django.db import connection, transaction

from listed_companies.models import Companies
from nepse_data.models import StockPrices
from .models import PriceAdjustments, StockPricesAdj


def rebuild_adjusted_prices(symbol):
    """
    Recalculates the entire adjusted price history for a given symbol.
    Uses Nepali stock market adjustment formulas:
    - Bonus: Factor = 1 / (1 + R)
    - Right: Factor = (P + Par*R) / (P * (1 + R))
    - Cash: Factor = (Price - Dividend) / Price [Based on par value]
    
    Only applies adjustments where book close date has already passed.
    """
    # Get today's date FIRST
    today = date.today()
    
    try:
        with transaction.atomic():
            # 1. Delete old adjusted data
            StockPricesAdj.objects.filter(symbol=symbol).delete()
            
            # 2. Copy raw data from stock_prices to stock_prices_adj
            copy_query = """
            INSERT INTO stock_prices_adj (
                id, business_date, security_id, symbol, security_name,
                open_price, high_price, low_price, close_price,
                open_price_adj, high_price_adj, low_price_adj, close_price_adj,
                average_traded_price_adj, adjustment_factor
            )
            SELECT
                id, business_date, NULLIF(security_id, ''), symbol, security_name,
                open_price, high_price, low_price, close_price,
                open_price, high_price, low_price, close_price,
                average_traded_price, 1.0
            FROM stock_prices
            WHERE symbol = %s
            """
            with connection.cursor() as cursor:
                cursor.execute(copy_query, [symbol])
                rowcount = cursor.rowcount
            
            if rowcount == 0:
                print(f"Warning: No raw price data for {symbol}.")

            # 3. Get all adjustments for this symbol (only those where book close date has passed)
            all_adjustments = PriceAdjustments.objects.filter(
                symbol__script_ticker=symbol
            ).order_by('book_close_date')
            
            # Filter to only include adjustments where book close date has passed
            adjustments = [adj for adj in all_adjustments if adj.book_close_date <= today]
            pending_adjustments = [adj for adj in all_adjustments if adj.book_close_date > today]
            
            print(f"Found {len(adjustments)} active adjustments for {symbol}.")
            if pending_adjustments:
                print(f"Skipping {len(pending_adjustments)} future adjustments (book close date not reached):")
                for pending in pending_adjustments:
                    print(f"  - {pending.adjustment_type.upper()} {pending.adjustment_percent}% on {pending.book_close_date} (pending)")
            
            if len(adjustments) == 0:
                print(f"No active adjustments to apply for {symbol}.")
                # Even if no active adjustments, we should still copy the raw prices
                # This ensures the stock_prices_adj table has data
                # (The initial copy at the beginning already did this, so we're good)

            # 4. Loop through each adjustment one by one (only active ones)
            for adj in adjustments:
                book_close_date = adj.book_close_date
                
                # Double-check that book close date has passed (safety check)
                if book_close_date > today:
                    print(f"SKIPPING: Book close date {book_close_date} hasn't passed yet (today is {today})")
                    continue
                
                adj_type = adj.adjustment_type
                adj_percent = adj.adjustment_percent
                par_value = adj.par_value or Decimal('100.0')
                
                print(f"\n--- Processing adjustment for {symbol} ---")
                print(f"    Type: '{adj_type}' (type: {type(adj_type).__name__})")
                print(f"    Percent: {adj_percent}")
                print(f"    Book Close Date: {book_close_date} (passed ✓)")
                print(f"    Par Value: {par_value}")
                
                R = adj_percent / Decimal('100.0')
                factor = Decimal('1.0')  # Default factor (does nothing)
                
                # Normalize the adjustment type (strip whitespace and convert to lowercase)
                adj_type_normalized = str(adj_type).strip().lower()
                print(f"    Normalized Type: '{adj_type_normalized}'")
                
                # --- Calculate the factor based on type ---
                if adj_type_normalized == 'bonus':
                    # Bonus Share Formula: Factor = 1 / (1 + R)
                    # Example: 10% bonus -> Factor = 1/1.10 = 0.909091
                    # This means for every 100 shares, you get 10 more, so price dilutes by factor
                    factor = Decimal('1.0') / (Decimal('1.0') + R)
                    print(f"BONUS {symbol}: {adj_percent}% -> Factor = {factor}")
                
                elif adj_type_normalized == 'right':
                    # Right Share Formula: Factor = (P + Par*R) / (P * (1 + R))
                    # Need to find the last adjusted price before BCD
                    last_price_obj = StockPricesAdj.objects.filter(
                        symbol=symbol, 
                        business_date__lt=book_close_date
                    ).order_by('-business_date').first()
                    
                    if last_price_obj and last_price_obj.close_price_adj is not None:
                        P_market_adj = last_price_obj.close_price_adj
                        if P_market_adj > 0:
                            # Factor = (P + Par*R) / (P * (1 + R))
                            numerator = P_market_adj + (par_value * R)
                            denominator = P_market_adj * (Decimal('1.0') + R)
                            factor = numerator / denominator
                            print(f"RIGHT {symbol}: {adj_percent}% at Par={par_value}, Price={P_market_adj} -> Factor = {factor}")
                        else:
                            print(f"Skipping RIGHT adj for {symbol}: Invalid price ({P_market_adj}) before {book_close_date}.")
                            factor = Decimal('1.0')
                    else:
                        print(f"Skipping RIGHT adj for {symbol}: No valid price found before {book_close_date}.")
                        factor = Decimal('1.0')

                elif adj_type_normalized == 'cash':
                    # Cash Dividend Formula (Nepali Market): 
                    # Dividend is paid as percentage of PAR VALUE, not market price
                    # Dividend Amount = Par Value × Dividend %
                    # Factor = (Price - Dividend) / Price = 1 - (Dividend / Price)
                    #
                    # Example: MMF1 with 11.75% cash dividend on Rs. 10 par value
                    # Dividend = 10 × 0.1175 = 1.175 NPR
                    # If previous price = 9.75, then:
                    # Adjusted Price = 9.75 - 1.175 = 8.575 ≈ 8.58 ✓
                    # Factor = 8.575 / 9.75 = 0.8795
                    
                    # Calculate the dividend amount in NPR
                    dividend_amount = par_value * R
                    
                    # Need to find the last adjusted price before BCD to calculate the factor
                    last_price_obj = StockPricesAdj.objects.filter(
                        symbol=symbol, 
                        business_date__lt=book_close_date
                    ).order_by('-business_date').first()
                    
                    if last_price_obj and last_price_obj.close_price_adj is not None:
                        P_market_adj = last_price_obj.close_price_adj
                        if P_market_adj > 0:
                            # Factor = (Price - Dividend) / Price
                            new_price = P_market_adj - dividend_amount
                            
                            # Safety check: adjusted price should be positive
                            if new_price <= 0:
                                print(f"WARNING: CASH dividend {dividend_amount} >= price {P_market_adj} for {symbol}. Setting factor to 0.01")
                                factor = Decimal('0.01')
                            else:
                                factor = new_price / P_market_adj
                            
                            print(f"CASH {symbol}: {adj_percent}% on Par={par_value}")
                            print(f"      Dividend Amount = {dividend_amount} NPR")
                            print(f"      Last Price = {P_market_adj}, New Price = {new_price}")
                            print(f"      Factor = {factor}")
                        else:
                            print(f"Skipping CASH adj for {symbol}: Invalid price ({P_market_adj}) before {book_close_date}.")
                            factor = Decimal('1.0')
                    else:
                        print(f"Skipping CASH adj for {symbol}: No valid price found before {book_close_date}.")
                        factor = Decimal('1.0')
                
                else:
                    print(f"WARNING: Unknown adjustment type '{adj_type}' (normalized: '{adj_type_normalized}') for {symbol}. Factor remains 1.0")
                    factor = Decimal('1.0')

                # --- 5. Apply the calculated factor to all rows before the BCD ---
                print(f"      About to apply factor {factor} to records before {book_close_date}")
                
                # First, let's check how many records exist before the BCD
                with connection.cursor() as cursor:
                    cursor.execute(
                        "SELECT COUNT(*) FROM stock_prices_adj WHERE symbol = %s AND business_date < %s",
                        [symbol, book_close_date]
                    )
                    count_before = cursor.fetchone()[0]
                    print(f"      Found {count_before} records before book close date")
                
                update_query = """
                UPDATE stock_prices_adj
                SET
                    open_price_adj = open_price_adj * %s,
                    high_price_adj = high_price_adj * %s,
                    low_price_adj = low_price_adj * %s,
                    close_price_adj = close_price_adj * %s,
                    average_traded_price_adj = average_traded_price_adj * %s,
                    adjustment_factor = adjustment_factor * %s
                WHERE
                    symbol = %s AND business_date < %s
                """
                with connection.cursor() as cursor:
                    params = (factor, factor, factor, factor, factor, factor, symbol, book_close_date)
                    cursor.execute(update_query, params)
                    rows_affected = cursor.rowcount
                    print(f"      UPDATE completed: {rows_affected} rows affected")
                
                # --- 6. Save the factor and count to the adjustment entry itself ---
                adj.records_adjusted = rows_affected
                adj.adjustment_factor = factor
                adj.save()
                
                print(f"Applied {adj_type.upper()} adjustment for {symbol}: {rows_affected} records updated with factor {factor}")
            
        return True

    except Exception as e:
        print(f"!!! --- ERROR during price rebuild for {symbol}: {e} --- !!!")
        import traceback
        traceback.print_exc()
        return False


def copy_unadjusted_prices(symbol):
    """
    Copies raw prices as-is for symbols with no adjustments.
    """
    try:
        with transaction.atomic():
            StockPricesAdj.objects.filter(symbol=symbol).delete()
            copy_query = """
            INSERT INTO stock_prices_adj (
                id, business_date, security_id, symbol, security_name,
                open_price, high_price, low_price, close_price,
                open_price_adj, high_price_adj, low_price_adj, close_price_adj,
                average_traded_price_adj, adjustment_factor
            )
            SELECT
                id, business_date, NULLIF(security_id, ''), symbol, security_name,
                open_price, high_price, low_price, close_price,
                open_price, high_price, low_price, close_price,
                average_traded_price, 1.0
            FROM stock_prices
            WHERE symbol = %s
            """
            with connection.cursor() as cursor:
                cursor.execute(copy_query, [symbol])
        return True
    except Exception as e:
        print(f"!!! --- ERROR during unadjusted price copy for {symbol}: {e} --- !!!")
        return False


@shared_task(bind=True)
def do_recalculation_work(self):
    """
    Background task for recalculating all adjusted prices.
    Updates progress via Celery state for real-time monitoring.
    """
    job_id = self.request.id
    try:
        all_symbols = set(StockPrices.objects.values_list('symbol', flat=True).distinct())
        adjusted_symbols = set(PriceAdjustments.objects.values_list('symbol__script_ticker', flat=True).distinct())
        unadjusted_symbols = all_symbols - adjusted_symbols
        
        total_symbols = len(all_symbols)
        if total_symbols == 0:
            return {"status": "complete", "progress": 0, "total": 0, "message": "No symbols found."}

        total_progress_steps = len(all_symbols)
        
        # Set initial state
        self.update_state(
            state='PROGRESS',
            meta={"progress": 0, "total": total_progress_steps, "message": "Starting job..."}
        )
        
        rebuild_failures = []
        current_progress = 0
        
        # --- STAGE 1: Process ADJUSTED symbols ---
        for i, symbol in enumerate(adjusted_symbols):
            current_progress += 1
            message = f"({current_progress}/{total_progress_steps}) Adjusting {symbol}..."
            
            self.update_state(
                state='PROGRESS',
                meta={"progress": current_progress, "total": total_progress_steps, "message": message}
            )
            
            if not rebuild_adjusted_prices(symbol):
                rebuild_failures.append(symbol)
                print(f"WARNING: Rebuild failed for {symbol}")
                
        # --- STAGE 2: Process UNADJUSTED symbols ---
        for i, symbol in enumerate(unadjusted_symbols):
            current_progress += 1
            message = f"({current_progress}/{total_progress_steps}) Copying {symbol}..."
            
            self.update_state(
                state='PROGRESS',
                meta={"progress": current_progress, "total": total_progress_steps, "message": message}
            )

            if not copy_unadjusted_prices(symbol):
                rebuild_failures.append(symbol)
                print(f"WARNING: Fast-copy failed for {symbol}")

        # --- Job Complete ---
        if rebuild_failures:
            failed_list = ", ".join(rebuild_failures)
            # Don't raise an exception, just return a result with the failure info
            return {
                "progress": total_progress_steps, 
                "total": total_progress_steps, 
                "message": f"Completed with errors. Failed symbols: {failed_list}",
                "status": "completed_with_errors",
                "failed_symbols": rebuild_failures
            }
        
        return {
            "progress": total_progress_steps, 
            "total": total_progress_steps, 
            "message": f"Successfully recalculated all {total_symbols} symbols.",
            "status": "success"
        }

    except Exception as e:
        error_msg = f"Critical error: {str(e)}"
        print(f"!!! --- CRITICAL ERROR in background job {job_id}: {error_msg} --- !!!")
        # Don't re-raise, just update state and return
        self.update_state(
            state='FAILURE',
            meta={
                "progress": 0, 
                "total": 0, 
                "message": error_msg,
                "error": str(e)
            }
        )
        # Return the error info instead of raising
        return {
            "progress": 0,
            "total": 0,
            "message": error_msg,
            "status": "error"
        }
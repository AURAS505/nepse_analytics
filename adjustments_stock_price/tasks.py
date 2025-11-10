# adjustments_stock_price/tasks.py
import threading
import uuid
from decimal import Decimal
from celery import shared_task
from django.db import connection, transaction

from listed_companies.models import Companies
from nepse_data.models import StockPrices
from .models import PriceAdjustments, StockPricesAdj

# NOTE: The global job_statuses dictionary has been REMOVED.
# We will use Celery's built-in result backend (Redis) instead.


def rebuild_adjusted_prices(symbol):
    """
    Recalculates the entire adjusted price history for a given symbol.
    (This helper function is unchanged)
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
                rowcount = cursor.rowcount
            
            if rowcount == 0:
                print(f"Warning: No raw price data for {symbol}.")

            adjustments = PriceAdjustments.objects.filter(
                symbol__script_ticker=symbol
            ).order_by('book_close_date')
            
            # This print is for your Celery log
            print(f"Found {len(adjustments)} adjustments for {symbol}.")

            for adj in adjustments:
                book_close_date = adj.book_close_date
                adj_type = adj.adjustment_type
                adj_percent = adj.adjustment_percent
                par_value = adj.par_value or Decimal('100.0')
                
                R = adj_percent / Decimal('100.0')
                factor = Decimal('1.0')
                
                if adj_type == 'bonus':
                    factor = Decimal('1.0') / (Decimal('1.0') + R)
                
                elif adj_type == 'right':
                    last_price_obj = StockPricesAdj.objects.filter(
                        symbol=symbol, 
                        business_date__lt=book_close_date
                    ).order_by('-business_date').first()
                    
                    if last_price_obj and last_price_obj.close_price_adj is not None:
                        P_market_adj = last_price_obj.close_price_adj
                        if P_market_adj <= 0:
                            continue 
                        
                        P_adj = (P_market_adj + (par_value * R)) / (Decimal('1.0') + R)
                        factor = P_adj / P_market_adj
                    else:
                        continue 

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
                
                adj.records_adjusted = rows_affected
                adj.adjustment_factor = factor
                adj.save()
            
        return True

    except Exception as e:
        print(f"!!! --- ERROR during price rebuild for {symbol}: {e} --- !!!")
        return False


def copy_unadjusted_prices(symbol):
    """
    (This helper function is unchanged)
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


# --- THIS IS THE UPDATED TASK ---
@shared_task(bind=True)
def do_recalculation_work(self):
    """
    This is the new background task.
    It uses self.update_state to save progress to Redis.
    """
    job_id = self.request.id
    try:
        all_symbols = set(StockPrices.objects.values_list('symbol', flat=True).distinct())
        adjusted_symbols = set(PriceAdjustments.objects.values_list('symbol__script_ticker', flat=True).distinct())
        unadjusted_symbols = all_symbols - adjusted_symbols
        
        total_symbols = len(all_symbols)
        if total_symbols == 0:
            # Return a final message
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
            
            # Update the state for the frontend to read
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
            
            # Update the state
            self.update_state(
                state='PROGRESS',
                meta={"progress": current_progress, "total": total_progress_steps, "message": message}
            )

            if not copy_unadjusted_prices(symbol):
                rebuild_failures.append(symbol)
                print(f"WARNING: Fast-copy failed for {symbol}")

        # --- 4. Job is complete ---
        if rebuild_failures:
            failed_list = ", ".join(rebuild_failures)
            # Raise an exception to set the state to FAILURE
            raise Exception(f"Finished with errors. Failed: {failed_list}")
        
        # Return a final message, Celery sets state to SUCCESS
        return {
            "progress": total_progress_steps, 
            "total": total_progress_steps, 
            "message": f"Successfully recalculated all {total_symbols} symbols."
        }

    except Exception as e:
        print(f"!!! --- CRITICAL ERROR in background job {job_id}: {e} --- !!!")
        # Update state to FAILURE with the error message
        self.update_state(
            state='FAILURE',
            meta={"progress": 0, "total": 0, "message": str(e)}
        )
        # Re-raise the exception so Celery logs it as a failure
        raise e
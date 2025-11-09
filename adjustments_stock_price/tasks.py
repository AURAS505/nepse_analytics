# adjustments_stock_price/tasks.py
import threading
import uuid
from decimal import Decimal
from celery import shared_task
from django.db import connection, transaction

from listed_companies.models import Companies
from nepse_data.models import StockPrices
from .models import PriceAdjustments, StockPricesAdj

# --- Global dictionary to store job statuses ---
# In a real production app, you'd use Redis or Django's cache
# but a global dict is fine for porting the logic.
job_statuses = {}


def rebuild_adjusted_prices(symbol):
    """
    Recalculates the entire adjusted price history for a given symbol.
    This is the core logic ported from your Flask app.
    """
    try:
        with transaction.atomic():
            # 1. Delete old adjusted prices
            StockPricesAdj.objects.filter(symbol=symbol).delete()

            # 2. Copy raw prices to 'stock_prices_adj'
            # We use raw SQL for this bulk-insert for performance
            copy_query = """
            INSERT INTO stock_prices_adj (
                id, business_date, security_id, symbol, security_name,
                open_price, high_price, low_price, close_price,
                open_price_adj, high_price_adj, low_price_adj, close_price_adj,
                average_traded_price_adj, adjustment_factor
            )
            SELECT
                id, business_date, security_id, symbol, security_name,
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

            # 3. Get all adjustments
            adjustments = PriceAdjustments.objects.filter(
                symbol__script_ticker=symbol
            ).order_by('book_close_date')

            print(f"Found {len(adjustments)} adjustments for {symbol}.")

            # 4. Apply each adjustment
            for adj in adjustments:
                book_close_date = adj.book_close_date
                adj_type = adj.adjustment_type
                adj_percent = adj.adjustment_percent
                par_value = adj.par_value or Decimal('100.0')

                R = adj_percent / Decimal('100.0') # Ratio
                factor = Decimal('1.0')

                if adj_type == 'bonus':
                    factor = Decimal('1.0') / (Decimal('1.0') + R)

                elif adj_type == 'right':
                    # Get last price *before* book close
                    last_price_obj = StockPricesAdj.objects.filter(
                        symbol=symbol, 
                        business_date__lt=book_close_date
                    ).order_by('-business_date').first()

                    if last_price_obj and last_price_obj.close_price_adj is not None:
                        P_market_adj = last_price_obj.close_price_adj
                        if P_market_adj <= 0:
                            continue # Skip

                        P_adj = (P_market_adj + (par_value * R)) / (Decimal('1.0') + R)
                        factor = P_adj / P_market_adj
                    else:
                        continue # No prior price, skip this adjustment

                # 5. Apply the factor to all prices *before* the date
                # We use raw SQL for this bulk-update for performance
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

                # 6. Update the adjustment record
                adj.records_adjusted = rows_affected
                adj.adjustment_factor = factor
                adj.save()

        return True

    except Exception as e:
        print(f"!!! --- ERROR during price rebuild for {symbol}: {e} --- !!!")
        return False


def copy_unadjusted_prices(symbol):
    """
    For stocks WITH NO adjustments.
    Copies fresh, unadjusted data from stock_prices.
    """
    try:
        with transaction.atomic():
            # 1. Delete old adjusted prices
            StockPricesAdj.objects.filter(symbol=symbol).delete()

            # 2. Copy raw prices
            copy_query = """
            INSERT INTO stock_prices_adj (
                id, business_date, security_id, symbol, security_name,
                open_price, high_price, low_price, close_price,
                open_price_adj, high_price_adj, low_price_adj, close_price_adj,
                average_traded_price_adj, adjustment_factor
            )
            SELECT
                id, business_date, security_id, symbol, security_name,
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


@shared_task # This decorator turns it into a Celery task
def do_recalculation_work(job_id):
    """
    This is the new background task.
    It implements the 2-stage optimization.
    """
    try:
        # 1. Get all symbols
        all_symbols = set(StockPrices.objects.values_list('symbol', flat=True).distinct())
        adjusted_symbols = set(PriceAdjustments.objects.values_list('symbol__script_ticker', flat=True).distinct())
        unadjusted_symbols = all_symbols - adjusted_symbols

        total_symbols = len(all_symbols)
        if total_symbols == 0:
            job_statuses[job_id] = {"status": "complete", "progress": 0, "total": 0, "message": "No symbols found."}
            return

        total_progress_steps = len(all_symbols)
        job_statuses[job_id] = {"status": "running", "progress": 0, "total": total_progress_steps, "message": "Starting job..."}

        rebuild_failures = []
        current_progress = 0

        # --- STAGE 1: Process ADJUSTED symbols ---
        for i, symbol in enumerate(adjusted_symbols):
            current_progress += 1
            job_statuses[job_id]["message"] = f"({current_progress}/{total_progress_steps}) Stage 1/2: Adjusting {symbol}..."

            if not rebuild_adjusted_prices(symbol):
                rebuild_failures.append(symbol)
                print(f"WARNING: Rebuild failed for {symbol}")

            job_statuses[job_id]["progress"] = current_progress

        # --- STAGE 2: Process UNADJUSTED symbols ---
        for i, symbol in enumerate(unadjusted_symbols):
            current_progress += 1
            job_statuses[job_id]["message"] = f"({current_progress}/{total_progress_steps}) Stage 2/2: Copying {symbol}..."

            if not copy_unadjusted_prices(symbol):
                rebuild_failures.append(symbol)
                print(f"WARNING: Fast-copy failed for {symbol}")

            job_statuses[job_id]["progress"] = current_progress

        # 4. Job is complete
        if rebuild_failures:
            failed_list = ", ".join(rebuild_failures)
            job_statuses[job_id]["status"] = "error"
            job_statuses[job_id]["message"] = f"Finished with errors. Failed: {failed_list}"
        else:
            job_statuses[job_id]["status"] = "complete"
            job_statuses[job_id]["message"] = f"Successfully recalculated all {total_symbols} symbols."

    except Exception as e:
        print(f"!!! --- CRITICAL ERROR in background job {job_id}: {e} --- !!!")
        job_statuses[job_id] = {"status": "error", "message": f"Critical error: {e}"}
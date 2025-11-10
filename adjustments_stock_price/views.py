# adjustments_stock_price/views.py
import uuid
import pandas as pd
import io
import csv
from decimal import Decimal
from datetime import datetime

from django.shortcuts import render, redirect, get_object_or_404
from django.http import JsonResponse, HttpResponse
from django.views.decorators.http import require_POST
from django.contrib import messages
from django.core.paginator import Paginator
from django.db import connection
from celery.result import AsyncResult # <-- NEW IMPORT

from .models import PriceAdjustments, StockPricesAdj
from listed_companies.models import Companies
# We import the task itself, but not the old 'job_statuses' dict
from .tasks import do_recalculation_work, rebuild_adjusted_prices 


# A helper function to fetch raw SQL as a dictionary
def dictfetchall(cursor):
    "Return all rows from a cursor as a dict"
    columns = [col[0] for col in cursor.description]
    return [
        dict(zip(columns, row))
        for row in cursor.fetchall()
    ]


def adjustment_tool_view(request):
    """
    Main admin page.
    (This function is unchanged from before)
    """
    if request.method == 'POST':
        try:
            symbol = request.POST.get('symbol').upper().strip()
            adj_type = request.POST.get('adjustment_type')
            book_close_date = request.POST.get('book_close_date')
            adj_percent = Decimal(request.POST.get('adjustment_percent'))

            if not all([symbol, adj_type, book_close_date, adj_percent]):
                messages.error(request, "All fields are required.")
                return redirect('adjustments_stock_price:index')

            company = get_object_or_404(Companies, script_ticker=symbol)
            par_value_decimal = company.par_value or Decimal('100.00')

            PriceAdjustments.objects.create(
                symbol=company,
                adjustment_type=adj_type,
                book_close_date=book_close_date,
                adjustment_percent=adj_percent,
                par_value=par_value_decimal,
                adjustment_date=datetime.now()
            )
            
            if rebuild_adjusted_prices(symbol):
                messages.success(request, f"Adjustment for {symbol} added and prices recalculated!")
            else:
                messages.error(request, f"Adjustment for {symbol} saved, but recalculation failed.")

        except Companies.DoesNotExist:
            messages.error(request, f"Company with symbol {symbol} not found in Listed Companies.")
        except Exception as e:
            messages.error(request, f"An error occurred: {e}")
        
        return redirect('adjustments_stock_price:index')

    # --- GET Request Logic ---
    page = request.GET.get('page', 1)
    per_page = request.GET.get('per_page', '10')
    adj_list = PriceAdjustments.objects.all().order_by('-book_close_date')
    
    if per_page != 'All':
        paginator = Paginator(adj_list, per_page, allow_empty_first_page=True)
        adjustments = paginator.get_page(page)
    else:
        adjustments = adj_list
        paginator = None 
        
    context = {
        'adjustments': adjustments,
        'per_page': per_page,
        'page_obj': adjustments if per_page != 'All' else None,
        'paginator': paginator,
    }
    return render(request, 'adjustments_stock_price/index.html', context)

# --- (edit, delete, view, bulk, download functions are all unchanged) ---

def edit_adjustment_view(request, adj_id):
    adjustment = get_object_or_404(PriceAdjustments, id=adj_id)
    if request.method == 'POST':
        try:
            adj_type = request.POST.get('adjustment_type')
            book_close_date = request.POST.get('book_close_date')
            adj_percent = Decimal(request.POST.get('adjustment_percent'))
            par_value_decimal = adjustment.symbol.par_value or Decimal('100.00')
            
            adjustment.adjustment_type = adj_type
            adjustment.book_close_date = book_close_date
            adjustment.adjustment_percent = adj_percent
            adjustment.par_value = par_value_decimal
            adjustment.save()

            if rebuild_adjusted_prices(adjustment.symbol.script_ticker):
                messages.success(request, f"Adjustment for {adjustment.symbol.script_ticker} updated!")
            else:
                messages.error(request, f"Adjustment saved, but recalculation failed.")
            return redirect('adjustments_stock_price:index')
        except Exception as e:
            messages.error(request, f"Failed to update: {e}")
    context = {'adjustment': adjustment}
    return render(request, 'adjustments_stock_price/edit_adjustment.html', context)

@require_POST
def delete_adjustment_view(request, adj_id):
    adjustment = get_object_or_404(PriceAdjustments, id=adj_id)
    symbol = adjustment.symbol.script_ticker
    try:
        adjustment.delete()
        if rebuild_adjusted_prices(symbol):
            messages.success(request, f"Adjustment for {symbol} deleted.")
        else:
            messages.error(request, f"Adjustment deleted, but recalculation failed.")
    except Exception as e:
        messages.error(request, f"Error deleting: {e}")
    return redirect('adjustments_stock_price:index')

def view_adjustments_view(request, symbol):
    company = get_object_or_404(Companies, script_ticker=symbol)
    query = """
    SELECT
        p.average_traded_price,
        adj.id, adj.business_date, adj.security_id, adj.symbol, adj.security_name,
        adj.open_price, adj.high_price, adj.low_price, adj.close_price,
        adj.open_price_adj, adj.high_price_adj, adj.low_price_adj, adj.close_price_adj,
        adj.adjustment_factor,
        COALESCE(adj.average_traded_price_adj, p.average_traded_price) as average_traded_price_adj,
        MAX(adj.high_price_adj) OVER (
            ORDER BY adj.business_date ROWS BETWEEN 364 PRECEDING AND CURRENT ROW
        ) as fifty_two_week_high_adj,
        MIN(adj.low_price_adj) OVER (
            ORDER BY adj.business_date ROWS BETWEEN 364 PRECEDING AND CURRENT ROW
        ) as fifty_two_week_low_adj
    FROM stock_prices_adj adj
    JOIN stock_prices p ON adj.id = p.id
    WHERE adj.symbol = %s
    ORDER BY adj.business_date DESC
    """
    with connection.cursor() as cursor:
        cursor.execute(query, [symbol])
        prices = dictfetchall(cursor)
    if not prices:
        messages.warning(request, f"No price data found for {symbol}.")
    context = {'prices': prices, 'symbol': symbol, 'company_name': company.company_name}
    return render(request, 'adjustments_stock_price/view_adjustments.html', context)

def get_company_name_view(request, symbol):
    try:
        company = Companies.objects.get(script_ticker=symbol.upper())
        return JsonResponse({"company_name": company.company_name})
    except Companies.DoesNotExist:
        return JsonResponse({"company_name": "Company not found"}, status=404)
    except Exception as e:
        return JsonResponse({"company_name": f"Error: {e}"}, status=500)

@require_POST
def bulk_upload_adjustments_view(request):
    file = request.FILES.get('file')
    if not file:
        messages.error(request, "No file part in the request.")
        return redirect('adjustments_stock_price:index')
    try:
        if file.name.endswith('.csv'):
            df = pd.read_csv(file.stream, dtype={'symbol': str})
        else:
            df = pd.read_excel(file.stream, dtype={'symbol': str})

        df.columns = [col.strip().lower() for col in df.columns]
        successful_symbols, failed_rows = [], []
        for index, row in df.iterrows():
            try:
                symbol_str = str(row['symbol']).upper().strip()
                company = Companies.objects.get(script_ticker=symbol_str)
                PriceAdjustments.objects.create(
                    symbol=company,
                    adjustment_type=str(row['adjustment_type']).lower().strip(),
                    book_close_date=pd.to_datetime(row['book_close_date']).date(),
                    adjustment_percent=Decimal(str(row['adjustment_percent'])),
                    par_value=company.par_value or Decimal('100.00'),
                    adjustment_date=datetime.now()
                )
                successful_symbols.append(symbol_str)
            except Exception as e:
                failed_rows.append({"row": index + 2, "symbol": row.get('symbol', 'N/A'), "error": str(e)})
        
        symbols_to_rebuild = sorted(list(set(successful_symbols)))
        rebuild_failures = []
        if symbols_to_rebuild:
            for symbol in symbols_to_rebuild:
                if not rebuild_adjusted_prices(symbol):
                    rebuild_failures.append(symbol)
        
        if successful_symbols:
            messages.success(request, f"Processed {len(successful_symbols)} adjustments for {len(symbols_to_rebuild)} unique symbols.")
        if failed_rows:
            messages.error(request, f"Failed to process {len(failed_rows)} rows. Check file.")
        if rebuild_failures:
            messages.warning(request, f"Recalculation failed for: {', '.join(rebuild_failures)}")
    except Companies.DoesNotExist:
        messages.error(request, f"A symbol in your file does not exist in the Companies table.")
    except Exception as e:
        messages.error(request, f"An unexpected error occurred: {e}")
    return redirect('adjustments_stock_price:index')

def download_adjustment_sample_csv_view(request):
    response = HttpResponse(content_type='text/csv')
    response['Content-Disposition'] = 'attachment; filename="bulk_adjustments_sample.csv"'
    writer = csv.writer(response)
    writer.writerow(["symbol", "adjustment_type", "book_close_date", "adjustment_percent"])
    writer.writerow(["NICA", "bonus", "2025-10-15", "10"])
    writer.writerow(["SBL", "right", "2025-11-01", "20"])
    return response


# --- UPDATED Background Task API Views ---

@require_POST
def start_recalc_view(request):
    """ API endpoint to START the background recalculation job. """
    # Run the Celery task
    task = do_recalculation_work.delay()
    # Get the job ID from the task
    job_id = task.id
    # Return the job_id to the browser
    return JsonResponse({"status": "started", "job_id": job_id})


def recalc_status_view(request, job_id):
    """ API endpoint for the frontend to POLL for progress updates. """
    # Get the task result from Celery/Redis
    task_result = AsyncResult(job_id)
    
    status = task_result.status
    
    if status == 'SUCCESS':
        # The task finished successfully
        result = task_result.result # Get the return value from the task
        return JsonResponse({
            "status": "complete",
            "progress": result.get("progress", 0),
            "total": result.get("total", 0),
            "message": result.get("message", "Complete!")
        })
    elif status == 'FAILURE':
        # The task failed
        result = task_result.info # Get the exception info
        return JsonResponse({
            "status": "error",
            "progress": 0,
            "total": 0,
            "message": str(result) # Send the error message
        })
    elif status == 'PROGRESS':
        # The task is running
        result = task_result.info # Get the 'meta' dictionary
        return JsonResponse({
            "status": "running",
            "progress": result.get("progress", 0),
            "total": result.get("total", 0),
            "message": result.get("message", "Running...")
        })
    else:
        # PENDING or other state
        return JsonResponse({
            "status": "pending",
            "progress": 0,
            "total": 0,
            "message": "Job is queued..."
        })


@require_POST
def clear_job_view(request, job_id):
    """ API endpoint to clear a completed job from memory. """
    try:
        task_result = AsyncResult(job_id)
        task_result.forget() # Tell Celery/Redis to forget the result
        return JsonResponse({"status": "cleared"})
    except Exception as e:
        return JsonResponse({"status": "error", "message": str(e)}, status=500)
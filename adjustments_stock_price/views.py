# adjustments_stock_price/views.py
from django.http import JsonResponse

# Placeholder view
def adjustment_tool_view(request):
    return JsonResponse({"status": "not_implemented", "page": "Adjustment Tool Index"})

# Placeholder view
def start_recalc_view(request):
    return JsonResponse({"status": "not_implemented", "action": "start_recalc"})

# Placeholder view
def recalc_status_view(request, job_id):
    return JsonResponse({"status": "not_implemented", "job_id": job_id})

# Placeholder view
def clear_job_view(request, job_id):
    return JsonResponse({"status": "not_implemented", "action": "clear_job"})
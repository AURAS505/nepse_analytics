# core/views.py
from django.shortcuts import render

def home_view(request):
    # This is just like your Flask 'home' function.
    # It renders an HTML template.
    return render(request, 'core/home.html')
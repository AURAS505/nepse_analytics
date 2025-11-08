# core/urls.py
from django.urls import path
from . import views  # Import views from this app

urlpatterns = [
    # This matches the homepage ('') and calls the
    # 'home_view' function from core/views.py
    path('', views.home_view, name='home'),
]
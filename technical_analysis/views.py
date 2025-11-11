from django.shortcuts import render, get_object_or_404
from django.http import JsonResponse
from django.contrib.auth.decorators import login_required
from django.db.models import Count, Q
from datetime import datetime, timedelta

from .models import Signal, IndicatorValue, ChartPattern, SupportResistanceLevel
from .services.data_service import MarketDataService
from .services.indicator_service import IndicatorService
from adjustments_stock_price.models import StockPricesAdj


# Main Dashboard
def dashboard(request):
    """Technical Analysis Dashboard Homepage"""
    
    # Get summary statistics
    context = {
        'active_signals': Signal.objects.filter(is_active=True).count(),
        'bullish_stocks': Signal.objects.filter(
            is_active=True,
            signal_type__in=['BUY', 'BUY_WEAK']
        ).values('symbol').distinct().count(),
        'bearish_stocks': Signal.objects.filter(
            is_active=True,
            signal_type__in=['SELL', 'SELL_WEAK']
        ).values('symbol').distinct().count(),
        'patterns_detected': ChartPattern.objects.filter(
            is_completed=False,
            detected_date__gte=datetime.now().date() - timedelta(days=30)
        ).count(),
    }
    
    return render(request, 'technical_analysis/technical_dashboard.html', context)


# Chart Viewer
def chart_viewer(request):
    """Interactive chart viewer with all stocks"""
    symbols = MarketDataService.get_active_symbols()
    
    context = {
        'symbols': symbols,
        'default_symbol': symbols[0] if symbols else None,
    }
    
    return render(request, 'technical_analysis/chart_viewer.html', context)


def stock_chart(request, symbol):
    """Chart for specific stock with indicators"""
    
    # Get chart data
    end_date = datetime.now().date()
    start_date = end_date - timedelta(days=180)  # 6 months
    
    ohlcv_data = MarketDataService.get_ohlcv(symbol, start_date, end_date)
    
    # Get recent indicators
    recent_indicators = IndicatorValue.objects.filter(
        symbol=symbol,
        business_date__gte=start_date
    ).select_related('indicator_type').order_by('indicator_type', '-business_date')
    
    context = {
        'symbol': symbol,
        'ohlcv_data': ohlcv_data.to_dict('records') if not ohlcv_data.empty else [],
        'indicators': recent_indicators,
    }
    
    return render(request, 'technical_analysis/stock_chart.html', context)


# Stock Screener
def stock_screener(request):
    """Stock screener with technical filters"""
    
    context = {
        'title': 'Stock Screener',
    }
    
    return render(request, 'technical_analysis/stock_screener.html', context)


# Trading Signals
def trading_signals(request):
    """View all active trading signals"""
    
    signals = Signal.objects.filter(
        is_active=True
    ).select_related('strategy').order_by('-business_date', '-created_at')[:100]
    
    context = {
        'signals': signals,
        'buy_signals': signals.filter(signal_type__in=['BUY', 'BUY_WEAK']),
        'sell_signals': signals.filter(signal_type__in=['SELL', 'SELL_WEAK']),
    }
    
    return render(request, 'technical_analysis/trading_signals.html', context)


def stock_signals(request, symbol):
    """Signals for specific stock"""
    
    signals = Signal.objects.filter(
        symbol=symbol,
        is_active=True
    ).select_related('strategy').order_by('-business_date')
    
    context = {
        'symbol': symbol,
        'signals': signals,
    }
    
    return render(request, 'technical_analysis/stock_signals.html', context)


# Pattern Recognition
def pattern_recognition(request):
    """View detected chart patterns"""
    
    patterns = ChartPattern.objects.filter(
        is_completed=False
    ).order_by('-detected_date')[:100]
    
    # Group by pattern type
    pattern_summary = ChartPattern.objects.filter(
        is_completed=False
    ).values('pattern_type', 'sentiment').annotate(count=Count('id'))
    
    context = {
        'patterns': patterns,
        'pattern_summary': pattern_summary,
    }
    
    return render(request, 'technical_analysis/pattern_recognition.html', context)


def stock_patterns(request, symbol):
    """Patterns for specific stock"""
    
    patterns = ChartPattern.objects.filter(
        symbol=symbol
    ).order_by('-detected_date')
    
    context = {
        'symbol': symbol,
        'patterns': patterns,
    }
    
    return render(request, 'technical_analysis/stock_patterns.html', context)


# Support & Resistance
def support_resistance(request):
    """View support and resistance levels"""
    
    levels = SupportResistanceLevel.objects.filter(
        is_active=True
    ).order_by('symbol', '-strength')
    
    context = {
        'levels': levels,
    }
    
    return render(request, 'technical_analysis/support_resistance.html', context)


def sr_levels(request, symbol):
    """S/R levels for specific stock"""
    
    levels = SupportResistanceLevel.objects.filter(
        symbol=symbol,
        is_active=True
    ).order_by('-strength')
    
    latest_price = MarketDataService.get_latest_price(symbol)
    
    context = {
        'symbol': symbol,
        'levels': levels,
        'latest_price': latest_price,
    }
    
    return render(request, 'technical_analysis/sr_levels.html', context)


# Backtesting
def backtest_strategy(request):
    """Strategy backtesting interface"""
    
    context = {
        'title': 'Strategy Backtesting',
    }
    
    return render(request, 'technical_analysis/backtest.html', context)


# Indicator Detail Pages
def indicator_detail(request, indicator_name):
    """Detail page for specific indicator"""
    
    # Map indicator names to display info
    indicator_info = {
        'sma': {
            'name': 'Simple Moving Average',
            'category': 'Trend',
            'description': 'Average price over a specified period.',
        },
        'ema': {
            'name': 'Exponential Moving Average',
            'category': 'Trend',
            'description': 'Weighted average giving more importance to recent prices.',
        },
        'rsi': {
            'name': 'Relative Strength Index',
            'category': 'Momentum',
            'description': 'Measures speed and magnitude of price changes.',
        },
        'macd': {
            'name': 'MACD',
            'category': 'Trend/Momentum',
            'description': 'Shows relationship between two moving averages.',
        },
        'bollinger-bands': {
            'name': 'Bollinger Bands',
            'category': 'Volatility',
            'description': 'Measures market volatility using standard deviations.',
        },
        'atr': {
            'name': 'Average True Range',
            'category': 'Volatility',
            'description': 'Measures market volatility.',
        },
        'obv': {
            'name': 'On-Balance Volume',
            'category': 'Volume',
            'description': 'Relates volume to price change.',
        },
        # Add more indicators as needed
    }
    
    info = indicator_info.get(indicator_name, {
        'name': indicator_name.upper(),
        'category': 'Technical',
        'description': 'Technical indicator',
    })
    
    # Get stocks with this indicator calculated
    symbols = MarketDataService.get_active_symbols()[:20]  # Top 20 for now
    
    context = {
        'indicator_name': indicator_name,
        'indicator_info': info,
        'symbols': symbols,
    }
    
    return render(request, 'technical_analysis/indicator_detail.html', context)


def indicator_symbol(request, indicator_name, symbol):
    """Indicator values for specific stock"""
    
    end_date = datetime.now().date()
    start_date = end_date - timedelta(days=90)
    
    # Get indicator values
    values = IndicatorValue.objects.filter(
        symbol=symbol,
        indicator_type__name__icontains=indicator_name,
        business_date__gte=start_date
    ).order_by('business_date')
    
    context = {
        'indicator_name': indicator_name,
        'symbol': symbol,
        'values': values,
    }
    
    return render(request, 'technical_analysis/indicator_symbol.html', context)


# Advanced Features
def market_scanner(request):
    """Market-wide technical scanner"""
    
    context = {
        'title': 'Market Scanner',
    }
    
    return render(request, 'technical_analysis/market_scanner.html', context)


@login_required
def watchlist_manager(request):
    """Manage user watchlists"""
    
    from .models import Watchlist
    
    watchlists = Watchlist.objects.filter(user=request.user)
    
    context = {
        'watchlists': watchlists,
    }
    
    return render(request, 'technical_analysis/watchlist_manager.html', context)


@login_required
def alert_center(request):
    """Manage price and indicator alerts"""
    
    from .models import PriceAlert
    
    alerts = PriceAlert.objects.filter(
        user=request.user,
        is_active=True
    ).order_by('-created_at')
    
    context = {
        'alerts': alerts,
    }
    
    return render(request, 'technical_analysis/alert_center.html', context)


def sector_analysis(request):
    """Sector-wise technical analysis"""
    
    context = {
        'title': 'Sector Analysis',
    }
    
    return render(request, 'technical_analysis/sector_analysis.html', context)


@login_required
def strategy_builder(request):
    """Build custom trading strategies"""
    
    context = {
        'title': 'Strategy Builder',
    }
    
    return render(request, 'technical_analysis/strategy_builder.html', context)


@login_required
def performance_tracker(request):
    """Track trading performance"""
    
    context = {
        'title': 'Performance Tracker',
    }
    
    return render(request, 'technical_analysis/performance_tracker.html', context)


def tutorial(request):
    """Technical analysis tutorial"""
    
    context = {
        'title': 'Technical Analysis Tutorial',
    }
    
    return render(request, 'technical_analysis/tutorial.html', context)


# API Endpoints
def calculate_indicator_api(request):
    """API to calculate indicator on-demand"""
    
    symbol = request.GET.get('symbol')
    indicator = request.GET.get('indicator')
    period = int(request.GET.get('period', 14))
    
    if not symbol or not indicator:
        return JsonResponse({'error': 'Missing parameters'}, status=400)
    
    try:
        # Get data and calculate
        data = MarketDataService.get_ohlcv(symbol)
        result = IndicatorService.calculate(indicator, data, period=period)
        
        return JsonResponse({
            'success': True,
            'data': result
        })
    except Exception as e:
        return JsonResponse({'error': str(e)}, status=500)


def get_ohlcv_api(request, symbol):
    """API to get OHLCV data"""
    
    days = int(request.GET.get('days', 90))
    
    try:
        end_date = datetime.now().date()
        start_date = end_date - timedelta(days=days)
        
        data = MarketDataService.get_ohlcv(symbol, start_date, end_date)
        
        return JsonResponse({
            'success': True,
            'data': data.to_dict('records')
        })
    except Exception as e:
        return JsonResponse({'error': str(e)}, status=500)


def screener_results_api(request):
    """API for screener results"""
    
    # Get filter parameters from request
    rsi_min = request.GET.get('rsi_min')
    rsi_max = request.GET.get('rsi_max')
    price_above_ma = request.GET.get('price_above_ma')
    
    # TODO: Implement screener logic
    
    return JsonResponse({
        'success': True,
        'results': []
    })


def signals_data_api(request):
    """API for signals data"""
    
    signal_type = request.GET.get('type', 'all')
    
    query = Signal.objects.filter(is_active=True)
    
    if signal_type != 'all':
        query = query.filter(signal_type=signal_type)
    
    signals = list(query.values(
        'symbol', 'signal_type', 'strength', 'price_at_signal',
        'business_date', 'reason'
    )[:50])
    
    return JsonResponse({
        'success': True,
        'signals': signals
    })
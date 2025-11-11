from django.db.models import Q
from django.db import models
from adjustments_stock_price.models import StockPricesAdj
import pandas as pd
from datetime import datetime, timedelta

class MarketDataService:
    """Service to fetch data from existing StockPricesAdj model"""
    
    @staticmethod
    def get_ohlcv(symbol, start_date=None, end_date=None, use_adjusted=True):
        """
        Fetch OHLCV data for a symbol
        
        Args:
            symbol: Stock symbol
            start_date: Start date for data
            end_date: End date for data
            use_adjusted: Use adjusted prices or raw prices
        
        Returns:
            DataFrame with OHLCV data
        """
        queryset = StockPricesAdj.objects.filter(symbol=symbol)
        
        if start_date:
            queryset = queryset.filter(business_date__gte=start_date)
        if end_date:
            queryset = queryset.filter(business_date__lte=end_date)
        
        queryset = queryset.order_by('business_date')
        
        # Select adjusted or raw prices
        if use_adjusted:
            # THIS IS THE CORRECT IMPLEMENTATION USING .values()
            data = queryset.values(
                'business_date',
                open='open_price_adj',
                high='high_price_adj',
                low='low_price_adj',
                close='close_price_adj',
                volume='total_traded_quantity' # <-- Includes Volume
            )
        else:
            # THIS IS THE CORRECT IMPLEMENTATION USING .values()
            data = queryset.values(
                'business_date',
                open='open_price',
                high='high_price',
                low='low_price',
                close='close_price',
                volume='total_traded_quantity' # <-- Includes Volume
            )
        
        df = pd.DataFrame(list(data))
        
        if not df.empty:
            df['business_date'] = pd.to_datetime(df['business_date'])
            # Convert volume to numeric, coercing errors
            df['volume'] = pd.to_numeric(df['volume'], errors='coerce').fillna(0)
            df.set_index('business_date', inplace=True)
        
        return df
    
    @staticmethod
    def get_latest_price(symbol):
        """Get the latest price for a symbol"""
        try:
            latest = StockPricesAdj.objects.filter(
                symbol=symbol
            ).latest('business_date')
            return {
                'symbol': symbol,
                'date': latest.business_date,
                'close': float(latest.close_price_adj or latest.close_price),
                'open': float(latest.open_price_adj or latest.open_price),
                'high': float(latest.high_price_adj or latest.high_price),
                'low': float(latest.low_price_adj or latest.low_price),
            }
        except StockPricesAdj.DoesNotExist:
            return None
    
    @staticmethod
    def get_multiple_symbols(symbols, start_date=None, end_date=None):
        """Fetch data for multiple symbols"""
        queryset = StockPricesAdj.objects.filter(symbol__in=symbols)
        
        if start_date:
            queryset = queryset.filter(business_date__gte=start_date)
        if end_date:
            queryset = queryset.filter(business_date__lte=end_date)
        
        return queryset.order_by('symbol', 'business_date')
    
    @staticmethod
    def get_active_symbols():
        """Get list of all active symbols with recent data"""
        # Get symbols with data in last 30 days
        cutoff_date = datetime.now().date() - timedelta(days=30)
        
        symbols = StockPricesAdj.objects.filter(
            business_date__gte=cutoff_date
        ).values_list('symbol', flat=True).distinct()
        
        return list(symbols)
    
    @staticmethod
    def get_date_range(symbol):
        """Get available date range for a symbol"""
        data = StockPricesAdj.objects.filter(
            symbol=symbol
        ).aggregate(
            min_date=models.Min('business_date'),
            max_date=models.Max('business_date')
        )
        return data
"""
Utility functions for Nepali-English date conversion and fiscal year operations
"""
from datetime import datetime, timedelta
from typing import Tuple, Optional, Dict
from django.core.cache import cache


# Nepali Calendar Data (2070-2090 BS)
NEPALI_CALENDAR_DATA = {
    2070: [31, 31, 32, 31, 31, 31, 30, 29, 30, 29, 30, 30],
    2071: [31, 31, 32, 31, 32, 30, 30, 29, 30, 29, 30, 30],
    2072: [31, 32, 31, 32, 31, 30, 30, 30, 29, 29, 30, 31],
    2073: [31, 31, 31, 32, 31, 31, 29, 30, 30, 29, 30, 30],
    2074: [31, 31, 32, 31, 31, 31, 30, 29, 30, 29, 30, 30],
    2075: [31, 31, 32, 32, 31, 30, 30, 29, 30, 29, 30, 30],
    2076: [31, 32, 31, 32, 31, 30, 30, 30, 29, 29, 30, 31],
    2077: [31, 31, 31, 32, 31, 31, 30, 29, 30, 29, 30, 30],
    2078: [31, 31, 32, 31, 31, 31, 30, 29, 30, 29, 30, 30],
    2079: [31, 32, 31, 32, 31, 30, 30, 30, 29, 29, 30, 30],
    2080: [31, 32, 31, 32, 31, 30, 30, 30, 29, 30, 29, 31],
    2081: [31, 31, 31, 32, 31, 31, 29, 30, 30, 29, 30, 30],
    2082: [31, 31, 32, 31, 31, 31, 30, 29, 30, 29, 30, 30],
    2083: [31, 31, 32, 32, 31, 30, 30, 29, 30, 29, 30, 30],
    2084: [31, 32, 31, 32, 31, 30, 30, 30, 29, 29, 30, 31],
    2085: [31, 31, 31, 32, 31, 31, 29, 30, 30, 29, 30, 30],
    2086: [31, 31, 32, 31, 31, 31, 30, 29, 30, 29, 30, 30],
    2087: [31, 32, 31, 32, 31, 30, 30, 30, 29, 29, 30, 30],
    2088: [31, 32, 31, 32, 31, 30, 30, 30, 29, 30, 29, 31],
    2089: [31, 31, 31, 32, 31, 31, 30, 29, 30, 29, 30, 30],
    2090: [31, 31, 32, 31, 31, 31, 30, 29, 30, 29, 30, 30],
}

NEPALI_MONTHS = [
    'Baisakh', 'Jestha', 'Ashadh', 'Shrawan', 'Bhadra', 'Ashwin',
    'Kartik', 'Mangsir', 'Poush', 'Magh', 'Falgun', 'Chaitra'
]

# Base reference point: 2070/01/01 BS = 2013/04/13 AD
BASE_BS_DATE = {'year': 2070, 'month': 1, 'day': 1}
BASE_AD_DATE = datetime(2013, 4, 13)


def get_nepali_month_name(month: int) -> str:
    """Get Nepali month name from month number (1-12)"""
    if 1 <= month <= 12:
        return NEPALI_MONTHS[month - 1]
    raise ValueError(f"Invalid month: {month}")


def is_valid_nepali_date(year: int, month: int, day: int) -> bool:
    """Validate if a Nepali date is valid"""
    if year not in NEPALI_CALENDAR_DATA:
        return False
    if month < 1 or month > 12:
        return False
    if day < 1 or day > NEPALI_CALENDAR_DATA[year][month - 1]:
        return False
    return True


def count_days_from_base_bs(year: int, month: int, day: int) -> int:
    """Count total days from base BS date"""
    total_days = 0
    
    # Add days for complete years
    for y in range(BASE_BS_DATE['year'], year):
        if y in NEPALI_CALENDAR_DATA:
            total_days += sum(NEPALI_CALENDAR_DATA[y])
    
    # Add days for complete months in target year
    for m in range(0, month - 1):
        if year in NEPALI_CALENDAR_DATA:
            total_days += NEPALI_CALENDAR_DATA[year][m]
    
    # Add remaining days
    total_days += day - 1
    
    return total_days


def bs_to_ad(year: int, month: int, day: int) -> datetime:
    """
    Convert Bikram Sambat (BS) date to Anno Domini (AD) date
    
    Args:
        year: BS year
        month: BS month (1-12)
        day: BS day
    
    Returns:
        datetime object representing the AD date
    
    Raises:
        ValueError: If date is invalid or year not supported
    """
    # Check cache first
    cache_key = f"bs_to_ad_{year}_{month}_{day}"
    cached_result = cache.get(cache_key)
    if cached_result:
        return cached_result
    
    if not is_valid_nepali_date(year, month, day):
        raise ValueError(
            f"Invalid Nepali date: {year}/{month}/{day}. "
            f"Supported years: {min(NEPALI_CALENDAR_DATA.keys())}-{max(NEPALI_CALENDAR_DATA.keys())}"
        )
    
    days_diff = count_days_from_base_bs(year, month, day)
    result_date = BASE_AD_DATE + timedelta(days=days_diff)
    
    # Cache for 1 hour
    cache.set(cache_key, result_date, 3600)
    
    return result_date


def ad_to_bs(ad_date: datetime) -> Dict[str, int]:
    """
    Convert Anno Domini (AD) date to Bikram Sambat (BS) date
    
    Args:
        ad_date: datetime object or date object
    
    Returns:
        Dictionary with keys: year, month, day, month_name
    
    Raises:
        ValueError: If date is before base date or not supported
    """
    if isinstance(ad_date, str):
        ad_date = datetime.strptime(ad_date, '%Y-%m-%d')
    
    # Check cache first
    cache_key = f"ad_to_bs_{ad_date.strftime('%Y-%m-%d')}"
    cached_result = cache.get(cache_key)
    if cached_result:
        return cached_result
    
    if ad_date < BASE_AD_DATE:
        raise ValueError(f"Date must be on or after {BASE_AD_DATE.strftime('%Y-%m-%d')}")
    
    days_diff = (ad_date - BASE_AD_DATE).days
    
    current_year = BASE_BS_DATE['year']
    remaining_days = days_diff
    
    # Find the year
    while remaining_days >= 0 and current_year in NEPALI_CALENDAR_DATA:
        days_in_year = sum(NEPALI_CALENDAR_DATA[current_year])
        if remaining_days < days_in_year:
            break
        remaining_days -= days_in_year
        current_year += 1
    
    if current_year not in NEPALI_CALENDAR_DATA:
        raise ValueError(f"Year {current_year} not supported. Please extend NEPALI_CALENDAR_DATA.")
    
    # Find the month
    current_month = 0
    while remaining_days >= 0 and current_month < 12:
        days_in_month = NEPALI_CALENDAR_DATA[current_year][current_month]
        if remaining_days < days_in_month:
            break
        remaining_days -= days_in_month
        current_month += 1
    
    result = {
        'year': current_year,
        'month': current_month + 1,
        'day': remaining_days + 1,
        'month_name': NEPALI_MONTHS[current_month]
    }
    
    # Cache for 1 hour
    cache.set(cache_key, result, 3600)
    
    return result


def get_fiscal_year(date, format='string') -> str:
    """
    Get fiscal year for a given date
    Nepal fiscal year: Shrawan 1 to Ashad end (approximately July to July)
    
    Args:
        date: datetime object or BS dict with year, month, day
        format: 'string' returns "2080/81", 'dict' returns {'start_year': 2080, 'end_year': 2081}
    
    Returns:
        Fiscal year string or dict
    """
    if isinstance(date, dict) and 'year' in date and 'month' in date:
        # BS date provided
        bs_year = date['year']
        bs_month = date['month']
    else:
        # AD date provided, convert to BS
        bs_date = ad_to_bs(date)
        bs_year = bs_date['year']
        bs_month = bs_date['month']
    
    # Fiscal year starts from Shrawan (month 4)
    if bs_month >= 4:  # Shrawan to Chaitra
        start_year = bs_year
        end_year = bs_year + 1
    else:  # Baisakh to Ashadh
        start_year = bs_year - 1
        end_year = bs_year
    
    if format == 'dict':
        return {'start_year': start_year, 'end_year': end_year}
    else:
        return f"{start_year}/{str(end_year)[-2:]}"


def get_fiscal_year_dates(fiscal_year_string: str) -> Tuple[datetime, datetime]:
    """
    Get start and end dates for a fiscal year
    
    Args:
        fiscal_year_string: String like "2080/81"
    
    Returns:
        Tuple of (start_date, end_date) as datetime objects
    """
    start_year = int(fiscal_year_string.split('/')[0])
    end_year = start_year + 1
    
    # Fiscal year starts on Shrawan 1
    start_date = bs_to_ad(start_year, 4, 1)  # Shrawan 1
    
    # Fiscal year ends on last day of Ashadh
    ashadh_days = NEPALI_CALENDAR_DATA[end_year][2]  # Ashadh is 3rd month (index 2)
    end_date = bs_to_ad(end_year, 3, ashadh_days)  # Last day of Ashadh
    
    return start_date, end_date


def format_bs_date(year: int, month: int, day: int, format='full') -> str:
    """
    Format BS date in different styles
    
    Args:
        year, month, day: BS date components
        format: 'full', 'short', 'numeric'
    
    Returns:
        Formatted date string
    """
    month_name = get_nepali_month_name(month)
    
    if format == 'full':
        return f"{month_name} {day}, {year}"
    elif format == 'short':
        return f"{month_name[:3]} {day}, {year}"
    elif format == 'numeric':
        return f"{year}/{month:02d}/{day:02d}"
    else:
        return f"{year}/{month}/{day}"


def get_current_fiscal_year() -> str:
    """Get current fiscal year based on today's date"""
    from django.utils import timezone
    today = timezone.now()
    return get_fiscal_year(today)


def get_bs_date_from_ad(ad_date: datetime, format='dict'):
    """
    Convenience function to get BS date from AD date
    
    Args:
        ad_date: datetime object
        format: 'dict', 'string', or 'formatted'
    
    Returns:
        BS date in requested format
    """
    bs_date = ad_to_bs(ad_date)
    
    if format == 'dict':
        return bs_date
    elif format == 'string':
        return f"{bs_date['year']}/{bs_date['month']}/{bs_date['day']}"
    elif format == 'formatted':
        return format_bs_date(bs_date['year'], bs_date['month'], bs_date['day'])
    else:
        return bs_date
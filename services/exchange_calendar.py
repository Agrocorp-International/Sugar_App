"""Shared exchange-calendar helpers.

Single source of truth for ICE futures month codes, NYSE holidays, and
business-day arithmetic used by both sugar (SB/SW) and cotton (CT) info pages
and downstream consumers (options pricing, prices map, dashboard, tradestation).
"""
from datetime import date, datetime, timedelta
import holidays as _holidays_lib


# Rolling window: 1 year of history + 3 years forward.
YEARS_BACK = 1
YEARS_FORWARD = 3


# ICE month-letter → calendar month number.
FUTURES_MONTH_CODES = {
    "F": 1, "G": 2, "H": 3, "J": 4, "K": 5, "M": 6,
    "N": 7, "Q": 8, "U": 9, "V": 10, "X": 11, "Z": 12,
}


def nyse_holidays(years_back=YEARS_BACK, years_forward=YEARS_FORWARD):
    """Return [(name, 'YYYY-MM-DD')] for NYSE holidays in the rolling window."""
    current_year = date.today().year
    years = range(current_year - years_back, current_year + years_forward + 1)
    nyse = _holidays_lib.financial_holidays("NYSE", years=years)
    return sorted(
        [(name, d.strftime("%Y-%m-%d")) for d, name in nyse.items()],
        key=lambda x: x[1],
    )


RAW_HOLIDAYS = nyse_holidays()
HOLIDAY_DATES = frozenset(
    datetime.strptime(d, "%Y-%m-%d").date() for _, d in RAW_HOLIDAYS
)


def workday(start, offset, holidays):
    """Equivalent to Excel WORKDAY(start, offset, holidays)."""
    current = start
    steps = abs(offset)
    direction = timedelta(days=1 if offset > 0 else -1)
    while steps > 0:
        current += direction
        if current.weekday() < 5 and current not in holidays:
            steps -= 1
    return current


def prev_bday(d, holidays):
    """Roll a date backward to the nearest business day."""
    while d.weekday() >= 5 or d in holidays:
        d -= timedelta(days=1)
    return d


def last_biz_of_month(year, month, holidays):
    """Return the last business day of (year, month), skipping weekends and `holidays`."""
    if month == 12:
        first_of_next = date(year + 1, 1, 1)
    else:
        first_of_next = date(year, month + 1, 1)
    return workday(first_of_next, -1, holidays)

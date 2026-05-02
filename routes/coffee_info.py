"""ICE Coffee C (KC) contract expiry logic.

Formulas (ICE Coffee C product specifications):

  Futures Last Trading Day:
    "8 business days before the last business day of the delivery month."
    Implementation: workday(last_biz_of_month(year, month), -8, holidays)

  Options Last Trading Day:
    Last Friday of the month preceding the delivery month.
    No holiday adjustment per ICE spec.
"""
import re
from datetime import date, timedelta
from flask import Blueprint

from services.exchange_calendar import (
    FUTURES_MONTH_CODES,
    YEARS_BACK,
    YEARS_FORWARD,
    HOLIDAY_DATES,
    workday,
    last_biz_of_month,
)

coffee_info_bp = Blueprint("coffee_info", __name__)

# KC delivery months: Mar(H), May(K), Jul(N), Sep(U), Dec(Z).
KC_FUTURES_MONTHS = ["H", "K", "N", "U", "Z"]

_KC_FUTURES_RE = re.compile(r'^KC([HKNUZ])(\d{2})$')
_KC_OPTION_RE  = re.compile(r'^(KC[HKNUZ]\d{2})([CP])(\d+)$')


def compute_kc_futures_expiry(contract):
    """Return last trading day for a KC futures contract, or None if unrecognised."""
    m = _KC_FUTURES_RE.match(contract)
    if not m:
        return None
    month_code, yy = m.group(1), int(m.group(2))
    year = 2000 + yy
    month = FUTURES_MONTH_CODES.get(month_code)
    if not month:
        return None
    lbom = last_biz_of_month(year, month, HOLIDAY_DATES)
    return workday(lbom, -8, HOLIDAY_DATES)


def _last_friday(year, month):
    """Last Friday of (year, month)."""
    if month == 12:
        last_day = date(year + 1, 1, 1) - timedelta(days=1)
    else:
        last_day = date(year, month + 1, 1) - timedelta(days=1)
    offset = (last_day.weekday() - 4) % 7  # 4 = Friday
    return last_day - timedelta(days=offset)


def compute_kc_option_expiry(contract):
    """Return last trading day for a KC option contract.

    Options expire on the last Friday of the month preceding the delivery month.
    """
    m = _KC_OPTION_RE.match(contract)
    if not m:
        return None
    base = m.group(1)  # e.g. "KCH26"
    fut_m = _KC_FUTURES_RE.match(base)
    if not fut_m:
        return None
    month_code, yy = fut_m.group(1), int(fut_m.group(2))
    year = 2000 + yy
    month = FUTURES_MONTH_CODES.get(month_code)
    if not month:
        return None
    prev_month = month - 1 if month > 1 else 12
    prev_year  = year if month > 1 else year - 1
    return _last_friday(prev_year, prev_month)


def _build_kc_expiry_map():
    """Build KC_FUTURES_EXPIRY_MAP for a rolling window around today."""
    current_year = date.today().year
    result = {}
    for yr in range(current_year - YEARS_BACK, current_year + YEARS_FORWARD + 1):
        yy = yr % 100
        for m in KC_FUTURES_MONTHS:
            contract = f"KC{m}{yy:02d}"
            expiry = compute_kc_futures_expiry(contract)
            if expiry:
                result[contract] = expiry
    return result


# Generated at import; covers YEARS_BACK..YEARS_FORWARD rolling window.
KC_FUTURES_EXPIRY_MAP = _build_kc_expiry_map()

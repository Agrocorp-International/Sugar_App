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
from collections import defaultdict
from datetime import date, datetime, timedelta
from flask import Blueprint, render_template

from services.exchange_calendar import (
    FUTURES_MONTH_CODES,
    YEARS_BACK,
    YEARS_FORWARD,
    RAW_HOLIDAYS,
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


def _build_kc_futures_list():
    current_year = date.today().year
    result = []
    for yr in range(current_year - YEARS_BACK, current_year + YEARS_FORWARD + 1):
        yy = yr % 100
        for m in KC_FUTURES_MONTHS:
            contract = f"KC{m}{yy:02d}"
            month = FUTURES_MONTH_CODES.get(m)
            ref_date = date(yr, month, 1) if month else None
            expiry = KC_FUTURES_EXPIRY_MAP.get(contract)
            result.append({"contract": contract, "ref_date": ref_date, "expiry": expiry})
    return result


def _build_kc_options_list():
    current_year = date.today().year
    result = []
    for yr in range(current_year - YEARS_BACK, current_year + YEARS_FORWARD + 1):
        yy = yr % 100
        for m in KC_FUTURES_MONTHS:
            contract = f"KC{m}{yy:02d}"
            month = FUTURES_MONTH_CODES.get(m)
            if month:
                prev_month = month - 1 if month > 1 else 12
                prev_year = yr if month > 1 else yr - 1
                ref_date = date(prev_year, prev_month, 1)
            else:
                ref_date = None
            expiry = _last_friday(prev_year, prev_month) if ref_date else None
            result.append({
                "contract": contract,
                "underlying": contract,
                "ref_date": ref_date,
                "expiry": expiry,
            })
    return result


@coffee_info_bp.route("/info")
def index():
    today = date.today()

    holidays_list = sorted([
        {
            "name": name,
            "date": datetime.strptime(d, "%Y-%m-%d").date(),
            "day": datetime.strptime(d, "%Y-%m-%d").strftime("%A"),
        }
        for name, d in RAW_HOLIDAYS
    ], key=lambda x: x["date"])

    upcoming_date = next(
        (h["date"] for h in holidays_list if h["date"] >= today), None
    )

    grouped = defaultdict(list)
    for h in holidays_list:
        grouped[h["date"].year].append(h)

    return render_template(
        "coffee/info.html",
        futures=_build_kc_futures_list(),
        options=_build_kc_options_list(),
        grouped=dict(sorted(grouped.items())),
        upcoming_date=upcoming_date,
    )

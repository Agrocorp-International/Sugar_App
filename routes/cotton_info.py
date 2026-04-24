"""Cotton (CT) info page — reference tables for futures, options, and NYSE holidays.

Formulas (ICE Cotton #2 product specifications):

  Futures Last Trading Day:
    "Seventeen business days from end of spot month."
    Implementation: workday(last_biz_of_month, LTD_OFFSET, holidays)
    LTD_OFFSET = -16 (last_biz counted as biz day 1, so -16 = 17th biz day back).
    Verified: CTH26 LTD = Mon 09 Mar 2026, CTN25 LTD = Wed 09 Jul 2025,
              CTV26 LTD = Thu 08 Oct 2026.

  Options expiry (LISTED months H, K, N, V, Z only):
    Listed-month option expires on the SAME DATE as its underlying future's LTD.
    Verified: CTN25 option expired 09 Jul 2025; CTV26 option expires 08 Oct 2026.
    Implementation: lookup underlying contract in CT_FUTURES_EXPIRY_MAP.

  Serial options (F, G, J, M, Q, U, X) — NOT shown.
    These follow a different rule (typically the last Friday of the option month
    or similar). Pending verification before display.
"""
import logging
from datetime import datetime, date, timedelta
from collections import defaultdict
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

cotton_info_bp = Blueprint("cotton_info", __name__)


# ICE Cotton #2 (CT) listed contract months: March, May, July, October, December.
CT_FUTURES_MONTHS = ["H", "K", "N", "V", "Z"]

# Listed-month options only: each shares its expiry with the same-month future.
# Serial months (F, G, J, M, Q, U, X) deliberately omitted pending verification.
CT_OPTION_TO_UNDERLYING = {m: (m, 0) for m in CT_FUTURES_MONTHS}

LTD_OFFSET = -16   # last_biz counted as day 1, so -16 means 17th biz day from end


def _generate_ct_futures(years_back=YEARS_BACK, years_forward=YEARS_FORWARD):
    current_year = date.today().year
    contracts = []
    for yr in range(current_year - years_back, current_year + years_forward + 1):
        yy = yr % 100
        for m in CT_FUTURES_MONTHS:
            contracts.append(f"CT {m}{yy:02d}")
    return contracts


def _generate_ct_options(years_back=YEARS_BACK, years_forward=YEARS_FORWARD):
    current_year = date.today().year
    out = []
    for yr in range(current_year - years_back, current_year + years_forward + 1):
        yy = yr % 100
        for opt_code in sorted(CT_OPTION_TO_UNDERLYING.keys(),
                               key=lambda c: FUTURES_MONTH_CODES[c]):
            und_code, year_offset = CT_OPTION_TO_UNDERLYING[opt_code]
            und_yy = (yr + year_offset) % 100
            out.append((f"CT {opt_code}{yy:02d}", f"CT {und_code}{und_yy:02d}"))
    return out


_RAW_CT_FUTURES = _generate_ct_futures()
_RAW_CT_OPTIONS = _generate_ct_options()


def _parse_ct_futures(contracts):
    """CT futures Last Trading Day = workday(last_biz_of_month, LTD_OFFSET)."""
    result = []
    for c in contracts:
        parts = c.split()
        code = parts[1][0]
        year = 2000 + int(parts[1][1:])
        month = FUTURES_MONTH_CODES.get(code)
        ref_date = date(year, month, 1) if month else None
        if month:
            last_biz = last_biz_of_month(year, month, HOLIDAY_DATES)
            expiry = workday(last_biz, LTD_OFFSET, HOLIDAY_DATES)
        else:
            expiry = None
        result.append({"contract": c, "ref_date": ref_date, "expiry": expiry})
    return result


def _parse_ct_options(options, futures_expiry_map):
    """CT listed-month option expiry = underlying future's LTD."""
    result = []
    for contract, underlying in options:
        opt_parts = contract.split()
        opt_code = opt_parts[1][0]
        opt_year = 2000 + int(opt_parts[1][1:])
        opt_month = FUTURES_MONTH_CODES.get(opt_code)
        ref_date = date(opt_year, opt_month, 1) if opt_month else None
        expiry = futures_expiry_map.get(underlying.replace(" ", ""))
        result.append({"contract": contract, "underlying": underlying,
                       "ref_date": ref_date, "expiry": expiry})
    return result


# Module-level cached parses (mirrors sugar pattern).
PARSED_CT_FUTURES = _parse_ct_futures(_RAW_CT_FUTURES)

# Exported for cotton_prices.py auto-archive wiring (parallels sugar's
# FUTURES_EXPIRY_MAP in routes/prices.py:26).
CT_FUTURES_EXPIRY_MAP = {
    f["contract"].replace(" ", ""): f["expiry"] for f in PARSED_CT_FUTURES
}

# Options computed after the futures map is available (option expiry = its underlying's LTD).
PARSED_CT_OPTIONS = _parse_ct_options(_RAW_CT_OPTIONS, CT_FUTURES_EXPIRY_MAP)


def _assert_regression():
    """Loud-fail at module import if any pinned cotton expiry has drifted.
    See routes/_cotton_info_regression.py for the anchors."""
    from routes._cotton_info_regression import (
        GOLDEN_CT_FUTURES, GOLDEN_CT_OPTIONS,
    )

    fut_by_code = {f["contract"]: f for f in PARSED_CT_FUTURES}
    for code, expected_ref, expected_expiry in GOLDEN_CT_FUTURES:
        actual = fut_by_code.get(code)
        assert actual is not None, f"Regression: cotton futures {code} missing"
        assert actual["ref_date"] == expected_ref, (
            f"Regression: {code} ref_date drifted: "
            f"got {actual['ref_date']}, expected {expected_ref}"
        )
        assert actual["expiry"] == expected_expiry, (
            f"Regression: {code} expiry drifted: "
            f"got {actual['expiry']}, expected {expected_expiry}"
        )

    opt_by_code = {o["contract"]: o for o in PARSED_CT_OPTIONS}
    for code, exp_und, expected_ref, expected_expiry in GOLDEN_CT_OPTIONS:
        actual = opt_by_code.get(code)
        assert actual is not None, f"Regression: cotton option {code} missing"
        assert actual["underlying"] == exp_und, (
            f"Regression: {code} underlying drifted: "
            f"got {actual['underlying']}, expected {exp_und}"
        )
        assert actual["ref_date"] == expected_ref, (
            f"Regression: {code} option ref_date drifted: "
            f"got {actual['ref_date']}, expected {expected_ref}"
        )
        assert actual["expiry"] == expected_expiry, (
            f"Regression: {code} option expiry drifted: "
            f"got {actual['expiry']}, expected {expected_expiry}"
        )

_assert_regression()


_log = logging.getLogger(__name__)


@cotton_info_bp.route("/info")
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

    return render_template("cotton/info.html",
                           grouped=dict(sorted(grouped.items())),
                           upcoming_date=upcoming_date,
                           futures=PARSED_CT_FUTURES,
                           options=PARSED_CT_OPTIONS)

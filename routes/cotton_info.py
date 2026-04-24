"""Cotton (CT) info page — reference tables for futures, options, and NYSE holidays.

Formulas (ICE Cotton #2 product specifications):

  Futures Last Trading Day:
    "Seventeen business days from end of spot month."
    Implementation: workday(last_biz_of_month, LTD_OFFSET, holidays)
    LTD_OFFSET = -16 (last_biz counted as biz day 1, so -16 = 17th biz day back).
    Verified: CTH26 LTD = Mon 09 Mar 2026, CTN25 LTD = Wed 09 Jul 2025,
              CTV26 LTD = Thu 08 Oct 2026.

  Options expiry — LISTED months (H, K, N, V, Z):
    Listed-month option expires on the last Friday preceding the underlying
    future's first notice day by at least 5 business days.
    Implementation:
      1. Compute futures first notice day = 5 business days before the first
         business day of the delivery month.
      2. Step back 5 business days from first notice day.
      3. Take the last Friday on or before that date.
    Examples verified against ICE expiry pages:
      Jul26 option LTD = 12 Jun 2026, Oct26 option LTD = 11 Sep 2026.

  Options expiry — SERIAL months (F, U, X only):
    Serial option expires on the THIRD FRIDAY of the option's own month.
    Per ICE Cotton No. 2 Options spec. Only 3 serials are listed:
      F (Jan) → rolls into H (Mar) same year
      U (Sep) → rolls into Z (Dec) same year
      X (Nov) → rolls into Z (Dec) same year
    G, J, M, Q are NOT listed CT option contracts.
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
    third_friday,
)

cotton_info_bp = Blueprint("cotton_info", __name__)


# ICE Cotton #2 (CT) listed contract months: March, May, July, October, December.
CT_FUTURES_MONTHS = ["H", "K", "N", "V", "Z"]

# Listed-month options expire on the last Friday preceding the underlying
# future's first notice day by at least 5 business days.
# Serial options (F, U, X) expire on the 3rd Friday of the option month.
# ICE spec: F→H same year, U→Z same year, X→Z same year.
CT_OPTION_TO_UNDERLYING = {m: (m, 0) for m in CT_FUTURES_MONTHS}
CT_OPTION_TO_UNDERLYING.update({
    "F": ("H", 0),
    "U": ("Z", 0),
    "X": ("Z", 0),
})

CT_SERIAL_OPTION_MONTHS = frozenset({"F", "U", "X"})

# Guardrail: the full set of CT option month codes per ICE spec. G, J, M, Q are
# explicitly forbidden — they are NOT listed as CT option contracts.
CT_EXPECTED_OPTION_CODES = frozenset({"H", "K", "N", "V", "Z", "F", "U", "X"})
CT_FORBIDDEN_OPTION_CODES = frozenset({"G", "J", "M", "Q"})

LTD_OFFSET = -16   # last_biz counted as day 1, so -16 means 17th biz day from end


def _first_biz_of_month(year, month, holidays):
    """Return the first business day of (year, month)."""
    d = date(year, month, 1)
    while d.weekday() >= 5 or d in holidays:
        d += timedelta(days=1)
    return d


def _last_friday_on_or_before(d):
    """Return the most recent Friday on or before *d*."""
    return d - timedelta(days=(d.weekday() - 4) % 7)


def _ct_regular_option_expiry(underlying, holidays):
    """ICE CT regular option LTD.

    Rule from ICE Cotton No. 2 Options spec:
    "Last Friday preceding the first notice day for the underlying futures by
    at least 5 business days."
    """
    parts = underlying.split()
    code = parts[1][0]
    year = 2000 + int(parts[1][1:])
    month = FUTURES_MONTH_CODES[code]
    first_notice_day = workday(_first_biz_of_month(year, month, holidays), -5, holidays)
    threshold = workday(first_notice_day, -5, holidays)
    return _last_friday_on_or_before(threshold)


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
    """Listed-month options: last Friday preceding FND by at least 5 business days.
       Serial options (F, U, X): expiry = 3rd Friday of option's own month."""
    result = []
    for contract, underlying in options:
        opt_parts = contract.split()
        opt_code = opt_parts[1][0]
        opt_year = 2000 + int(opt_parts[1][1:])
        opt_month = FUTURES_MONTH_CODES.get(opt_code)
        if opt_month is None:
            raise ValueError(f"Unknown CT option month code: {opt_code!r} in {contract!r}")
        ref_date = date(opt_year, opt_month, 1)
        if opt_code in CT_SERIAL_OPTION_MONTHS:
            expiry = third_friday(opt_year, opt_month)
        else:
            expiry = _ct_regular_option_expiry(underlying, HOLIDAY_DATES)
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

# Options computed after the futures map is available.
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

    # Negative guard: CT option codes must exactly match ICE spec.
    # G, J, M, Q are NOT listed CT option contracts — protects against
    # re-introducing the old (incorrect) 7-serial list.
    actual_codes = {o["contract"].split()[1][0] for o in PARSED_CT_OPTIONS}
    assert actual_codes == CT_EXPECTED_OPTION_CODES, (
        f"Regression: CT option codes drifted from ICE spec. "
        f"Got {sorted(actual_codes)}, expected {sorted(CT_EXPECTED_OPTION_CODES)}. "
        f"Unexpected serials (G/J/M/Q are NOT listed CT options per ICE spec): "
        f"{sorted(actual_codes & CT_FORBIDDEN_OPTION_CODES)}"
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

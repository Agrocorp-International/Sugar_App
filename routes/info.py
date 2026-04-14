import logging
from datetime import datetime, date, timedelta
from collections import defaultdict
import holidays as _holidays_lib
from flask import Blueprint, render_template

info_bp = Blueprint("info", __name__)

# Rolling window: 1 year of history + 3 years forward.
_YEARS_BACK = 1
_YEARS_FORWARD = 3

_FUTURES_MONTH_CODES = {
    "F": 1, "G": 2, "H": 3, "J": 4, "K": 5, "M": 6,
    "N": 7, "Q": 8, "U": 9, "V": 10, "X": 11, "Z": 12,
}

# ICE Sugar #11 (SB) futures cycle: March (H), May (K), July (N), October (V).
_SB_FUTURES_MONTHS = ["H", "K", "N", "V"]
# ICE White Sugar #5 (SW) futures cycle: March (H), May (K), August (Q), October (V), December (Z).
_SW_FUTURES_MONTHS = ["H", "K", "Q", "V", "Z"]

# Sugar option month → underlying future month (deterministic roll pattern).
# F/G/H roll into H, J/K roll into K, M/N roll into N, Q/U/V roll into V,
# X/Z roll into NEXT YEAR's H.
_OPTION_TO_UNDERLYING = {
    "F": ("H", 0), "G": ("H", 0), "H": ("H", 0),
    "J": ("K", 0), "K": ("K", 0),
    "M": ("N", 0), "N": ("N", 0),
    "Q": ("V", 0), "U": ("V", 0), "V": ("V", 0),
    "X": ("H", 1), "Z": ("H", 1),
}


def _generate_futures(prefix, months, years_back=_YEARS_BACK, years_forward=_YEARS_FORWARD):
    """Generate a futures contract list for a rolling window around today."""
    current_year = date.today().year
    contracts = []
    for yr in range(current_year - years_back, current_year + years_forward + 1):
        yy = yr % 100
        for m in months:
            contracts.append(f"{prefix} {m}{yy:02d}")
    return contracts


def _generate_options(years_back=_YEARS_BACK, years_forward=_YEARS_FORWARD):
    """Generate the (option, underlying) tuples for the SB options series."""
    current_year = date.today().year
    out = []
    for yr in range(current_year - years_back, current_year + years_forward + 1):
        yy = yr % 100
        for opt_code in sorted(_OPTION_TO_UNDERLYING.keys(),
                               key=lambda c: _FUTURES_MONTH_CODES[c]):
            und_code, year_offset = _OPTION_TO_UNDERLYING[opt_code]
            und_yy = (yr + year_offset) % 100
            out.append((f"SB {opt_code}{yy:02d}", f"SB {und_code}{und_yy:02d}"))
    return out


def _generate_holidays(years_back=_YEARS_BACK, years_forward=_YEARS_FORWARD):
    """Return [(name, 'YYYY-MM-DD')] for NYSE holidays in the rolling window."""
    current_year = date.today().year
    years = range(current_year - years_back, current_year + years_forward + 1)
    nyse = _holidays_lib.financial_holidays("NYSE", years=years)
    return sorted(
        [(name, d.strftime("%Y-%m-%d")) for d, name in nyse.items()],
        key=lambda x: x[1],
    )


# Module-load: build the same names the rest of the app already imports.
_RAW_FUTURES = _generate_futures("SB", _SB_FUTURES_MONTHS)
_RAW_SW_FUTURES = _generate_futures("SW", _SW_FUTURES_MONTHS)
_RAW_OPTIONS = _generate_options()
_RAW_HOLIDAYS = _generate_holidays()
_HOLIDAY_DATES = frozenset(
    datetime.strptime(d, "%Y-%m-%d").date() for _, d in _RAW_HOLIDAYS
)


def _workday(start, offset, holidays):
    """Equivalent to Excel WORKDAY(start, offset, holidays)."""
    current = start
    steps = abs(offset)
    direction = timedelta(days=1 if offset > 0 else -1)
    while steps > 0:
        current += direction
        if current.weekday() < 5 and current not in holidays:
            steps -= 1
    return current


def _prev_bday(d, holidays):
    """Roll a date backward to the nearest business day."""
    while d.weekday() >= 5 or d in holidays:
        d -= timedelta(days=1)
    return d


def _parse_futures(contracts):
    """Parse SB futures: expiry = last business day before 1st of delivery month."""
    result = []
    for c in contracts:
        parts = c.split()
        code = parts[1][0]
        year = 2000 + int(parts[1][1:])
        month = _FUTURES_MONTH_CODES.get(code)
        ref_date = date(year, month, 1) if month else None
        expiry = _workday(ref_date, -1, _HOLIDAY_DATES) if ref_date else None
        result.append({"contract": c, "ref_date": ref_date, "expiry": expiry})
    return result


def _parse_sw_futures(contracts):
    """Parse SW futures: expiry = 16 calendar days before 1st of delivery month,
    adjusted to previous business day."""
    result = []
    for c in contracts:
        parts = c.split()
        code = parts[1][0]
        year = 2000 + int(parts[1][1:])
        month = _FUTURES_MONTH_CODES.get(code)
        ref_date = date(year, month, 1) if month else None
        if ref_date:
            raw = ref_date - timedelta(days=16)
            expiry = _prev_bday(raw, _HOLIDAY_DATES)
        else:
            expiry = None
        result.append({"contract": c, "ref_date": ref_date, "expiry": expiry})
    return result


def _parse_options(options):
    result = []
    for contract, underlying in options:
        parts = contract.split()
        code = parts[1][0]
        year = 2000 + int(parts[1][1:])
        month = _FUTURES_MONTH_CODES.get(code)
        if month:
            prev_month = month - 1 if month > 1 else 12
            prev_year = year if month > 1 else year - 1
            ref_date = date(prev_year, prev_month, 1)
        else:
            ref_date = None
        # =WORKDAY([@[Ref Date]]+13, 1, holidays)
        expiry = _workday(ref_date + timedelta(days=13), 1, _HOLIDAY_DATES) if ref_date else None
        result.append({"contract": contract, "underlying": underlying,
                       "ref_date": ref_date, "expiry": expiry})
    return result


# ─── Runtime regression assertion ────────────────────────────────────────────
# Runs at module import. If any anchor breaks, the app won't start —
# loud, fast, immediate. Update routes/_info_regression.py only when an
# intentional change to the source data has been verified externally.
def _assert_regression():
    from routes._info_regression import (
        GOLDEN_FUTURES, GOLDEN_OPTIONS, GOLDEN_HOLIDAY_DATES,
    )

    fut_by_code = {f["contract"]: f for f in _parse_futures(_RAW_FUTURES)}
    for code, expected_ref, expected_expiry in GOLDEN_FUTURES:
        actual = fut_by_code.get(code)
        assert actual is not None, f"Regression: futures contract {code} missing"
        assert actual["ref_date"] == expected_ref, (
            f"Regression: {code} ref_date drifted: "
            f"got {actual['ref_date']}, expected {expected_ref}"
        )
        assert actual["expiry"] == expected_expiry, (
            f"Regression: {code} expiry drifted: "
            f"got {actual['expiry']}, expected {expected_expiry}"
        )

    opt_by_code = {o["contract"]: o for o in _parse_options(_RAW_OPTIONS)}
    for code, exp_und, expected_ref, expected_expiry in GOLDEN_OPTIONS:
        actual = opt_by_code.get(code)
        assert actual is not None, f"Regression: options contract {code} missing"
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

    missing_holidays = GOLDEN_HOLIDAY_DATES - _HOLIDAY_DATES
    assert not missing_holidays, (
        f"Regression: holiday dates missing from _HOLIDAY_DATES: "
        f"{sorted(missing_holidays)}"
    )

_assert_regression()


_log = logging.getLogger(__name__)


@info_bp.route("/info")
def index():
    today = date.today()

    holidays_list = sorted([
        {
            "name": name,
            "date": datetime.strptime(d, "%Y-%m-%d").date(),
            "day": datetime.strptime(d, "%Y-%m-%d").strftime("%A"),
        }
        for name, d in _RAW_HOLIDAYS
    ], key=lambda x: x["date"])

    upcoming_date = next(
        (h["date"] for h in holidays_list if h["date"] >= today), None
    )

    grouped = defaultdict(list)
    for h in holidays_list:
        grouped[h["date"].year].append(h)

    # Parse futures and options with formula-calculated expiry (fallback)
    futures = _parse_futures(_RAW_FUTURES)
    sw_futures = _parse_sw_futures(_RAW_SW_FUTURES)
    options = _parse_options(_RAW_OPTIONS)

    return render_template("info.html",
                           grouped=dict(sorted(grouped.items())),
                           upcoming_date=upcoming_date,
                           futures=futures,
                           sw_futures=sw_futures,
                           options=options)

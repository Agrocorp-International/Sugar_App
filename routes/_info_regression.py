"""Frozen regression anchors for routes/info.py.

These are exact expected values for known contracts and holidays.
If any of these break, the import of routes/info.py will fail loudly.
Update only when you have intentionally changed the underlying data
and have verified the new values against the ICE Sugar #11 spec.
"""
from datetime import date

# Anchor futures: (contract code, expected ref_date, expected expiry)
# Picked to cover: simple cases, holiday-adjacent cases, year boundary.
GOLDEN_FUTURES = [
    ("SB H26", date(2026, 3, 1),  date(2026, 2, 27)),  # Fri, Pres Day was Mon Feb 16
    ("SB K26", date(2026, 5, 1),  date(2026, 4, 30)),  # Thu, no nearby holidays
    ("SB N26", date(2026, 7, 1),  date(2026, 6, 30)),  # Tue, after Juneteenth (Jun 19)
    ("SB V26", date(2026, 10, 1), date(2026, 9, 30)),  # Wed, no nearby holidays
    ("SB H27", date(2027, 3, 1),  date(2027, 2, 26)),  # Fri, Pres Day was Mon Feb 15
]

# Anchor options: (contract, underlying, expected ref_date, expected expiry)
# Includes a Juneteenth-adjacent case and a Presidents-Day-adjacent case.
GOLDEN_OPTIONS = [
    # SB N26 option: ref = 2026-06-01, ref+13 = 2026-06-14 (Sun) → workday +1 = Mon Jun 15
    ("SB N26", "SB N26", date(2026, 6, 1), date(2026, 6, 15)),
    # SB G26 option: ref = 2026-01-01 (NYE — itself a holiday), ref+13 = 2026-01-14 (Wed) → +1 = Thu Jan 15
    ("SB G26", "SB H26", date(2026, 1, 1), date(2026, 1, 15)),
    # SB H26 option: ref = 2026-02-01, ref+13 = 2026-02-14 (Sat) → +1 skips Pres Day Mon Feb 16 → Tue Feb 17
    ("SB H26", "SB H26", date(2026, 2, 1), date(2026, 2, 17)),
]

# Anchor holiday dates that MUST be in _HOLIDAY_DATES.
# These are the dates that, if missing, would silently shift expiry math.
GOLDEN_HOLIDAY_DATES = frozenset({
    date(2026, 1, 1),   # New Year
    date(2026, 1, 19),  # MLK
    date(2026, 2, 16),  # Presidents (Washington's Birthday)
    date(2026, 4, 3),   # Good Friday
    date(2026, 5, 25),  # Memorial
    date(2026, 6, 19),  # Juneteenth
    date(2026, 7, 3),   # Independence (observed — Jul 4 is Sat)
    date(2026, 9, 7),   # Labor
    date(2026, 11, 26), # Thanksgiving
    date(2026, 12, 25), # Christmas
})

"""Frozen regression anchors for routes/cotton_info.py.

Pinned values were verified against the user's broker records:
  CTN25 option expired 09 Jul 2025  (= CTN25 futures LTD)
  CTV26 option expires 08 Oct 2026  (= CTV26 futures LTD)

If any anchor breaks, the import of routes/cotton_info.py will fail loudly.
Update only when an intentional change to ICE Cotton #2 spec or NYSE holidays
has been verified externally.
"""
from datetime import date


# (contract, expected ref_date, expected expiry).
# Picked to cover all 5 listed months + a holiday-adjacent case + year boundary.
GOLDEN_CT_FUTURES = [
    # Anchor verified by user: CTN25 future LTD = 09 Jul 2025.
    ("CT N25", date(2025, 7, 1),  date(2025, 7, 9)),
    # Spans a holiday (Memorial Day Mon 25 May 2026).
    ("CT K26", date(2026, 5, 1),  date(2026, 5, 6)),
    # Anchor verified by user: CTV26 future LTD = 08 Oct 2026.
    ("CT V26", date(2026, 10, 1), date(2026, 10, 8)),
    # Memory anchor: CTH26 LTD = 09 Mar 2026 (matches Pres Day + weekends pattern).
    ("CT H26", date(2026, 3, 1),  date(2026, 3, 9)),
    # Year-end: spans Christmas Fri 25 Dec 2026 + Sat/Sun.
    ("CT Z26", date(2026, 12, 1), date(2026, 12, 8)),
]

# (contract, underlying, expected ref_date, expected expiry).
# Covers both rules:
#   Listed-month options (H, K, N, V, Z) share LTD with their same-month future.
#   Serial options (F, U, X) expire on the 3rd Friday of the option's own month.
GOLDEN_CT_OPTIONS = [
    ("CT N25", "CT N25", date(2025, 7, 1),  date(2025, 7, 9)),   # user-verified
    ("CT V26", "CT V26", date(2026, 10, 1), date(2026, 10, 8)),  # user-verified
    ("CT H26", "CT H26", date(2026, 3, 1),  date(2026, 3, 9)),
    ("CT Z26", "CT Z26", date(2026, 12, 1), date(2026, 12, 8)),
    # Serial options (3rd Friday of option month; none fall on a NYSE holiday).
    ("CT F26", "CT H26", date(2026, 1, 1),  date(2026, 1, 16)),
    ("CT U26", "CT Z26", date(2026, 9, 1),  date(2026, 9, 18)),
    ("CT X25", "CT Z25", date(2025, 11, 1), date(2025, 11, 21)),
]

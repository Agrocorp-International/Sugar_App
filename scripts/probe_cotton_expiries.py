"""Cotton expiry probe — print computed expiries for cross-checking against
ICE official calendar / TradeStation / sb_live_quotes.py before pinning the
regression in routes/_cotton_info_regression.py.

Usage:
    python -m scripts.probe_cotton_expiries

Cross-check sources:
- ICE Cotton #2 product spec ("Last Trading Day = seventeen business days from
  end of spot month")
- ../Options/sb_live_quotes.py COMMODITY_CONFIGS["CT"] active contracts
- TradeStation expiry metadata / broker statements / trader notes

If any printed expiry disagrees with an authoritative source, adjust
LTD_OFFSET / FND_OFFSET in routes/cotton_info.py and re-run.
"""
import sys
import os

# Allow running as `python scripts/probe_cotton_expiries.py` from project root
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from routes.cotton_info import (
    PARSED_CT_FUTURES,
    PARSED_CT_OPTIONS,
    LTD_OFFSET,
)

PROBE_FUTURES = ["CT N25", "CT H26", "CT K26", "CT N26", "CT V26",
                 "CT Z26", "CT H27"]

# Listed-month options only (serials not generated).
PROBE_OPTIONS = ["CT N25", "CT H26", "CT K26", "CT N26", "CT V26", "CT Z26"]


def main():
    print(f"Cotton expiry probe (LTD_OFFSET={LTD_OFFSET})")
    print()
    print("=== CT Futures ===")
    fut_by_code = {f["contract"]: f for f in PARSED_CT_FUTURES}
    for code in PROBE_FUTURES:
        f = fut_by_code.get(code)
        if not f:
            print(f"  {code}: NOT IN PARSED LIST")
            continue
        ref = f["ref_date"].strftime("%a %d %b %Y")
        exp = f["expiry"].strftime("%a %d %b %Y") if f["expiry"] else "—"
        print(f"  {code}: ref={ref}  expiry={exp}")

    print()
    print("=== CT Options ===")
    opt_by_code = {o["contract"]: o for o in PARSED_CT_OPTIONS}
    for code in PROBE_OPTIONS:
        o = opt_by_code.get(code)
        if not o:
            print(f"  {code}: NOT IN PARSED LIST")
            continue
        ref = o["ref_date"].strftime("%a %d %b %Y") if o["ref_date"] else "—"
        exp = o["expiry"].strftime("%a %d %b %Y") if o["expiry"] else "—"
        print(f"  {code}: underlying={o['underlying']}  ref={ref}  expiry={exp}")


if __name__ == "__main__":
    main()

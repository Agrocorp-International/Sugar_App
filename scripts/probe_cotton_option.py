"""Re-verify cotton futures + options fetch using active contracts.

Expected: both calls return non-empty quotes, confirming no code changes
needed to services/tradestation.py for cotton.

Active cotton expiries per sb_live_quotes.py: CTK26 (May), CTN26 (Jul), CTV26 (Oct).
"""
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from app import create_app
from services.tradestation import fetch_prices

app = create_app(); ctx = app.app_context(); ctx.push()

CONTRACTS = ["CTK26", "CTK26C6900", "CTN26C6500"]
print(f"Fetching: {CONTRACTS}")
results, errors, sett_date = fetch_prices(CONTRACTS)
print(f"\nSETT_DATE: {sett_date}")
print(f"ERRORS:   {errors}")
print(f"RESULTS ({len(results)}):")
for r in results:
    print(f"  {r['contract']:15s} sett={r.get('settlement')!r:10s} "
          f"live={r.get('live_price')!r:10s} iv={r.get('iv')!r:10s} "
          f"delta={r.get('delta')!r}")

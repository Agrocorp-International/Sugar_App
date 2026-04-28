"""Print cotton option IV/delta diagnostics without updating stored prices.

Usage:
    python scripts/diagnose_cotton_iv_delta.py CTK26C6900 CTN26P6500

If no contracts are supplied, the script uses the first five active watched
cotton options. Compare the output against broker/ICE screen values to identify
whether the mismatch is from quote fields, dates, rate assumptions, or expiry.
"""
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from app import create_app
from models.cotton import CottonWatchedContract
from routes.cotton_prices import OPTIONS_BASE_EXPIRY_MAP
from services.tradestation import fetch_cotton_price_diagnostics, is_option_contract


def _default_contracts():
    return [
        wc.contract
        for wc in CottonWatchedContract.query.filter_by(expired=False)
                                             .order_by(CottonWatchedContract.sort_order,
                                                       CottonWatchedContract.created_at)
                                             .all()
        if is_option_contract(wc.contract)
    ][:5]


def main():
    app = create_app()
    with app.app_context():
        contracts = [c.strip().upper().replace(" ", "") for c in sys.argv[1:] if c.strip()]
        if not contracts:
            contracts = _default_contracts()
        result = fetch_cotton_price_diagnostics(
            contracts,
            internal_expiry_map=OPTIONS_BASE_EXPIRY_MAP,
        )
        print(json.dumps(result, indent=2, default=str))


if __name__ == "__main__":
    main()

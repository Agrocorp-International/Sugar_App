"""
Dashboard P&L summary computation.
Aggregates Alpha, Whites, Raws, and FFA P&L values from DB + Excel.
"""
from models.db import TradePosition, MarketPrice, FFATrade, FFASettlement
from routes.positions import build_contract_key, LOT_MULTIPLIERS as _MULTIPLIERS
from services.physical_pnl import compute_all_pnl_totals
from services.price_source import load_price_map, resolve_delta


def _load_settlement_prices(source='sett1'):
    """Return ``{normalised_contract: price}`` using the chosen source.

    Function name preserved for backward compatibility; the actual data
    may be live prices when ``source='live'``.
    """
    pm, _ = load_price_map(source)
    return pm


def _compute_alpha_pnl(settlement_prices):
    """Return (alpha_m2m, alpha_pnl) for all Book='Spec' positions.

    alpha_m2m: sum for Unrealised positions
    alpha_pnl: sum for Realised positions
    Both use: (settlement - price) × lots × multiplier + commission
    Returns (None, None) if no Spec positions exist.
    """
    alpha_m2m = None
    alpha_pnl = None
    for pos in TradePosition.query.all():
        d = pos.data
        if d.get("Book__c") != "Spec":
            continue
        key = build_contract_key(d)
        settlement = settlement_prices.get(key)
        is_option = bool(d.get("Put_Call_2__c") and d.get("Strike__c") is not None)
        if settlement is None and is_option:
            settlement = 0
        price = d.get("Price__c")
        lots = float(d.get("Long__c") or 0) + float(d.get("Short__c") or 0)
        multiplier = _MULTIPLIERS.get(d.get("Commodity_Name__c") or "", 0)
        commission = float(d.get("Broker_Commission__c") or 0)
        if settlement is not None and price is not None and multiplier:
            pnl = (settlement - float(price)) * lots * multiplier + commission
            if d.get("Realised__c") == "Realised":
                alpha_pnl = (alpha_pnl or 0) + pnl
            else:
                alpha_m2m = (alpha_m2m or 0) + pnl
    return alpha_m2m, alpha_pnl


def _compute_ffa_m2m():
    """Return total FFA PNL using the same formula as routes/ffa.py."""
    settlements = FFASettlement.query.all()
    settlement_lookup = {
        s.shipment.strip().lower(): {"smx": s.smx, "pmx": s.pmx}
        for s in settlements
    }
    total = None
    for trade in FFATrade.query.all():
        s_row = settlement_lookup.get((trade.shipment or "").strip().lower(), {})
        s_val = s_row.get((trade.size or "").strip().lower())
        if s_val is not None and trade.trade_price is not None and (trade.long_ is not None or trade.short_ is not None):
            position = (trade.long_ or 0) - (trade.short_ or 0)
            total = (total or 0) + (s_val - trade.trade_price) * position
    return total


def compute_pnl_summary(source='sett1'):
    """Return dict with all P&L summary values for the dashboard.

    Any value may be None if data is missing (Excel not found, prices not loaded, etc.).
    Sign convention: positive = profit, negative = loss.

    ``source`` selects sett-1 (default) or live prices, with silent
    fallback to sett-1 for any contract whose live value is missing.
    """
    settlement_prices = _load_settlement_prices(source)

    alpha_m2m, alpha_pnl = _compute_alpha_pnl(settlement_prices)
    net_alpha = _safe_sum(alpha_m2m, alpha_pnl)

    raws_physical, whites_physical, raws_futures, whites_futures, _, _ = compute_all_pnl_totals(source)

    whites_pnl = _safe_sum(whites_physical, whites_futures)
    ffa_m2m = _compute_ffa_m2m()
    net_raws = _safe_sum(raws_physical, raws_futures, ffa_m2m)

    total_pnl = _safe_sum(net_alpha, whites_pnl, net_raws)

    return {
        "alpha_m2m":         alpha_m2m,
        "alpha_pnl":         alpha_pnl,
        "net_alpha_pnl":     net_alpha,
        "whites_physical_m2m": whites_physical,
        "whites_futures_m2m":  whites_futures,
        "whites_pnl":          whites_pnl,
        "raws_physical_m2m": raws_physical,
        "raws_futures_m2m":  raws_futures,
        "ffa_m2m":           ffa_m2m,
        "net_raws_pnl":      net_raws,
        "total_pnl":         total_pnl,
    }


def _compute_alpha_position(market, source='sett1'):
    """Return total delta-adjusted position for all Book=Spec positions."""
    total = None
    for pos in TradePosition.query.all():
        d = pos.data
        if d.get("Book__c") != "Spec":
            continue
        key = build_contract_key(d)
        mp = market.get(key)
        is_option = bool(d.get("Put_Call_2__c") and d.get("Strike__c") is not None)
        delta_val = resolve_delta(mp, source)
        if delta_val is not None:
            delta = delta_val
        elif is_option:
            delta = 0
        else:
            delta = 1.0  # futures default
        lots = float(d.get("Long__c") or 0) + float(d.get("Short__c") or 0)
        total = (total or 0) + delta * lots
    return total


def _compute_spread_position(market, source='sett1'):
    """Return net spread leg position for all positions with a Spread Contract set.

    Near leg (contract last3 == spread last3): contributes 0.
    Far leg: contributes delta × lots.
    """
    total = None
    for pos in TradePosition.query.all():
        d = pos.data
        if d.get("Book__c") != "Spec":
            continue
        if d.get("Realised__c") != "Unrealised":
            continue
        if (pos.instrument or "").strip() != "Spread":
            continue
        spread = (pos.spread or "").strip()
        if not spread:
            continue
        key = build_contract_key(d)
        contract_last3 = (d.get("Contract__c") or "").replace(" ", "")[-3:]
        spread_last3 = spread[-3:]
        if contract_last3 == spread_last3:
            continue  # near leg — zero contribution
        mp = market.get(key)
        is_option_s = bool(d.get("Put_Call_2__c") and d.get("Strike__c") is not None)
        delta_val = resolve_delta(mp, source)
        if delta_val is not None:
            delta = delta_val
        elif is_option_s:
            delta = 0
        else:
            delta = 1.0  # futures default
        if delta is None:
            continue
        lots = float(d.get("Long__c") or 0) + float(d.get("Short__c") or 0)
        total = (total or 0) + delta * lots
    return total


def compute_exposure(source='sett1'):
    """Return dict with exposure values for the dashboard exposure table.

    Keys: alpha, raws, whites, spread, total.
    Any value may be None if data is missing.

    ``source`` selects sett-1 (default) or live deltas with fallback.
    """
    _, _, _, _, raws_exposure, whites_exposure = compute_all_pnl_totals(source)
    market = {mp.contract: mp for mp in MarketPrice.query.all()}
    alpha = _compute_alpha_position(market, source)
    spread = _compute_spread_position(market, source)
    return {
        "alpha":  alpha,
        "raws":   raws_exposure,
        "whites": whites_exposure,
        "spread": spread,
        "total":  _safe_sum(alpha, raws_exposure, whites_exposure),
    }


def _safe_sum(*values):
    """Sum values, treating None as 0 only if at least one value is not None."""
    non_none = [v for v in values if v is not None]
    if not non_none:
        return None
    return sum(non_none)


def get_reference_snapshots():
    """Return (daily_snap, weekly_snap, monthly_snap) by named slot."""
    from models.db import db, PnlSnapshot
    return (
        db.session.get(PnlSnapshot, 'daily'),
        db.session.get(PnlSnapshot, 'weekly'),
        db.session.get(PnlSnapshot, 'monthly'),
    )

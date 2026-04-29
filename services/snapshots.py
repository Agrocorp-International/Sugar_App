"""Centralized PnL snapshot creation — used by both manual and auto routes."""
from datetime import datetime
from sqlalchemy import cast, Date
from models.db import db, PnlSnapshot, TradePosition
from services.pnl_summary import compute_pnl_summary


SNAPSHOT_DETAIL_FIELDS = [
    "alpha_m2m",
    "alpha_pnl",
    "whites_physical_m2m",
    "whites_futures_m2m",
    "raws_physical_m2m",
    "raws_futures_m2m",
    "ffa_m2m",
]

SNAPSHOT_CALCULATED_FIELDS = [
    "net_alpha_pnl",
    "whites_pnl",
    "net_raws_pnl",
    "total_pnl",
]


def _to_number_or_none(value):
    if value is None or value == "":
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _safe_sum(*values):
    numbers = [_to_number_or_none(v) for v in values]
    non_none = [v for v in numbers if v is not None]
    if not non_none:
        return None
    return sum(non_none)


def recalculate_snapshot_totals(data):
    """Return snapshot data with subtotal and grand-total fields derived."""
    updated = dict(data or {})
    net_alpha = _safe_sum(updated.get("alpha_m2m"), updated.get("alpha_pnl"))
    whites_pnl = _safe_sum(updated.get("whites_physical_m2m"), updated.get("whites_futures_m2m"))
    net_raws = _safe_sum(updated.get("raws_physical_m2m"), updated.get("raws_futures_m2m"), updated.get("ffa_m2m"))
    total_pnl = _safe_sum(net_alpha, whites_pnl, net_raws)
    updated.update({
        "net_alpha_pnl": net_alpha,
        "whites_pnl": whites_pnl,
        "net_raws_pnl": net_raws,
        "total_pnl": total_pnl,
    })
    return updated


def create_snapshot(slot: str, source: str = "manual", scheduled_for: datetime | None = None) -> PnlSnapshot:
    """Compute a fresh PnL summary, persist it for ``slot``, return the row.

    Uses ``db.session.merge`` so the existing single-row-per-slot behavior
    (overwrite previous snapshot) is preserved. Raises on failure so the
    caller can flash/log; session rollback is the caller's job.
    """
    if slot not in ("daily", "weekly", "monthly"):
        raise ValueError(f"invalid slot: {slot!r}")

    latest = TradePosition.query.order_by(
        cast(TradePosition.data["Trade_Date__c"].as_string(), Date).desc()
    ).first()
    as_of = latest.data.get("Trade_Date__c") if latest else None

    pnl_data = compute_pnl_summary()
    pnl_data["as_of_date"] = as_of

    snap_time = datetime.utcnow()

    # Freeze per-leg state for Taylor-series attribution (daily slot only).
    # Failure here must not kill the snapshot — attribution is a nice-to-have.
    if slot == "daily":
        try:
            from services.pnl_attribution import build_attribution_legs
            legs, meta = build_attribution_legs(snap_time)
            pnl_data["attribution_legs"] = legs
            pnl_data["attribution_meta"] = meta
        except Exception:
            import logging
            logging.getLogger(__name__).exception("attribution snapshot failed")

    snap = PnlSnapshot(
        slot=slot,
        snapshotted_at=snap_time,
        data=pnl_data,
        source=source,
        scheduled_for=scheduled_for,
    )
    merged = db.session.merge(snap)
    db.session.commit()
    return merged

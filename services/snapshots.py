"""Centralized PnL snapshot creation — used by both manual and auto routes."""
from datetime import datetime
from sqlalchemy import cast, Date
from models.db import db, PnlSnapshot, TradePosition
from services.pnl_summary import compute_pnl_summary


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

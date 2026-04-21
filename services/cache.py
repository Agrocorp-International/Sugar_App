"""Tiny in-process TTL cache for expensive dashboard computations.

Designed for a single-worker Gunicorn setup (Azure App Service default). If
the app is ever scaled to multiple workers, swap this for flask-caching with
FileSystemCache so all workers share a single store.

Usage:
    from services.cache import get_or_compute, bump_positions, bump_prices

    result = get_or_compute(
        key=("compute_pnl_summary", positions_version(), prices_version(), price_source),
        ttl=120,
        fn=lambda: compute_pnl_summary(...),
    )

Invalidation uses two module-level version counters, `positions_version` and
`prices_version`, that every write path to TradePosition / MarketPrice must
bump. Including both in every dashboard cache key means any write auto-busts
every cached aggregate that depended on that table.
"""

import time
from threading import Lock

_cache = {}  # {key: (value, expires_at)}
_cache_lock = Lock()

_versions = {"positions": 0, "prices": 0}
_versions_lock = Lock()


def get_or_compute(key, ttl, fn):
    """Return cached value for `key` or compute and store it with a TTL."""
    now = time.time()
    with _cache_lock:
        entry = _cache.get(key)
        if entry is not None and now < entry[1]:
            return entry[0]
    value = fn()
    with _cache_lock:
        _cache[key] = (value, time.time() + ttl)
    return value


def invalidate_all():
    """Drop every cached entry. Cheap sledgehammer for tests/admin."""
    with _cache_lock:
        _cache.clear()


def positions_version():
    with _versions_lock:
        return _versions["positions"]


def prices_version():
    with _versions_lock:
        return _versions["prices"]


def bump_positions():
    """Call after any write to TradePosition (sync, edit, delete, admin)."""
    with _versions_lock:
        _versions["positions"] += 1


def bump_prices():
    """Call after any write to MarketPrice or WatchedContract."""
    with _versions_lock:
        _versions["prices"] += 1


def install_autobump():
    """Install SQLAlchemy session listeners that auto-bump version counters
    whenever dashboard-relevant models are inserted, updated, or deleted.

    Call once at app startup (from create_app after db.init_app). Using
    after_commit rather than after_flush ensures we only bump on successful
    transactions — failed flushes that get rolled back don't invalidate the
    cache.
    """
    from sqlalchemy import event
    from models.db import (
        db, TradePosition, MarketPrice, WatchedContract,
        PhysicalTrade, FFATrade, FFASettlement, PhysicalDeal, PnlOverride,
    )

    position_models = {
        TradePosition, PhysicalTrade, FFATrade, FFASettlement,
        PhysicalDeal, PnlOverride,
    }
    price_models = {MarketPrice, WatchedContract}

    @event.listens_for(db.session, "before_commit")
    def _mark_pending_bumps(session):
        # Snapshot which model classes have pending writes. Stored on the
        # session so after_commit can act after the transaction succeeds.
        seen = set()
        for obj in list(session.new) + list(session.dirty) + list(session.deleted):
            seen.add(type(obj))
        session.info["_cache_bump_models"] = seen

    @event.listens_for(db.session, "after_commit")
    def _apply_bumps(session):
        seen = session.info.pop("_cache_bump_models", set())
        if seen & position_models:
            bump_positions()
        if seen & price_models:
            bump_prices()

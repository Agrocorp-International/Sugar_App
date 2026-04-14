"""Centralized price-source resolution for the sett-1 vs live PnL toggle.

Used by dashboard, raws, summary, positions, and options pages so that
all P&L calculations agree on which price field to read for a given
contract.

Conventions
-----------
- ``source`` is one of ``'sett1'`` or ``'live'``.
- ``'sett1'`` always reads ``mp.settlement`` / ``mp.delta`` / ``mp.iv``.
- ``'live'`` prefers ``mp.live_price`` / ``mp.live_delta`` / ``mp.live_iv``,
  with a silent fallback to the corresponding settlement field when the
  live value is missing. The fallback is **counted** so the UI can warn
  the user that the displayed numbers are mixed.
"""
from flask import request
from models.db import MarketPrice

VALID_SOURCES = ("sett1", "live")
DEFAULT_SOURCE = "sett1"


def get_price_source():
    """Resolve the active price source for the current request.

    Resolution order:
      1. Explicit ``?price_source=`` query param wins (allows sharing a
         link in a specific mode without changing the user's persisted
         preference).
      2. Otherwise, read the ``price_source`` cookie set by the navbar
         toggle (see ``dashboard.set_price_source``).
      3. Otherwise, fall back to ``'sett1'``.

    Safe to call from any view function or ``before_request`` hook.
    """
    src = request.args.get("price_source")
    if src in VALID_SOURCES:
        return src
    src = request.cookies.get("price_source")
    if src in VALID_SOURCES:
        return src
    return DEFAULT_SOURCE


def resolve_price(mp, source=DEFAULT_SOURCE):
    """Return the price for a single MarketPrice row using the chosen source.

    Falls back to settlement when ``source='live'`` and ``live_price`` is None.
    Returns None if both fields are None.
    """
    if mp is None:
        return None
    if source == "live":
        return mp.live_price if mp.live_price is not None else mp.settlement
    return mp.settlement


def resolve_delta(mp, source=DEFAULT_SOURCE):
    """Same as resolve_price but for the delta field."""
    if mp is None:
        return None
    if source == "live":
        return mp.live_delta if mp.live_delta is not None else mp.delta
    return mp.delta


def resolve_iv(mp, source=DEFAULT_SOURCE):
    """Same as resolve_price but for the IV field."""
    if mp is None:
        return None
    if source == "live":
        return mp.live_iv if mp.live_iv is not None else mp.iv
    return mp.iv


def load_price_map(source=DEFAULT_SOURCE, normalise=True):
    """Return ``({contract_key: price}, fallback_count)``.

    Loads every row from MarketPrice once and applies the source rules.

    Parameters
    ----------
    source : str
        ``'sett1'`` or ``'live'``.
    normalise : bool
        If True (default), keys are uppercased with spaces stripped
        (e.g. ``"SBH26"``). This matches the convention used by
        ``routes/raws.py:_load_settlement_prices``,
        ``services/pnl_summary.py:_load_settlement_prices``, and
        ``routes/positions.py:build_contract_key``.
        Set to False if you need raw contract strings.

    Returns
    -------
    (dict, int)
        The price map and the number of contracts that fell back to
        settlement (always 0 when source='sett1').
    """
    rows = MarketPrice.query.all()
    pm = {}
    fallbacks = 0
    for mp in rows:
        key = mp.contract.replace(" ", "").upper() if normalise else mp.contract
        if source == "live":
            if mp.live_price is not None:
                pm[key] = mp.live_price
            elif mp.settlement is not None:
                pm[key] = mp.settlement
                fallbacks += 1
        else:
            if mp.settlement is not None:
                pm[key] = mp.settlement
    return pm, fallbacks


def load_delta_map(source=DEFAULT_SOURCE, normalise=True):
    """Like ``load_price_map`` but for the delta field.

    Returns ``({contract_key: delta}, fallback_count)``.
    """
    rows = MarketPrice.query.all()
    dm = {}
    fallbacks = 0
    for mp in rows:
        key = mp.contract.replace(" ", "").upper() if normalise else mp.contract
        if source == "live":
            if mp.live_delta is not None:
                dm[key] = mp.live_delta
            elif mp.delta is not None:
                dm[key] = mp.delta
                fallbacks += 1
        else:
            if mp.delta is not None:
                dm[key] = mp.delta
    return dm, fallbacks


def count_fallbacks(source=DEFAULT_SOURCE):
    """Return the number of *active* contracts that would fall back to sett-1
    if ``source='live'``. Always 0 when source='sett1'.

    Expired contracts (``WatchedContract.expired=True``) are excluded because
    they will never have live prices and the fallback is expected.
    """
    if source != "live":
        return 0
    from models.db import WatchedContract
    active = {wc.contract for wc in WatchedContract.query.filter_by(expired=False).all()}
    fallbacks = 0
    for mp in MarketPrice.query.all():
        if mp.contract in active and mp.live_price is None and mp.settlement is not None:
            fallbacks += 1
    return fallbacks

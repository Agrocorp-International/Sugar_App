"""Request-scoped DB cache using Flask g.

Each helper fetches its table at most once per HTTP request, then returns the
same list on subsequent calls within the same request. Falls back to a direct
DB query when called outside a request context (CLI commands, tests, background
jobs) so callers never have to worry about context availability.
"""
from flask import g, has_request_context
from models.db import TradePosition, MarketPrice


def get_all_positions():
    """Return all TradePosition rows, cached for the lifetime of the current request."""
    if has_request_context():
        if not hasattr(g, '_positions'):
            g._positions = TradePosition.query.all()
        return g._positions
    return TradePosition.query.all()


def get_all_market_prices():
    """Return all MarketPrice rows, cached for the lifetime of the current request."""
    if has_request_context():
        if not hasattr(g, '_market_prices'):
            g._market_prices = MarketPrice.query.all()
        return g._market_prices
    return MarketPrice.query.all()

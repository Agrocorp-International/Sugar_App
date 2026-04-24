from datetime import timedelta
from urllib.parse import urlencode
from flask import Flask, g, request, redirect, url_for
from config import Config
from models.db import db
from models.cotton import CottonMarketPrice
from routes.dashboard import dashboard_bp
from routes.positions import positions_bp
from routes.sync import sync_bp
from routes.prices import prices_bp
from routes.summary import summary_bp
from routes.physical import physical_bp
from routes.ffa import ffa_bp
from routes.raws import raws_bp
from routes.options import options_bp
from routes.info import info_bp
from routes.strategy_warnings import strategy_warnings_bp
from routes.admin import admin_bp
from routes.wip import wip_bp
from routes.notes import notes_bp
from routes.cotton_dashboard import cotton_dashboard_bp
from routes.cotton_sync import cotton_sync_bp
from routes.cotton_positions import cotton_positions_bp
from routes.cotton_prices import cotton_prices_bp
from routes.cotton_info import cotton_info_bp
from routes.neon_sync import neon_sync_bp


def create_app():
    app = Flask(__name__)
    app.config.from_object(Config)

    db.init_app(app)

    # Auto-bump cache version counters on writes to dashboard-relevant tables.
    from services.cache import install_autobump
    install_autobump()

    # Sugar section — all existing blueprints mounted under /sugar.
    app.register_blueprint(dashboard_bp,         url_prefix="/sugar")
    app.register_blueprint(positions_bp,         url_prefix="/sugar")
    app.register_blueprint(sync_bp,              url_prefix="/sugar")
    app.register_blueprint(prices_bp,            url_prefix="/sugar")
    app.register_blueprint(summary_bp,           url_prefix="/sugar")
    app.register_blueprint(physical_bp,          url_prefix="/sugar")
    app.register_blueprint(ffa_bp,               url_prefix="/sugar")
    app.register_blueprint(raws_bp,              url_prefix="/sugar")
    app.register_blueprint(options_bp,           url_prefix="/sugar")
    app.register_blueprint(info_bp,              url_prefix="/sugar")
    app.register_blueprint(strategy_warnings_bp, url_prefix="/sugar")
    app.register_blueprint(admin_bp,             url_prefix="/sugar")
    app.register_blueprint(wip_bp,               url_prefix="/sugar")
    app.register_blueprint(notes_bp,             url_prefix="/sugar")
    app.register_blueprint(neon_sync_bp,         url_prefix="/sugar")

    # Cotton section — mounted under /cotton.
    app.register_blueprint(cotton_dashboard_bp, url_prefix="/cotton")
    app.register_blueprint(cotton_sync_bp,      url_prefix="/cotton")
    app.register_blueprint(cotton_positions_bp, url_prefix="/cotton")
    app.register_blueprint(cotton_prices_bp,    url_prefix="/cotton")
    app.register_blueprint(cotton_info_bp,      url_prefix="/cotton")

    @app.route("/")
    def root():
        # Redirect by endpoint name (prefix-agnostic); resolves to /sugar/.
        return redirect(url_for("dashboard.index"))

    @app.before_request
    def _set_section():
        p = request.path
        if p.startswith("/sugar"):
            g.section = "sugar"
        elif p.startswith("/cotton"):
            g.section = "cotton"
        else:
            g.section = None  # root redirect, static, unknown

    @app.before_request
    def _set_price_source():
        # Make the active price source available to every template via g.
        # base.html reads g.price_source for the navbar toggle highlight
        # and the red-navbar live indicator.
        from services.price_source import get_price_source, count_fallbacks
        g.price_source = get_price_source()
        g.fallback_count = count_fallbacks(g.price_source)
        # Build a "clean" current URL with price_source stripped, used by
        # the navbar Sett-1/Live toggle as its `next` target. Preserves
        # multi-value params (e.g. trade_id appears 5x on /options) by
        # iterating with multi=True instead of to_dict(flat=True).
        args = [(k, v) for k, v in request.args.items(multi=True)
                if k != 'price_source']
        g.toggle_next_url = request.path + ('?' + urlencode(args) if args else '')

    @app.template_filter("comma_int")
    def comma_int(value):
        return "{:,}".format(int(round(value)))

    @app.template_filter("sgt")
    def to_sgt(dt, fmt="%d %b %Y %H:%M"):
        if dt is None:
            return "—"
        return (dt + timedelta(hours=8)).strftime(fmt) + " SGT"

    @app.template_filter("format_contract")
    def format_contract(value):
        """Insert space after 2-char commodity prefix: SBQ26 -> SB Q26."""
        if not value or len(value) < 3:
            return value or ''
        if value[2] == ' ':
            return value
        prefix = value[:2].upper()
        if prefix in ('SB', 'SW', 'CT'):
            return value[:2] + ' ' + value[2:]
        return value

    with app.app_context():
        db.create_all()
        # Lightweight idempotent migration: add per-mode fetch timestamp
        # columns to sugar_market_prices if they don't already exist.
        from sqlalchemy import text
        db.session.execute(text(
            "ALTER TABLE sugar_market_prices "
            "ADD COLUMN IF NOT EXISTS sett_fetched_at TIMESTAMP, "
            "ADD COLUMN IF NOT EXISTS live_fetched_at TIMESTAMP"
        ))
        db.session.execute(text(
            "ALTER TABLE sugar_pnl_snapshots "
            "ADD COLUMN IF NOT EXISTS source VARCHAR(10), "
            "ADD COLUMN IF NOT EXISTS scheduled_for TIMESTAMP"
        ))
        db.session.execute(text(
            "ALTER TABLE sugar_pnl_overrides "
            "ADD COLUMN IF NOT EXISTS slot VARCHAR(10) NOT NULL DEFAULT 'current'"
        ))
        # Neon ingestion: widen sf_id, add source/unique_trade_id/dedup_key.
        # Widen is metadata-only (no FK refs to sf_id). Postgres allows
        # multiple NULLs on a UNIQUE column, so SF rows (NULL unique_trade_id)
        # coexist fine with Neon rows (non-NULL) on a plain unique index.
        db.session.execute(text(
            "ALTER TABLE sugar_trade_positions "
            "ALTER COLUMN sf_id TYPE VARCHAR(64)"
        ))
        db.session.execute(text(
            "ALTER TABLE sugar_trade_positions "
            "ADD COLUMN IF NOT EXISTS source VARCHAR(10) NOT NULL DEFAULT 'sf', "
            "ADD COLUMN IF NOT EXISTS unique_trade_id VARCHAR(128), "
            "ADD COLUMN IF NOT EXISTS dedup_key VARCHAR(64)"
        ))
        db.session.execute(text(
            "UPDATE sugar_trade_positions SET source = 'sf' WHERE source IS NULL"
        ))
        db.session.execute(text(
            "CREATE UNIQUE INDEX IF NOT EXISTS ix_sugar_trade_positions_unique_trade_id "
            "ON sugar_trade_positions (unique_trade_id)"
        ))
        db.session.execute(text(
            "CREATE INDEX IF NOT EXISTS ix_sugar_trade_positions_source "
            "ON sugar_trade_positions (source)"
        ))
        db.session.execute(text(
            "CREATE INDEX IF NOT EXISTS ix_sugar_trade_positions_dedup_key "
            "ON sugar_trade_positions (dedup_key)"
        ))
        db.session.execute(text(
            "ALTER TABLE cotton_market_prices "
            "ADD COLUMN IF NOT EXISTS sett_fetched_at TIMESTAMP, "
            "ADD COLUMN IF NOT EXISTS live_fetched_at TIMESTAMP"
        ))
        db.session.commit()

    return app


if __name__ == "__main__":
    app = create_app()
    app.run(debug=app.config.get("DEBUG", False), port=5001)

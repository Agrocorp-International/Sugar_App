from datetime import timedelta
from urllib.parse import urlencode
from flask import Flask, g, request
from config import Config
from models.db import db
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


def create_app():
    app = Flask(__name__)
    app.config.from_object(Config)

    db.init_app(app)

    app.register_blueprint(dashboard_bp)
    app.register_blueprint(positions_bp)
    app.register_blueprint(sync_bp)
    app.register_blueprint(prices_bp)
    app.register_blueprint(summary_bp)
    app.register_blueprint(physical_bp)
    app.register_blueprint(ffa_bp)
    app.register_blueprint(raws_bp)
    app.register_blueprint(options_bp)
    app.register_blueprint(info_bp)
    app.register_blueprint(strategy_warnings_bp)
    app.register_blueprint(admin_bp)

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
        if prefix in ('SB', 'SW'):
            return value[:2] + ' ' + value[2:]
        return value

    with app.app_context():
        db.create_all()

    return app


if __name__ == "__main__":
    app = create_app()
    app.run(debug=True, port=5001)

from flask import Blueprint, render_template
from models.cotton import CottonTradePosition, CottonSyncLog

cotton_dashboard_bp = Blueprint("cotton_dashboard", __name__)


@cotton_dashboard_bp.route("/")
def index():
    last_sync = CottonSyncLog.query.order_by(CottonSyncLog.synced_at.desc()).first()
    trade_count = CottonTradePosition.query.count()
    return render_template(
        "cotton/dashboard.html",
        last_sync=last_sync,
        trade_count=trade_count,
    )

from datetime import datetime
from flask import Blueprint, render_template, request, redirect, url_for, flash
from models.db import db, PhysicalTrade
from services.salesforce import get_sf_connection, fetch_report

REPORT_ID = "00OQ800000Dog1NMAR"
PAGE_SIZE = 40

physical_bp = Blueprint("physical", __name__)


@physical_bp.route("/physical")
def index():
    pagination = PhysicalTrade.query.order_by(PhysicalTrade.row_index).paginate(
        page=request.args.get("page", 1, type=int),
        per_page=PAGE_SIZE,
        error_out=False,
    )
    rows = pagination.items
    columns = list(rows[0].data.keys()) if rows else []
    last_synced = rows[0].synced_at if rows else None
    return render_template(
        "physical.html",
        pagination=pagination,
        columns=columns,
        last_synced=last_synced,
    )


@physical_bp.route("/physical/sync", methods=["POST"])
def sync():
    try:
        sf = get_sf_connection()
        column_labels, rows = fetch_report(sf, REPORT_ID)
        now = datetime.utcnow()
        PhysicalTrade.query.delete()
        db.session.bulk_insert_mappings(
            PhysicalTrade,
            [{"row_index": i, "data": row, "synced_at": now} for i, row in enumerate(rows)],
        )
        db.session.commit()
        flash(f"Synced {len(rows)} physical trades.", "success")
    except Exception as e:
        db.session.rollback()
        flash(f"Sync failed: {e}", "danger")
    return redirect(url_for("physical.index"))

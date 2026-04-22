from collections import OrderedDict
from datetime import datetime
from flask import Blueprint, render_template, redirect, url_for, flash
from models.db import db, PhysicalTrade
from services.salesforce import get_sf_connection, fetch_report

REPORT_ID = "00OQ800000Dog1NMAR"

physical_bp = Blueprint("physical", __name__)


# Columns shown in the per-venture row table. Everything else is available on
# the full record but hidden by default to keep the page scannable.
ROW_COLUMNS = [
    "Sub Contract Name",
    "Commodity: Commodity Name",
    "Quantity",
    "Price per Unit",
    "Basis Price",
    "Future Month code",
    "Shipment Start Date",
    "Shipment  End Date",
    "Incoterms",
    "Price Type",
    "Packing",
]


def _to_float(x):
    try:
        return float(str(x).replace(",", "").strip())
    except (TypeError, ValueError):
        return None


def _first_nonblank(values):
    for v in values:
        if v not in (None, "", "-"):
            return v
    return ""


def _build_groups(rows):
    """Group rows by Venture, preserving first-seen order. Blank ventures go
    into a final 'Unassigned' group."""
    buckets = OrderedDict()
    for r in rows:
        key = (r.data.get("Venture") or "").strip()
        buckets.setdefault(key, []).append(r)

    groups = []
    unassigned = None
    for venture_id, bucket_rows in buckets.items():
        qtys = [_to_float(i.data.get("Quantity")) for i in bucket_rows]
        total_qty = sum(q for q in qtys if q is not None)
        summary = {
            "venture_id": venture_id or "Unassigned",
            "is_unassigned": venture_id == "",
            "row_count": len(bucket_rows),
            "total_qty": total_qty,
            "counterparty": _first_nonblank(
                i.data.get("Counterparty: Account Name") for i in bucket_rows
            ),
            "main_commodity": _first_nonblank(
                i.data.get("Main Commodity") for i in bucket_rows
            ),
            "status": _first_nonblank(i.data.get("Venture Status") for i in bucket_rows),
            "vessel": _first_nonblank(
                (i.data.get("Venture: Vessel Name") or "").strip() for i in bucket_rows
            ) or _first_nonblank(
                (i.data.get("Venture: To be nominated vessel name") or "").strip()
                for i in bucket_rows
            ),
            "incoterms": _first_nonblank(i.data.get("Incoterms") for i in bucket_rows),
            "origin": _first_nonblank(
                i.data.get("Origin: Country Name") for i in bucket_rows
            ),
            "rows": bucket_rows,
        }
        if summary["is_unassigned"]:
            unassigned = summary
        else:
            groups.append(summary)

    # Actual ventures first, Unassigned last.
    if unassigned is not None:
        groups.append(unassigned)
    return groups


@physical_bp.route("/physical")
def index():
    rows = PhysicalTrade.query.order_by(PhysicalTrade.row_index).all()
    groups = _build_groups(rows)
    last_synced = rows[0].synced_at if rows else None
    all_columns = list(rows[0].data.keys()) if rows else []
    return render_template(
        "physical.html",
        groups=groups,
        total_rows=len(rows),
        last_synced=last_synced,
        row_columns=ROW_COLUMNS,
        all_columns=all_columns,
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

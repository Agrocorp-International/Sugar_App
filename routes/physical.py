from collections import OrderedDict
from datetime import datetime
from flask import Blueprint, render_template, redirect, url_for, flash

_MTM_FORMATS = ["%b-%y", "%b %Y", "%B %Y", "%Y-%m", "%m/%Y"]

def _mtm_sort_key(val):
    # Normalise non-standard abbreviations
    normalised = val.strip().replace("Sept-", "Sep-").replace("Sept ", "Sep ")
    for fmt in _MTM_FORMATS:
        try:
            return datetime.strptime(normalised, fmt)
        except ValueError:
            continue
    return datetime.max
from models.db import db, PhysicalTrade, TradePosition
from services.contract_match import master_key
from services.request_cache import get_all_positions
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


def _build_hedge_index():
    """Return {master_key: net_lots} summed across all AGP/AGS futures/options
    positions. Net lots = sum(Long__c + Short__c); Short__c is stored negative,
    so zero net means fully closed — treated as unhedged for display."""
    hedge_lots = {}
    for p in get_all_positions():
        cx = p.contract_xl or ""
        if not (cx.upper().startswith("AGP") or cx.upper().startswith("AGS")):
            continue
        k = master_key(cx)
        if not k:
            continue
        d = p.data or {}
        try:
            net = float(d.get("Long__c") or 0) + float(d.get("Short__c") or 0)
        except (TypeError, ValueError):
            net = 0.0
        hedge_lots[k] = hedge_lots.get(k, 0.0) + net
    return hedge_lots


def _build_groups(rows, hedge_lots=None):
    """Group rows by Venture, preserving first-seen order. Blank ventures go
    into a final 'Unassigned' group."""
    hedge_lots = hedge_lots or {}

    # Attach hedge_key / hedge_lots to every row up front, so downstream
    # aggregation and the template can read straight off each row.
    for r in rows:
        k = master_key(r.data.get("Sub Contract Name"))
        r.hedge_key = k
        r.hedge_lots = hedge_lots.get(k) if k else None

    buckets = OrderedDict()
    for r in rows:
        key = (r.data.get("Venture") or "").strip()
        buckets.setdefault(key, []).append(r)

    groups = []
    unassigned = None
    for venture_id, bucket_rows in buckets.items():
        qtys = [_to_float(i.data.get("Quantity")) for i in bucket_rows]
        total_qty = sum(q for q in qtys if q is not None)

        rows_with_key = [r for r in bucket_rows if r.hedge_key]
        hedged_rows = [r for r in rows_with_key
                       if r.hedge_lots is not None and abs(r.hedge_lots) > 1e-9]
        agp_keys = sorted({r.hedge_key for r in rows_with_key if r.hedge_key.startswith("AGP/")})
        ags_keys = sorted({r.hedge_key for r in rows_with_key if r.hedge_key.startswith("AGS/")})
        # Coverage label drives the chip style in the template.
        if not rows_with_key:
            hedge_state = "none"
        elif len(hedged_rows) == len(rows_with_key):
            hedge_state = "full"
        elif not hedged_rows:
            hedge_state = "unhedged"
        else:
            hedge_state = "partial"

        summary = {
            "venture_id": venture_id or "Unassigned",
            "is_unassigned": venture_id == "",
            "row_count": len(bucket_rows),
            "total_qty": total_qty,
            "mtm_months": sorted({
                (r.data.get("MTM Shipment Month") or "").strip()
                for r in bucket_rows
                if (r.data.get("MTM Shipment Month") or "").strip() not in ("", "-")
            }, key=_mtm_sort_key),
            "commodities": sorted({
                (r.data.get("Commodity: Commodity Name") or "").strip()
                for r in bucket_rows
                if (r.data.get("Commodity: Commodity Name") or "").strip() not in ("", "-")
            }),
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
            "hedge_state": hedge_state,
            "hedged_row_count": len(hedged_rows),
            "keyed_row_count": len(rows_with_key),
            "agp_keys": agp_keys,
            "ags_keys": ags_keys,
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
    hedge_lots = _build_hedge_index()
    groups = _build_groups(rows, hedge_lots=hedge_lots)
    last_synced = rows[0].synced_at if rows else None
    all_columns = list(rows[0].data.keys()) if rows else []
    all_mtm = sorted({
        (r.data.get("MTM Shipment Month") or "").strip()
        for r in rows
        if (r.data.get("MTM Shipment Month") or "").strip()
    }, key=_mtm_sort_key)
    all_commodities = sorted({
        (r.data.get("Commodity: Commodity Name") or "").strip()
        for r in rows
        if (r.data.get("Commodity: Commodity Name") or "").strip()
    })
    return render_template(
        "physical.html",
        groups=groups,
        total_rows=len(rows),
        last_synced=last_synced,
        row_columns=ROW_COLUMNS,
        all_columns=all_columns,
        mtm_months=all_mtm,
        commodities=all_commodities,
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

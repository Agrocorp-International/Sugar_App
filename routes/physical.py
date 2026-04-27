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

# Cost breakdown columns shown in the expanded per-row table.
# "Freight /MT" is a derived column (see _pick_freight) — Fixed Freight wins,
# else Freight MTM, else Freight Original. "Total Cost / MT" is also derived
# from the components below so it stays consistent with the freight pick.
COST_COLUMNS = [
    "Total Cost / MT",
    "Freight /MT",
    "Finance Cost /MT",
    "Insurance /MT",
    "Fumigation /MT",
    "Weight Loss /MT",
    "Estimated Demurrage /MT",
    "Demurrage Recovery /MT",
    "Other Cost / MT",
    "Fixed Costs",
    "Master Contract: Broker Commission",
    "Master Contract: Interco commission",
]

# Per-MT component fields (excluding freight, which has its own pick rule)
# that get summed into our derived Total Cost / MT.
_PER_MT_COMPONENTS = [
    "Finance Cost /MT",
    "Insurance /MT",
    "Fumigation /MT",
    "Weight Loss /MT",
    "Estimated Demurrage /MT",
    "Other Cost / MT",
]


def _pick_freight(d):
    """Return (freight_value_or_None, short_code_or_None).

    Priority: Fixed Freight /MT → Freight MTM /MT → Freight Original /MT.
    A value of 0 counts as "exists" — only None/blank means missing.
    Short codes (Fix / MTM / Org) are displayed in the table and expanded
    to full names in the cell tooltip via FREIGHT_SOURCE_LABELS.
    """
    for field, code in [
        ("Fixed Freight /MT", "Fix"),
        ("Freight MTM /MT", "MTM"),
        ("Freight Original /MT", "Org"),
    ]:
        v = _to_float(d.get(field))
        if v is not None:
            return v, code
    return None, None


FREIGHT_SOURCE_LABELS = {
    "Fix": "Fixed Freight /MT",
    "MTM": "Freight MTM /MT",
    "Org": "Freight Original /MT",
}


def _compute_row_costs(d):
    """Return (freight_per_mt, freight_source, total_per_mt).

    total_per_mt = picked freight + finance + insurance + fumigation +
                   weight_loss + estimated_demurrage - demurrage_recovery + other
    Any missing component is treated as 0. Returns total=None only if no
    cost components at all are present on the row.
    """
    freight, source = _pick_freight(d)
    parts = []
    if freight is not None:
        parts.append(freight)
    for k in _PER_MT_COMPONENTS:
        v = _to_float(d.get(k))
        if v is not None:
            parts.append(v)
    recovery = _to_float(d.get("Demurrage Recovery /MT"))
    if recovery is not None:
        parts.append(-recovery)
    total = sum(parts) if parts else None
    return freight, source, total


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


def _lots_per_mt_divisor(commodity):
    """MT-per-lot divisor for converting physical Quantity (MT) to lot
    equivalents. Bulk Raws hedge in ICE No.11 SB (50.8 MT/lot); whites
    (Refined Sugar / Low Quality Whites) hedge in ICE No.5 LSU (50 MT/lot)."""
    return 50.8 if (commodity or "").strip() == "Bulk Raws" else 50.0


def _build_physical_lots_index(rows):
    """Return {master_key: total_lot_equivalents} summed across all physical
    rows. Same master is often referenced by multiple sub-contracts (e.g.
    SAGS/.../-1, -2, -3) — we sum them so the hedge comparison uses the
    aggregate physical exposure on that master."""
    idx = {}
    for r in rows:
        k = master_key(r.data.get("Sub Contract Name"))
        if not k:
            continue
        qty_mt = _to_float(r.data.get("Quantity"))
        if qty_mt is None or qty_mt <= 0:
            continue
        divisor = _lots_per_mt_divisor(r.data.get("Commodity: Commodity Name"))
        idx[k] = idx.get(k, 0.0) + (qty_mt / divisor)
    return idx


def _hedge_status(hedge_lots, physical_lots):
    """Mirror the /raws Flat-terms hedge classification.

    Compares rounded absolute values, so a single rogue lot doesn't flip
    Fully → Over. Returns one of 'full' / 'partial' / 'over' / 'unhedged',
    or None when there's nothing to compare (no physical qty on master).
    """
    q = abs(round(physical_lots or 0))
    h = abs(round(hedge_lots or 0))
    if q == 0 and h == 0:
        return None
    if h == 0:
        return "unhedged"
    if h < q:
        return "partial"
    if h == q:
        return "full"
    return "over"


def _build_groups(rows, hedge_lots=None, physical_lots=None):
    """Group rows by Venture, preserving first-seen order. Blank ventures go
    into a final 'Unassigned' group."""
    hedge_lots = hedge_lots or {}
    physical_lots = physical_lots or {}

    # Attach hedge_key / hedge_lots, physical-lot rollup, hedge status, and
    # computed cost fields to every row up front, so downstream aggregation
    # and the template can read straight off each row.
    for r in rows:
        k = master_key(r.data.get("Sub Contract Name"))
        r.hedge_key = k
        r.hedge_lots = hedge_lots.get(k) if k else None
        r.physical_lots_master = physical_lots.get(k) if k else None
        r.hedge_status = _hedge_status(r.hedge_lots, r.physical_lots_master) if k else None
        freight, freight_src, total_per_mt = _compute_row_costs(r.data)
        r.freight_per_mt = freight
        r.freight_source = freight_src
        r.total_cost_per_mt = total_per_mt

    buckets = OrderedDict()
    for r in rows:
        key = (r.data.get("Venture") or "").strip()
        buckets.setdefault(key, []).append(r)

    groups = []
    unassigned = None
    for venture_id, bucket_rows in buckets.items():
        qtys = [_to_float(i.data.get("Quantity")) for i in bucket_rows]
        total_qty = sum(q for q in qtys if q is not None)

        # Venture-level cost aggregates: per-MT costs (using our derived
        # Total Cost / MT, which applies the freight priority rule) are
        # summed weighted by row quantity; "Fixed Costs" is absolute.
        total_cost_value = 0.0
        weighted_qty = 0.0
        any_cost = False
        for r2 in bucket_rows:
            qty = _to_float(r2.data.get("Quantity")) or 0
            tc_mt = r2.total_cost_per_mt
            if tc_mt is not None and qty:
                total_cost_value += tc_mt * qty
                weighted_qty += qty
                any_cost = True
            fixed = _to_float(r2.data.get("Fixed Costs"))
            if fixed is not None and fixed != 0:
                total_cost_value += fixed
                any_cost = True
        avg_cost_per_mt = (total_cost_value / weighted_qty) if weighted_qty else None

        rows_with_key = [r for r in bucket_rows if r.hedge_key]
        hedged_rows = [r for r in rows_with_key
                       if r.hedge_lots is not None and abs(r.hedge_lots) > 1e-9]
        agp_keys = sorted({r.hedge_key for r in rows_with_key if r.hedge_key.startswith("AGP/")})
        ags_keys = sorted({r.hedge_key for r in rows_with_key if r.hedge_key.startswith("AGS/")})

        # Worst-case hedge state across all sub-contracts in the venture.
        # Severity order: over > unhedged > partial > full. "Over" is worst
        # because it implies a speculative position beyond the physical book.
        SEVERITY = {"over": 4, "unhedged": 3, "partial": 2, "full": 1}
        statuses = {r.hedge_status for r in rows_with_key if r.hedge_status}
        if not rows_with_key:
            hedge_state = "none"
        elif not statuses:
            hedge_state = "none"
        else:
            hedge_state = max(statuses, key=lambda s: SEVERITY.get(s, 0))
        full_count    = sum(1 for r in rows_with_key if r.hedge_status == "full")
        partial_count = sum(1 for r in rows_with_key if r.hedge_status == "partial")
        over_count    = sum(1 for r in rows_with_key if r.hedge_status == "over")
        unhedged_count = sum(1 for r in rows_with_key if r.hedge_status == "unhedged")

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
            "full_count": full_count,
            "partial_count": partial_count,
            "over_count": over_count,
            "unhedged_count": unhedged_count,
            "total_cost": total_cost_value if any_cost else None,
            "avg_cost_per_mt": avg_cost_per_mt,
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
    physical_lots = _build_physical_lots_index(rows)
    groups = _build_groups(rows, hedge_lots=hedge_lots, physical_lots=physical_lots)
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
        cost_columns=COST_COLUMNS,
        freight_source_labels=FREIGHT_SOURCE_LABELS,
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

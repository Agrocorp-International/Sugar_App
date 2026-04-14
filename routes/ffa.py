import os
from datetime import datetime
from flask import Blueprint, render_template, redirect, url_for, flash, request, jsonify
import openpyxl
from models.db import db, FFATrade, FFASettlement

ffa_bp = Blueprint("ffa", __name__)

EXCEL_PATH = os.path.join(os.path.dirname(os.path.dirname(__file__)), "sugarm2m10.xlsm")


@ffa_bp.route("/ffa")
def index():
    trades = FFATrade.query.order_by(FFATrade.row_index).all()
    settlements = FFASettlement.query.order_by(FFASettlement.row_index).all()
    last_synced = trades[0].synced_at if trades else None
    # Build lookup: {shipment_lower: {size_lower: value}}
    settlement_lookup = {}
    for s in settlements:
        key = s.shipment.strip().lower()
        settlement_lookup[key] = {"smx": s.smx, "pmx": s.pmx}

    total_pnl = None
    for trade in trades:
        s_row = settlement_lookup.get(trade.shipment.strip().lower(), {})
        s_val = s_row.get(trade.size.strip().lower())
        if s_val is not None and trade.trade_price is not None and (trade.long_ is not None or trade.short_ is not None):
            position = (trade.long_ or 0) - (trade.short_ or 0)
            if total_pnl is None:
                total_pnl = 0
            total_pnl += (s_val - trade.trade_price) * position

    return render_template("ffa.html", trades=trades, settlements=settlements,
                           settlement_lookup=settlement_lookup, total_pnl=total_pnl,
                           last_synced=last_synced)


@ffa_bp.route("/ffa/sync", methods=["POST"])
def sync():
    try:
        wb = openpyxl.load_workbook(EXCEL_PATH, read_only=True, keep_vba=True)
        ws = wb["FFA"]
        trade_rows = []
        settlement_rows = []
        for row in ws.iter_rows(values_only=True):
            # Left table: rows where col 0 is a datetime
            date = row[0]
            if isinstance(date, datetime):
                trade_rows.append(row)
            # Right table: rows where col 10 is a non-empty string (month name)
            shipment = row[10] if len(row) > 10 else None
            if isinstance(shipment, str) and shipment.strip():
                smx = row[11] if len(row) > 11 else None
                pmx = row[12] if len(row) > 12 else None
                # Skip header row
                if shipment.lower() != "shipment":
                    settlement_rows.append((shipment.strip(), smx, pmx))
        wb.close()

        now = datetime.utcnow()

        FFATrade.query.delete()
        db.session.bulk_insert_mappings(FFATrade, [
            {
                "row_index": i,
                "trade_date": row[0].strftime("%d %b %Y"),
                "shipment": row[1] or "",
                "size": row[2] or "",
                "long_": row[3],
                "short_": row[4],
                "trade_price": row[6],
                "synced_at": now,
            }
            for i, row in enumerate(trade_rows)
        ])

        FFASettlement.query.delete()
        db.session.bulk_insert_mappings(FFASettlement, [
            {
                "row_index": i,
                "shipment": shipment,
                "smx": smx if isinstance(smx, (int, float)) else None,
                "pmx": pmx if isinstance(pmx, (int, float)) else None,
                "synced_at": now,
            }
            for i, (shipment, smx, pmx) in enumerate(settlement_rows)
        ])

        db.session.commit()
        flash(f"Synced {len(trade_rows)} FFA trades and {len(settlement_rows)} settlement rows.", "success")
    except Exception as e:
        db.session.rollback()
        flash(f"Sync failed: {e}", "danger")
    return redirect(url_for("ffa.index"))


@ffa_bp.route("/ffa/add", methods=["POST"])
def add():
    try:
        trade_date = request.form.get("trade_date", "").strip()
        shipment = request.form.get("shipment", "").strip()
        size = request.form.get("size", "").strip()
        long_val = request.form.get("long_", "").strip()
        short_val = request.form.get("short_", "").strip()
        trade_price = request.form.get("trade_price", "").strip()

        if not trade_date or not shipment or not size:
            flash("Date, Shipment, and Size are required.", "danger")
            return redirect(url_for("ffa.index"))

        parsed_date = datetime.strptime(trade_date, "%Y-%m-%d")
        display_date = parsed_date.strftime("%d %b %Y")

        max_index = db.session.query(db.func.max(FFATrade.row_index)).scalar() or -1
        new_trade = FFATrade(
            row_index=max_index + 1,
            trade_date=display_date,
            shipment=shipment,
            size=size,
            long_=float(long_val) if long_val else None,
            short_=float(short_val) if short_val else None,
            trade_price=float(trade_price) if trade_price else None,
            synced_at=datetime.utcnow(),
        )
        db.session.add(new_trade)
        db.session.commit()
        flash("Trade added successfully.", "success")
    except Exception as e:
        db.session.rollback()
        flash(f"Failed to add trade: {e}", "danger")
    return redirect(url_for("ffa.index"))


@ffa_bp.route("/ffa/api/update", methods=["POST"])
def api_update():
    data = request.get_json()
    if not data:
        return jsonify({"error": "No JSON body"}), 400
    changes = data.get("changes", [])
    if not changes:
        return jsonify({"ok": True, "updated": 0})

    ALLOWED_FIELDS = {"trade_date", "shipment", "size", "long_", "short_", "trade_price"}
    try:
        for change in changes:
            record_id = change.get("record_id")
            field = change.get("field", "").strip()
            value = change.get("value")
            if field not in ALLOWED_FIELDS:
                return jsonify({"error": f"Unknown field: {field}"}), 400
            trade = db.session.get(FFATrade, record_id)
            if not trade:
                continue
            if field == "trade_date":
                for fmt in ("%d %b %Y", "%Y-%m-%d", "%d/%m/%Y"):
                    try:
                        parsed = datetime.strptime(value, fmt)
                        trade.trade_date = parsed.strftime("%d %b %Y")
                        break
                    except ValueError:
                        continue
                else:
                    return jsonify({"error": f"Invalid date: {value}"}), 400
            elif field in ("long_", "short_", "trade_price"):
                setattr(trade, field, float(value) if value not in (None, "") else None)
            else:
                setattr(trade, field, value)
        db.session.commit()
        return jsonify({"ok": True, "updated": len(changes)})
    except Exception as e:
        db.session.rollback()
        return jsonify({"error": str(e)}), 500


@ffa_bp.route("/ffa/api/update-settlement", methods=["POST"])
def api_update_settlement():
    data = request.get_json()
    if not data:
        return jsonify({"error": "No JSON body"}), 400
    changes = data.get("changes", [])
    if not changes:
        return jsonify({"ok": True, "updated": 0})

    ALLOWED_FIELDS = {"smx", "pmx"}
    try:
        for change in changes:
            record_id = change.get("record_id")
            field = change.get("field", "").strip()
            value = change.get("value")
            if field not in ALLOWED_FIELDS:
                return jsonify({"error": f"Unknown field: {field}"}), 400
            row = db.session.get(FFASettlement, record_id)
            if not row:
                continue
            setattr(row, field, float(value) if value not in (None, "") else None)
        db.session.commit()
        return jsonify({"ok": True, "updated": len(changes)})
    except Exception as e:
        db.session.rollback()
        return jsonify({"error": str(e)}), 500


@ffa_bp.route("/ffa/delete/<int:trade_id>", methods=["POST"])
def delete(trade_id):
    trade = FFATrade.query.get_or_404(trade_id)
    try:
        db.session.delete(trade)
        db.session.commit()
        flash("Trade deleted.", "success")
    except Exception as e:
        db.session.rollback()
        flash(f"Failed to delete trade: {e}", "danger")
    return redirect(url_for("ffa.index"))

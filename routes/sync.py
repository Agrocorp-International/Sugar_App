from flask import Blueprint, redirect, url_for, flash, current_app
from models.db import db, TradePosition, SyncLog
from services.salesforce import get_sf_connection, list_custom_objects, fetch_trade_records
from datetime import datetime

sync_bp = Blueprint("sync", __name__)


@sync_bp.route("/sync", methods=["POST"])
def run_sync():
    trade_object = current_app.config.get("SF_TRADE_OBJECT", "")

    try:
        sf = get_sf_connection()

        # If no trade object configured yet, list available custom objects and bail
        if not trade_object:
            custom_objects = list_custom_objects(sf)
            current_app.logger.info(
                "SF_TRADE_OBJECT not set. Available custom objects:\n%s",
                "\n".join(custom_objects),
            )
            flash(
                f"SF_TRADE_OBJECT not set in .env. "
                f"Check app logs for available objects ({len(custom_objects)} found).",
                "warning",
            )
            log = SyncLog(
                synced_at=datetime.utcnow(),
                record_count=0,
                status="warning",
                message=f"SF_TRADE_OBJECT not configured. Found: {', '.join(custom_objects[:10])}",
            )
            db.session.add(log)
            db.session.commit()
            return redirect(url_for("dashboard.index"))

        # Fetch records from Salesforce
        records = fetch_trade_records(sf, trade_object)

        # Clear all existing trades before inserting fresh data
        TradePosition.query.delete()

        # Insert all records
        count = 0
        for rec in records:
            sf_id = rec.get("Id")
            if not sf_id:
                continue
            strategy_str = rec.get('Strategy__c') or ''
            parts = strategy_str.split('-', 3)  # maxsplit=3: protects against dashes inside Contract XL
            if len(parts) == 4:
                parsed_instrument  = parts[0].strip()
                parsed_spread      = parts[1].strip()
                parsed_contract_xl = parts[2].strip()
                parsed_book        = parts[3].strip()
            else:
                parsed_instrument  = None
                parsed_spread      = None
                parsed_contract_xl = None
                parsed_book        = None
                if strategy_str:
                    current_app.logger.warning(
                        f"Invalid Strategy__c format for {sf_id}: '{strategy_str}'"
                    )
            db.session.add(TradePosition(
                sf_id=sf_id,
                name=rec.get("Name", ""),
                data=rec,
                last_synced_at=datetime.utcnow(),
                instrument=parsed_instrument,
                spread=parsed_spread,
                contract_xl=parsed_contract_xl,
                book_parsed=parsed_book,
            ))
            count += 1

        log = SyncLog(
            synced_at=datetime.utcnow(),
            record_count=count,
            status="success",
            message=f"Synced {count} records from {trade_object}",
        )
        db.session.add(log)
        db.session.commit()
        flash(f"Synced {count} records from Salesforce.", "success")

    except Exception as e:
        db.session.rollback()
        log = SyncLog(
            synced_at=datetime.utcnow(),
            record_count=0,
            status="error",
            message=str(e),
        )
        db.session.add(log)
        db.session.commit()
        flash(f"Sync failed: {e}", "danger")

    return redirect(url_for("dashboard.index"))

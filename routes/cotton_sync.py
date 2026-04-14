from datetime import datetime
from flask import Blueprint, redirect, url_for, flash, current_app
from models.db import db
from models.cotton import CottonTradePosition, CottonSyncLog
from services.salesforce import get_sf_connection, fetch_trade_records

cotton_sync_bp = Blueprint("cotton_sync", __name__)


@cotton_sync_bp.route("/sync", methods=["POST"])
def run_sync():
    # Contract: full refresh (truncate + insert), mirroring sugar sync behavior.
    # Do not switch to upsert without explicit product discussion.
    trade_object = current_app.config.get("SF_TRADE_OBJECT", "")

    try:
        sf = get_sf_connection()

        if not trade_object:
            flash("SF_TRADE_OBJECT not set in .env.", "warning")
            return redirect(url_for("cotton_dashboard.index"))

        records = fetch_trade_records(sf, trade_object, commodity_names=['Cotton'])

        CottonTradePosition.query.delete()

        count = 0
        for rec in records:
            sf_id = rec.get("Id")
            if not sf_id:
                continue
            # Cotton Strategy__c is 5-part: Instrument-Spread-ContractXL-Book-Region
            strategy_str = rec.get('Strategy__c') or ''
            parts = strategy_str.split('-', 4)
            if len(parts) == 5:
                parsed_instrument  = parts[0].strip()
                parsed_spread      = parts[1].strip()
                parsed_contract_xl = parts[2].strip()
                parsed_book        = parts[3].strip()
                parsed_region      = parts[4].strip()
            else:
                parsed_instrument = parsed_spread = parsed_contract_xl = None
                parsed_book = parsed_region = None
                if strategy_str:
                    current_app.logger.warning(
                        f"Invalid cotton Strategy__c format for {sf_id}: '{strategy_str}'"
                    )
            db.session.add(CottonTradePosition(
                sf_id=sf_id,
                name=rec.get("Name", ""),
                data=rec,
                last_synced_at=datetime.utcnow(),
                instrument=parsed_instrument,
                spread=parsed_spread,
                contract_xl=parsed_contract_xl,
                book_parsed=parsed_book,
                region=parsed_region,
            ))
            count += 1

        db.session.add(CottonSyncLog(
            synced_at=datetime.utcnow(),
            record_count=count,
            status="success",
            message=f"Synced {count} cotton records from {trade_object}",
        ))
        db.session.commit()
        flash(f"Synced {count} cotton records from Salesforce.", "success")

    except Exception as e:
        db.session.rollback()
        db.session.add(CottonSyncLog(
            synced_at=datetime.utcnow(),
            record_count=0,
            status="error",
            message=str(e),
        ))
        db.session.commit()
        flash(f"Cotton sync failed: {e}", "danger")

    return redirect(url_for("cotton_dashboard.index"))

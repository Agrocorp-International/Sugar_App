import re
from flask import Blueprint, redirect, url_for, flash, current_app
from models.db import db, TradePosition, SyncLog
from services.salesforce import get_sf_connection, list_custom_objects, fetch_trade_records
from services.neon import build_dedup_key
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
        records = fetch_trade_records(sf, trade_object, commodity_names=['ICE Raw Sugar', 'LDN Sugar #5'])

        # Clear all existing trades before inserting fresh data
        TradePosition.query.delete()
        db.session.info.setdefault("_cache_bump_models", set()).add(TradePosition)

        # Insert all records
        count = 0
        for rec in records:
            sf_id = rec.get("Id")
            if not sf_id:
                continue
            strategy_str = rec.get('Strategy__c') or ''
            parts = strategy_str.split('-', 4)  # maxsplit=4: supports 4- and 5-part formats
            if len(parts) >= 4:
                parsed_instrument  = parts[0].strip()
                parsed_spread      = parts[1].strip()
                parsed_contract_xl = parts[2].strip()
                parsed_book        = parts[3].strip()
                bf_tag = parts[4].strip() if len(parts) == 5 else ''
                bf_match = re.search(r'BF=([\d.]+)', bf_tag)
                parsed_bf = float(bf_match.group(1)) if bf_match else None
            else:
                parsed_instrument  = None
                parsed_spread      = None
                parsed_contract_xl = None
                parsed_book        = None
                parsed_bf          = None
                if strategy_str:
                    current_app.logger.warning(
                        f"Invalid Strategy__c format for {sf_id}: '{strategy_str}'"
                    )
            # Canonical dedup key — same formula Neon sync uses, so cross-source
            # lookup is a single indexed equality on TradePosition.dedup_key.
            dedup_key = build_dedup_key(
                trade_date=rec.get("Trade_Date__c") or "",
                contract=rec.get("Contract__c") or "",
                account=rec.get("Account_No__c") or "",
                price=rec.get("Price__c") or 0,
                long_qty=rec.get("Long__c") or 0,
                short_qty=rec.get("Short__c") or 0,
                put_call=rec.get("Put_Call_2__c"),
                strike=rec.get("Strike__c"),
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
                bf_parsed=parsed_bf,
                source="sf",
                dedup_key=dedup_key,
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

"""Neon Markets manual sync — layers today's intraday fills on top of SF.

Triggered by the "Sync from Neon" button on the dashboard. Fetches today +
yesterday (SGT) so late-booked fills, overnight sessions that cross Singapore
midnight, and missed clicks are picked up without historical replay.
"""
import logging
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

from flask import Blueprint, redirect, url_for, flash, current_app
from sqlalchemy.dialects.postgresql import insert

from models.db import db, TradePosition, SyncLog
from services.neon import NeonClient, build_dedup_key

log = logging.getLogger(__name__)

neon_sync_bp = Blueprint("neon_sync", __name__)


@neon_sync_bp.route("/sync-neon", methods=["POST"])
def run_neon_sync():
    sg = ZoneInfo("Asia/Singapore")
    today = datetime.now(sg).date()
    yesterday = today - timedelta(days=1)

    counts = {
        "inserted": 0, "dup_uid": 0, "dup_tuple": 0,
        "non_sugar": 0, "unmapped_acct": 0, "other": 0,
    }
    sample_logged = {"Futures": False, "Option": False}

    try:
        client = NeonClient()
        client.connect()
        log.info("Neon /accounts → %s", client.account_lst)

        for day in (today.isoformat(), yesterday.isoformat()):
            payload = client.get_trades(day)
            raw_trades = payload.get("trades", [])
            log.info("Neon /trades %s → %d raw", day, len(raw_trades))

            mapped_count = 0
            for raw in raw_trades:
                mapped, skip_reason = client.preprocess_trade(raw, day)
                if mapped is None:
                    counts[skip_reason] = counts.get(skip_reason, 0) + 1
                    continue
                mapped_count += 1

                # One-shot sample log per contract type — catches price-scale,
                # contract-month, strike-format, and account-transform issues.
                ctype = mapped["Contract_type__c"]
                if not sample_logged.get(ctype):
                    log.info("Neon sample %s row: %s", ctype, mapped)
                    sample_logged[ctype] = True

                uid = mapped["Id"].removeprefix("NEON_")
                key = build_dedup_key(
                    trade_date=mapped["Trade_Date__c"],
                    contract=mapped["Contract__c"],
                    account=mapped["Account_No__c"],
                    price=mapped["Price__c"],
                    long_qty=mapped["Long__c"],
                    short_qty=mapped["Short__c"],
                    put_call=mapped.get("Put_Call_2__c"),
                    strike=mapped.get("Strike__c"),
                )

                # (a) Hard dedup on Neon UniqueTradeId
                existing_uid = db.session.query(TradePosition.sf_id)\
                    .filter_by(unique_trade_id=uid).first()
                if existing_uid:
                    counts["dup_uid"] += 1
                    continue

                # (b) Cross-source: same fill already present (SF or prior Neon)?
                existing_key = db.session.query(TradePosition.sf_id)\
                    .filter_by(dedup_key=key).first()
                if existing_key:
                    counts["dup_tuple"] += 1
                    continue

                # (b2) Aggregation-tolerant SF-side guard. If SF already has
                # any row at this (date, contract, account, price, direction),
                # trust SF's version and skip — SF often stores one aggregated
                # row per price level while Neon has many sub-fills, so exact
                # qty match in (b) would miss them.
                #
                # Uses .as_string() / .as_float() accessors (generic JSON API,
                # consistent with routes/positions.py etc.) instead of the
                # PG-specific .astext — TradePosition.data is declared as
                # db.JSON, not the PG variant.
                direction_field = "Long__c" if mapped["Long__c"] > 0 else "Short__c"
                sf_price_match = db.session.query(TradePosition.sf_id).filter(
                    TradePosition.source == "sf",
                    TradePosition.data["Trade_Date__c"].as_string() == mapped["Trade_Date__c"],
                    TradePosition.data["Contract__c"].as_string()   == mapped["Contract__c"],
                    TradePosition.data["Account_No__c"].as_string() == mapped["Account_No__c"],
                    TradePosition.data["Price__c"].as_float() == float(mapped["Price__c"]),
                    TradePosition.data[direction_field].as_string().isnot(None),
                ).first()
                if sf_price_match:
                    counts["dup_tuple"] += 1
                    continue

                # (c) Conflict-safe insert — ON CONFLICT DO NOTHING handles
                # double-clicks and concurrent POSTs racing on unique_trade_id.
                stmt = insert(TradePosition).values(
                    sf_id=f"NEON_{uid}",
                    name=mapped.get("Name", ""),
                    data=mapped,
                    source="neon",
                    unique_trade_id=uid,
                    dedup_key=key,
                    last_synced_at=datetime.utcnow(),
                    instrument=None,
                    spread=None,
                    contract_xl=None,
                    book_parsed=None,
                ).on_conflict_do_nothing(index_elements=["unique_trade_id"])
                result = db.session.execute(stmt)
                if result.rowcount:
                    counts["inserted"] += 1
                else:
                    counts["dup_uid"] += 1

            log.info("Neon /trades %s → mapped %d / raw %d", day, mapped_count, len(raw_trades))

        # Mark TradePosition dirty so the dashboard cache bumps.
        db.session.info.setdefault("_cache_bump_models", set()).add(TradePosition)

        msg = (f"Neon: +{counts['inserted']} new, "
               f"{counts['dup_uid']} dup-uid, "
               f"{counts['dup_tuple']} dup-tuple, "
               f"{counts['non_sugar']} non-sugar, "
               f"{counts['unmapped_acct']} unmapped-acct, "
               f"{counts['other']} other")
        db.session.add(SyncLog(
            synced_at=datetime.utcnow(),
            record_count=counts["inserted"],
            status="success",
            message=msg,
        ))
        db.session.commit()
        flash(
            f"Neon sync: {counts['inserted']} new trades, "
            f"{counts['dup_uid']} already ingested, "
            f"{counts['dup_tuple']} likely already in Salesforce.",
            "success",
        )
    except Exception as e:
        db.session.rollback()
        current_app.logger.exception("Neon sync failed")
        db.session.add(SyncLog(
            synced_at=datetime.utcnow(),
            record_count=0,
            status="error",
            message=f"Neon: {e}",
        ))
        db.session.commit()
        flash(f"Neon sync failed: {e}", "danger")

    return redirect(url_for("dashboard.index"))

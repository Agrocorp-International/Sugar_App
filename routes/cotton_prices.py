"""Cotton market prices — parallel to routes/prices.py, keyed on CottonMarketPrice
and CottonWatchedContract. Expiry dates come from routes/cotton_info.py so the
watchlist can display contract expiries and auto-expire stale contracts."""

import os
import re
import json
from datetime import datetime, timedelta, timezone
from services.tradestation import (
    fetch_prices as _ts_fetch_prices,
    fetch_cotton_price_diagnostics,
    _fetch_sofr,
)
from flask import Blueprint, render_template, request, redirect, url_for, flash, jsonify, abort, current_app
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.exc import IntegrityError
from models.db import db, RefreshLog
from models.cotton import CottonMarketPrice, CottonWatchedContract, CottonTradePosition
from routes.cotton_info import (
    CT_FUTURES_EXPIRY_MAP,
    PARSED_CT_OPTIONS,
    compute_ct_futures_expiry,
    compute_ct_option_expiry,
)
from services.exchange_calendar import HOLIDAY_DATES

cotton_prices_bp = Blueprint("cotton_prices", __name__)

_CONTRACT_RE = re.compile(r'^CT[A-Z]\d{2}([CP]\d+)?$')
_OPTION_RE = re.compile(r'^(CT[A-Z]\d{2})([CP])(\d+)$')

# Module-level cached maps (parallel to sugar's routes/prices.py).
OPTIONS_BASE_EXPIRY_MAP = {
    o["contract"].replace(" ", ""): o["expiry"]
    for o in PARSED_CT_OPTIONS
}


def _build_expiry_map():
    """Back-compat pass-through to the cached cotton option expiry map."""
    return OPTIONS_BASE_EXPIRY_MAP


def _diagnostic_contracts_from_request():
    """Return explicit contract query values, or a representative active CT option set."""
    raw_values = request.args.getlist("contracts")
    if not raw_values and request.args.get("contract"):
        raw_values = [request.args.get("contract")]
    contracts = []
    for raw in raw_values:
        contracts.extend(p.strip().upper().replace(" ", "") for p in re.split(r"[,\s]+", raw or ""))
    contracts = [c for c in contracts if c]
    if contracts:
        return list(dict.fromkeys(contracts))

    return [
        wc.contract
        for wc in CottonWatchedContract.query.filter_by(expired=False)
                                             .order_by(CottonWatchedContract.sort_order,
                                                       CottonWatchedContract.created_at)
                                             .all()
        if _OPTION_RE.match(wc.contract)
    ][:5]


@cotton_prices_bp.route("/prices")
def index():
    prices = CottonMarketPrice.query.order_by(CottonMarketPrice.contract).all()
    watched = sorted(
        CottonWatchedContract.query.order_by(
            CottonWatchedContract.sort_order, CottonWatchedContract.created_at
        ).all(),
        key=lambda wc: (1 if re.search(r'[CP]\d+$', wc.contract) else 0,
                        wc.sort_order or 0,
                        wc.created_at),
    )
    price_map = {mp.contract: mp for mp in prices}

    missing_prices = set()
    for pos in CottonTradePosition.query.with_entities(CottonTradePosition.data).all():
        d = pos.data
        ref = (d.get('Contract__c') or '').replace(' ', '')
        put_call = d.get('Put_Call_2__c')
        strike = d.get('Strike__c')
        key = f"{ref}{put_call[0]}{int(round(strike * 100))}" if put_call and strike is not None else ref
        if key and key not in price_map:
            missing_prices.add(key)
    missing_prices = sorted(missing_prices)

    expiry_map = {}
    for wc in watched:
        c = wc.contract
        m = _OPTION_RE.match(c)
        if m:
            base = m.group(1)
            expiry_map[c] = OPTIONS_BASE_EXPIRY_MAP.get(base) or compute_ct_option_expiry(base)
        else:
            expiry_map[c] = CT_FUTURES_EXPIRY_MAP.get(c) or compute_ct_futures_expiry(c)

    # Auto-expire contracts whose expiry date has passed.
    # For options, also zero Sett-1 / Delta-1 / IV-1 once on the active->expired
    # transition (the contract no longer trades). Don't re-zero on later page
    # loads so manual price entry on the Expired table can persist.
    today = datetime.utcnow().date()
    dirty = False
    for wc in watched:
        exp = expiry_map.get(wc.contract)
        if not (exp and exp < today):
            continue
        if not wc.expired:
            wc.expired = True
            dirty = True
            if _OPTION_RE.match(wc.contract):
                mp = price_map.get(wc.contract)
                if mp and (mp.settlement or mp.delta or mp.iv):
                    mp.settlement = 0.0
                    mp.delta = 0.0
                    mp.iv = 0.0
    if dirty:
        db.session.commit()

    pricing_date = db.session.query(db.func.max(CottonMarketPrice.sett_date)).scalar()
    sgt = timezone(timedelta(hours=8))

    def _to_sgt(dt):
        if dt is None:
            return None
        return dt.replace(tzinfo=timezone.utc).astimezone(sgt)

    last_sett_sgt = _to_sgt(db.session.query(db.func.max(CottonMarketPrice.sett_fetched_at)).scalar())
    last_live_sgt = _to_sgt(db.session.query(db.func.max(CottonMarketPrice.live_fetched_at)).scalar())

    return render_template(
        "cotton/prices.html",
        watched=watched, price_map=price_map,
        missing_prices=missing_prices, expiry_map=expiry_map,
        pricing_date=pricing_date,
        last_sett_sgt=last_sett_sgt,
        last_live_sgt=last_live_sgt,
    )


@cotton_prices_bp.route("/prices/diagnostics")
def diagnostics():
    """Read-only cotton IV/delta reconciliation endpoint.

    Usage:
      /cotton/prices/diagnostics?contracts=CTK26C6900,CTN26P6500

    If no contracts are supplied, the first five active watched cotton options
    are diagnosed.
    """
    contracts = _diagnostic_contracts_from_request()
    result = fetch_cotton_price_diagnostics(
        contracts,
        internal_expiry_map=OPTIONS_BASE_EXPIRY_MAP,
    )
    current_app.logger.info(
        "cotton_price_diagnostics contracts=%s result=%s",
        contracts,
        json.dumps(result, default=str)[:4000],
    )
    return jsonify(result)


def _run_sett1_fetch(include_live=False):
    """Sugar-style Sett-1 fetch path for cotton.

    Fetches all active watchlist contracts, archives Sett-1 -> Sett-2 only when
    TradeStation publishes a newer settlement date, and stamps separate fetch
    timestamps for Sett-1 and Live.
    """
    contracts = [
        wc.contract
        for wc in CottonWatchedContract.query.filter_by(expired=False)
                                             .order_by(CottonWatchedContract.sort_order,
                                                       CottonWatchedContract.created_at)
                                             .all()
    ]
    expected = len(contracts)
    if not contracts:
        return {"expected": 0, "fetched": 0, "errors": [], "sett_date": None, "archived": False}

    results, errors, sett_date = _ts_fetch_prices(contracts)
    archived = False

    if results:
        if sett_date is not None:
            old_sett_date = db.session.query(db.func.max(CottonMarketPrice.sett_date)).scalar()
            if old_sett_date is not None and sett_date > old_sett_date:
                db.session.execute(
                    db.update(CottonMarketPrice).values(
                        settlement2=CottonMarketPrice.settlement,
                        delta2=CottonMarketPrice.delta,
                    )
                )
                archived = True

        now_utc = datetime.utcnow()
        for r in results:
            r["sett_fetched_at"] = now_utc
            if include_live:
                r["live_fetched_at"] = now_utc

        stmt = pg_insert(CottonMarketPrice).values(results)
        update_cols = {
            "settlement": stmt.excluded.settlement,
            "delta": stmt.excluded.delta,
            "iv": stmt.excluded.iv,
            "sett_date": stmt.excluded.sett_date,
            "fetched_at": stmt.excluded.fetched_at,
            "sett_fetched_at": stmt.excluded.sett_fetched_at,
        }
        if include_live:
            update_cols.update({
                "live_price": stmt.excluded.live_price,
                "live_iv": stmt.excluded.live_iv,
                "live_delta": stmt.excluded.live_delta,
                "live_fetched_at": stmt.excluded.live_fetched_at,
            })
        stmt = stmt.on_conflict_do_update(index_elements=["contract"], set_=update_cols)
        db.session.execute(stmt)
        db.session.commit()

    return {
        "expected": expected,
        "fetched": len(results),
        "errors": errors,
        "sett_date": sett_date,
        "archived": archived,
    }


@cotton_prices_bp.route("/prices/fetch", methods=["POST"])
def fetch_tradestation():
    mode = request.form.get("mode", "all")

    if mode == "sett1":
        try:
            result = _run_sett1_fetch()
        except Exception as e:
            flash(f"TradeStation fetch error: {e}", "danger")
            return redirect(url_for("cotton_prices.index"))
        if result["expected"] == 0:
            flash("No active cotton contracts in watchlist.", "warning")
            return redirect(url_for("cotton_prices.index"))
        if result["fetched"] > 0:
            r, sofr_date = _fetch_sofr()
            sofr_label = f"{r*100:.3f}% (as of {sofr_date})" if sofr_date else f"{r*100:.3f}% (fallback)"
            sett_label = result["sett_date"].strftime('%d %b %Y') if result["sett_date"] else "unknown"
            flash(f"Fetched {result['fetched']} cotton price(s). Pricing date: {sett_label}. SOFR = {sofr_label}", "success")
        else:
            flash("No cotton prices returned from TradeStation.", "warning")
        for msg in result["errors"]:
            flash(msg, "warning")
        return redirect(url_for("cotton_prices.index"))

    contracts = [
        wc.contract
        for wc in CottonWatchedContract.query.filter_by(expired=False)
                                             .order_by(CottonWatchedContract.sort_order,
                                                       CottonWatchedContract.created_at)
                                             .all()
    ]
    if not contracts:
        flash("No active cotton contracts in watchlist.", "warning")
        return redirect(url_for("cotton_prices.index"))

    try:
        results, errors, sett_date = _ts_fetch_prices(contracts)
    except Exception as e:
        flash(f"TradeStation fetch error: {e}", "danger")
        return redirect(url_for("cotton_prices.index"))

    if results:
        if mode == "all" and sett_date is not None:
            old_sett_date = db.session.query(db.func.max(CottonMarketPrice.sett_date)).scalar()
            if old_sett_date is not None and sett_date > old_sett_date:
                db.session.execute(
                    db.update(CottonMarketPrice).values(
                        settlement2=CottonMarketPrice.settlement,
                        delta2=CottonMarketPrice.delta,
                    )
                )

        now_utc = datetime.utcnow()
        for r in results:
            if mode == "all":
                r["sett_fetched_at"] = now_utc
            if mode in ("live", "all"):
                r["live_fetched_at"] = now_utc

        stmt = pg_insert(CottonMarketPrice).values(results)
        if mode == "live":
            update_cols = {
                "live_price": stmt.excluded.live_price,
                "live_iv": stmt.excluded.live_iv,
                "live_delta": stmt.excluded.live_delta,
                "sett_date": stmt.excluded.sett_date,
                "fetched_at": stmt.excluded.fetched_at,
                "live_fetched_at": stmt.excluded.live_fetched_at,
            }
        else:
            update_cols = {
                "settlement": stmt.excluded.settlement,
                "delta": stmt.excluded.delta,
                "iv": stmt.excluded.iv,
                "live_price": stmt.excluded.live_price,
                "live_iv": stmt.excluded.live_iv,
                "live_delta": stmt.excluded.live_delta,
                "sett_date": stmt.excluded.sett_date,
                "fetched_at": stmt.excluded.fetched_at,
                "sett_fetched_at": stmt.excluded.sett_fetched_at,
                "live_fetched_at": stmt.excluded.live_fetched_at,
            }
        stmt = stmt.on_conflict_do_update(index_elements=["contract"], set_=update_cols)
        db.session.execute(stmt)
        db.session.commit()
        r, sofr_date = _fetch_sofr()
        sofr_label = f"{r*100:.3f}% (as of {sofr_date})" if sofr_date else f"{r*100:.3f}% (fallback)"
        sett_label = sett_date.strftime('%d %b %Y') if sett_date else "unknown"
        flash(f"Fetched {len(results)} cotton price(s). Pricing date: {sett_label}. SOFR = {sofr_label}", "success")
    else:
        flash("No cotton prices returned from TradeStation.", "warning")

    for msg in errors:
        flash(msg, "warning")

    return redirect(url_for("cotton_prices.index"))


def _prices_target_utc(now_utc):
    """Canonical primary-tick target (08:00 SGT = 00:00 UTC)."""
    now_sgt = now_utc + timedelta(hours=8)
    target_sgt = now_sgt.replace(hour=8, minute=0, second=0, microsecond=0)
    if now_sgt < target_sgt:
        target_sgt -= timedelta(days=1)
    return target_sgt - timedelta(hours=8)


@cotton_prices_bp.route("/prices/tick", methods=["POST"])
def prices_tick():
    """Cron entrypoint for cotton's daily EOD Sett-1 pull."""
    expected_key = os.getenv("SNAPSHOT_CRON_KEY")
    if not expected_key or request.headers.get("X-Cron-Key") != expected_key:
        abort(403)
    now_utc = datetime.utcnow()
    now_sgt = (now_utc + timedelta(hours=8)).date()
    if now_sgt.weekday() >= 5:  # skip Sat/Sun SGT — ICE closed
        return jsonify({"skipped": "weekend"}), 200
    if now_sgt in HOLIDAY_DATES:  # skip NYSE holidays — ICE closed
        return jsonify({"skipped": "holiday"}), 200
    target_utc = _prices_target_utc(now_utc)
    delay = int((now_utc - target_utc).total_seconds())
    try:
        result = _run_sett1_fetch(include_live=True)
        current_app.logger.info(
            "cotton_prices_tick expected=%d fetched=%d errors=%d archived=%s sett_date=%s",
            result["expected"], result["fetched"], len(result["errors"]),
            result["archived"], result["sett_date"],
        )
        detail = (f"cotton expected={result['expected']} fetched={result['fetched']} "
                  f"errors={len(result['errors'])} archived={result['archived']} "
                  f"sett_date={result['sett_date'].isoformat() if result['sett_date'] else None}")
        try:
            db.session.add(RefreshLog(
                kind='cotton_prices', slot=None,
                scheduled_for=target_utc, fired_at=now_utc,
                delay_seconds=delay,
                status='success' if not result["errors"] else 'partial',
                detail=detail,
            ))
            db.session.commit()
        except Exception:
            db.session.rollback()
        return jsonify({
            "expected": result["expected"],
            "fetched": result["fetched"],
            "errors": result["errors"],
            "sett_date": result["sett_date"].isoformat() if result["sett_date"] else None,
            "archived": result["archived"],
        })
    except Exception as e:
        current_app.logger.exception("cotton_prices_tick failed")
        db.session.rollback()
        try:
            db.session.add(RefreshLog(
                kind='cotton_prices', slot=None,
                scheduled_for=target_utc, fired_at=now_utc,
                delay_seconds=delay, status='error', detail=str(e)[:500],
            ))
            db.session.commit()
        except Exception:
            db.session.rollback()
        return jsonify({"error": str(e)}), 500


@cotton_prices_bp.route("/prices/clear", methods=["POST"])
def clear():
    expired = [wc.contract for wc in CottonWatchedContract.query.filter_by(expired=True).all()]
    CottonWatchedContract.query.filter_by(expired=False).delete()
    if expired:
        CottonMarketPrice.query.filter(~CottonMarketPrice.contract.in_(expired)).delete(synchronize_session=False)
    else:
        CottonMarketPrice.query.delete()
    db.session.commit()
    flash("Active cotton contracts and prices cleared.", "success")
    return redirect(url_for("cotton_prices.index"))


@cotton_prices_bp.route("/prices/archive", methods=["POST"])
def archive():
    db.session.execute(
        db.update(CottonMarketPrice).values(
            settlement2=CottonMarketPrice.settlement,
            delta2=CottonMarketPrice.delta,
        )
    )
    db.session.commit()
    flash("Cotton Sett-1 and Delta-1 copied to Sett-2 and Delta-2.", "success")
    return redirect(url_for("cotton_prices.index"))


def _next_sort_order():
    return (db.session.query(db.func.max(CottonWatchedContract.sort_order)).scalar() or 0) + 1


@cotton_prices_bp.route("/prices/contracts/add", methods=["POST"])
def contracts_add():
    contract = (request.form.get("contract") or "").strip().upper().replace(' ', '')
    if contract:
        if not _CONTRACT_RE.match(contract):
            flash(f"'{contract}' is not a valid cotton contract. Expected format: CTH26 or CTH26C8000.", "danger")
        else:
            try:
                db.session.add(CottonWatchedContract(contract=contract, sort_order=_next_sort_order()))
                db.session.commit()
            except IntegrityError:
                db.session.rollback()
                flash(f"{contract} is already in the watchlist.", "warning")
    return redirect(url_for("cotton_prices.index"))


@cotton_prices_bp.route("/prices/contracts/bulk_add", methods=["POST"])
def contracts_bulk_add():
    raw = request.form.get("contracts", "")
    parts = re.split(r'[,\n\r]+', raw)
    added = 0
    duplicates = []
    invalid = []
    for part in parts:
        contract = part.strip().upper().replace(' ', '')
        if not contract:
            continue
        if not _CONTRACT_RE.match(contract):
            invalid.append(contract)
            continue
        try:
            db.session.add(CottonWatchedContract(contract=contract, sort_order=_next_sort_order()))
            db.session.commit()
            added += 1
        except IntegrityError:
            db.session.rollback()
            duplicates.append(contract)
    if added:
        flash(f"Added {added} cotton contract(s).", "success")
    if duplicates:
        flash(f"Already in watchlist: {', '.join(duplicates)}.", "warning")
    if invalid:
        flash(f"Invalid format (expected CTH26 or CTH26C8000): {', '.join(invalid)}.", "danger")
    return redirect(url_for("cotton_prices.index"))


@cotton_prices_bp.route("/prices/contracts/list")
def contracts_list():
    contracts = (CottonWatchedContract.query
                 .filter_by(expired=False)
                 .order_by(CottonWatchedContract.sort_order, CottonWatchedContract.created_at)
                 .all())
    return jsonify({"contracts": [wc.contract for wc in contracts]})


@cotton_prices_bp.route("/prices/contracts/reorder", methods=["POST"])
def contracts_reorder():
    ids = request.json.get("ids", [])
    for i, id_ in enumerate(ids):
        CottonWatchedContract.query.filter_by(id=id_).update({"sort_order": i})
    db.session.commit()
    return jsonify({"ok": True})


@cotton_prices_bp.route("/prices/contracts/delete/<int:id>", methods=["POST"])
def contracts_delete(id):
    wc = CottonWatchedContract.query.get_or_404(id)
    CottonMarketPrice.query.filter_by(contract=wc.contract).delete()
    db.session.delete(wc)
    db.session.commit()
    return redirect(url_for("cotton_prices.index"))


@cotton_prices_bp.route("/prices/contracts/update_expired_price/<int:id>", methods=["POST"])
def contracts_update_expired_price(id):
    """Manual entry of Sett-1 / Delta-1 / IV-1 for expired contracts that were
    never priced (e.g. added to the watchlist after expiry). Inline-edit hook
    on the Expired Contracts table; intended as a one-off backfill path."""
    wc = CottonWatchedContract.query.get_or_404(id)
    data = request.get_json() or {}

    def _parse(v):
        if v is None or str(v).strip() == "":
            return None
        try:
            return float(v)
        except (ValueError, TypeError):
            return None

    mp = CottonMarketPrice.query.filter_by(contract=wc.contract).first()
    if mp is None:
        mp = CottonMarketPrice(contract=wc.contract, fetched_at=datetime.utcnow())
        db.session.add(mp)

    if "settlement" in data:
        mp.settlement = _parse(data["settlement"])
    if "delta" in data:
        mp.delta = _parse(data["delta"])
    if "iv" in data:
        mp.iv = _parse(data["iv"])
    db.session.commit()
    return jsonify({
        "ok": True,
        "settlement": mp.settlement,
        "delta": mp.delta,
        "iv": mp.iv,
    })


@cotton_prices_bp.route("/prices/contracts/update_sett2/<int:id>", methods=["POST"])
def contracts_update_sett2(id):
    wc = CottonWatchedContract.query.get_or_404(id)
    data = request.get_json()
    s2 = data.get("settlement2")
    d2 = data.get("delta2")

    def _parse(v):
        if v is None or str(v).strip() == "":
            return None
        try:
            return float(v)
        except (ValueError, TypeError):
            return None

    mp = CottonMarketPrice.query.filter_by(contract=wc.contract).first()
    if mp is None:
        mp = CottonMarketPrice(contract=wc.contract, fetched_at=datetime.utcnow())
        db.session.add(mp)
    if "settlement2" in data:
        mp.settlement2 = _parse(s2)
    if "delta2" in data:
        mp.delta2 = _parse(d2)
    db.session.commit()
    return jsonify({"ok": True, "settlement2": mp.settlement2, "delta2": mp.delta2})

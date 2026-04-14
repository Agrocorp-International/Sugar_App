"""Cotton market prices — parallel to routes/prices.py, keyed on CottonMarketPrice
and CottonWatchedContract. Cotton has no info-page calendar yet, so auto-expiry
is not wired; users must mark contracts expired manually (UI shows a banner)."""

import re
from datetime import datetime
from services.tradestation import fetch_prices as _ts_fetch_prices, _fetch_sofr
from flask import Blueprint, render_template, request, redirect, url_for, flash, jsonify
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.exc import IntegrityError
from models.db import db
from models.cotton import CottonMarketPrice, CottonWatchedContract, CottonTradePosition

cotton_prices_bp = Blueprint("cotton_prices", __name__)

_CONTRACT_RE = re.compile(r'^CT[A-Z]\d{2}([CP]\d+)?$')
_OPTION_RE = re.compile(r'^(CT[A-Z]\d{2})([CP])(\d+)$')


def _build_expiry_map():
    """TODO: populate when cotton contract calendar is added (see routes/info.py
    for the sugar analog). For now, return empty — contracts will not auto-expire."""
    return {}


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

    # No auto-expiry for cotton v1 (no contract calendar). Map stays empty.
    expiry_map = {wc.contract: None for wc in watched}

    pricing_date = db.session.query(db.func.max(CottonMarketPrice.sett_date)).scalar()

    return render_template(
        "cotton/prices.html",
        watched=watched, price_map=price_map,
        missing_prices=missing_prices, expiry_map=expiry_map,
        pricing_date=pricing_date,
    )


@cotton_prices_bp.route("/prices/fetch", methods=["POST"])
def fetch_tradestation():
    mode = request.form.get("mode", "all")
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
        if mode in ("sett1", "all") and sett_date is not None:
            old_sett_date = db.session.query(db.func.max(CottonMarketPrice.sett_date)).scalar()
            if old_sett_date is not None and sett_date > old_sett_date:
                db.session.execute(
                    db.update(CottonMarketPrice).values(
                        settlement2=CottonMarketPrice.settlement,
                        delta2=CottonMarketPrice.delta,
                    )
                )

        stmt = pg_insert(CottonMarketPrice).values(results)
        if mode == "sett1":
            update_cols = {
                "settlement": stmt.excluded.settlement,
                "delta":      stmt.excluded.delta,
                "iv":         stmt.excluded.iv,
                "sett_date":  stmt.excluded.sett_date,
                "fetched_at": stmt.excluded.fetched_at,
            }
        elif mode == "live":
            update_cols = {
                "live_price": stmt.excluded.live_price,
                "live_iv":    stmt.excluded.live_iv,
                "live_delta": stmt.excluded.live_delta,
                "sett_date":  stmt.excluded.sett_date,
                "fetched_at": stmt.excluded.fetched_at,
            }
        else:
            update_cols = {
                "settlement": stmt.excluded.settlement,
                "delta":      stmt.excluded.delta,
                "iv":         stmt.excluded.iv,
                "live_price": stmt.excluded.live_price,
                "live_iv":    stmt.excluded.live_iv,
                "live_delta": stmt.excluded.live_delta,
                "sett_date":  stmt.excluded.sett_date,
                "fetched_at": stmt.excluded.fetched_at,
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

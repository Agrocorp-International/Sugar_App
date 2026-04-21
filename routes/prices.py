import re
from datetime import datetime, timedelta, timezone
from services.tradestation import fetch_prices as _ts_fetch_prices, _fetch_sofr
from flask import Blueprint, render_template, request, redirect, url_for, flash, jsonify
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.exc import IntegrityError
from models.db import db, MarketPrice, WatchedContract, TradePosition
from routes.info import PARSED_FUTURES, PARSED_SW_FUTURES, PARSED_OPTIONS

prices_bp = Blueprint("prices", __name__)

_CONTRACT_RE = re.compile(r'^S[BW][A-Z]\d{2}([CP]\d+)?$')
_OPTION_RE = re.compile(r'^(S[BW][A-Z]\d{2})([CP])(\d+)$')


# Module-level cache: static relative to holidays, recomputed only at import.
# OPTIONS_BASE_EXPIRY_MAP maps futures contract code (no spaces) → option expiry
# date, using the option series where contract == underlying (the 'last' series
# per underlying).
OPTIONS_BASE_EXPIRY_MAP = {
    o['contract'].replace(' ', ''): o['expiry']
    for o in PARSED_OPTIONS
    if o['contract'].replace(' ', '') == o['underlying'].replace(' ', '')
}
FUTURES_EXPIRY_MAP = {f['contract'].replace(' ', ''): f['expiry'] for f in PARSED_FUTURES}
FUTURES_EXPIRY_MAP.update(
    {f['contract'].replace(' ', ''): f['expiry'] for f in PARSED_SW_FUTURES}
)


def _build_expiry_map():
    """Back-compat pass-through to OPTIONS_BASE_EXPIRY_MAP (cached at import).

    Prefer `OPTIONS_BASE_EXPIRY_MAP` directly in new code.
    """
    return OPTIONS_BASE_EXPIRY_MAP


@prices_bp.route("/prices")
def index():
    prices = MarketPrice.query.order_by(MarketPrice.contract).all()
    watched = sorted(
        WatchedContract.query.order_by(WatchedContract.sort_order, WatchedContract.created_at).all(),
        key=lambda wc: (1 if re.search(r'[CP]\d+$', wc.contract) else 0,
                        wc.sort_order or 0,
                        wc.created_at),
    )
    price_map = {mp.contract: mp for mp in prices}

    missing_prices = set()
    for pos in TradePosition.query.with_entities(TradePosition.data).all():
        d = pos.data
        ref = (d.get('Contract__c') or '').replace(' ', '')
        put_call = d.get('Put_Call_2__c')
        strike = d.get('Strike__c')
        key = f"{ref}{put_call[0]}{int(round(strike * 100))}" if put_call and strike is not None else ref
        if key and key not in price_map:
            missing_prices.add(key)
    missing_prices = sorted(missing_prices)

    # Reuse module-level expiry maps (FUTURES_EXPIRY_MAP, OPTIONS_BASE_EXPIRY_MAP).
    expiry_map = {}
    for wc in watched:
        c = wc.contract
        m = _OPTION_RE.match(c)
        if m:
            expiry_map[c] = OPTIONS_BASE_EXPIRY_MAP.get(m.group(1))
        else:
            expiry_map[c] = FUTURES_EXPIRY_MAP.get(c)

    # Auto-expire contracts whose expiry date has passed.
    # For options, also zero Sett-1 / Delta-1 / IV-1 since the contract no longer trades.
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
                dirty = True
    if dirty:
        db.session.commit()

    # Pricing date: read the actual settlement date stored from last fetch
    pricing_date = db.session.query(db.func.max(MarketPrice.sett_date)).scalar()

    sgt = timezone(timedelta(hours=8))
    def _to_sgt(dt):
        if dt is None:
            return None
        return dt.replace(tzinfo=timezone.utc).astimezone(sgt)

    last_sett_sgt = _to_sgt(db.session.query(db.func.max(MarketPrice.sett_fetched_at)).scalar())
    last_live_sgt = _to_sgt(db.session.query(db.func.max(MarketPrice.live_fetched_at)).scalar())

    return render_template("prices.html", watched=watched, price_map=price_map,
                           missing_prices=missing_prices, expiry_map=expiry_map,
                           pricing_date=pricing_date,
                           last_sett_sgt=last_sett_sgt, last_live_sgt=last_live_sgt)


@prices_bp.route("/prices/fetch", methods=["POST"])
def fetch_tradestation():
    mode = request.form.get("mode", "all")  # sett1 | live | all
    contracts = [
        wc.contract
        for wc in WatchedContract.query.filter_by(expired=False)
                                       .order_by(WatchedContract.sort_order,
                                                 WatchedContract.created_at)
                                       .all()
    ]
    if not contracts:
        flash("No active contracts in watchlist.", "warning")
        return redirect(url_for("prices.index"))

    try:
        results, errors, sett_date = _ts_fetch_prices(contracts)
    except Exception as e:
        flash(f"TradeStation fetch error: {e}", "danger")
        return redirect(url_for("prices.index"))

    if results:
        if mode in ("sett1", "all") and sett_date is not None:
            old_sett_date = db.session.query(db.func.max(MarketPrice.sett_date)).scalar()
            if old_sett_date is not None and sett_date > old_sett_date:
                db.session.execute(
                    db.update(MarketPrice).values(
                        settlement2=MarketPrice.settlement,
                        delta2=MarketPrice.delta,
                    )
                )

        now_utc = datetime.utcnow()
        for r in results:
            if mode in ("sett1", "all"):
                r["sett_fetched_at"] = now_utc
            if mode in ("live", "all"):
                r["live_fetched_at"] = now_utc

        stmt = pg_insert(MarketPrice).values(results)
        if mode == "sett1":
            update_cols = {
                "settlement": stmt.excluded.settlement,
                "delta":      stmt.excluded.delta,
                "iv":         stmt.excluded.iv,
                "sett_date":  stmt.excluded.sett_date,
                "fetched_at": stmt.excluded.fetched_at,
                "sett_fetched_at": stmt.excluded.sett_fetched_at,
            }
        elif mode == "live":
            update_cols = {
                "live_price": stmt.excluded.live_price,
                "live_iv":    stmt.excluded.live_iv,
                "live_delta": stmt.excluded.live_delta,
                "sett_date":  stmt.excluded.sett_date,
                "fetched_at": stmt.excluded.fetched_at,
                "live_fetched_at": stmt.excluded.live_fetched_at,
            }
        else:  # all
            update_cols = {
                "settlement": stmt.excluded.settlement,
                "delta":      stmt.excluded.delta,
                "iv":         stmt.excluded.iv,
                "live_price": stmt.excluded.live_price,
                "live_iv":    stmt.excluded.live_iv,
                "live_delta": stmt.excluded.live_delta,
                "sett_date":  stmt.excluded.sett_date,
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
        flash(f"Fetched {len(results)} price(s). Pricing date: {sett_label}. SOFR = {sofr_label}", "success")
    else:
        flash("No prices returned from TradeStation.", "warning")

    for msg in errors:
        flash(msg, "warning")

    return redirect(url_for("prices.index"))



@prices_bp.route("/prices/clear", methods=["POST"])
def clear():
    expired = [wc.contract for wc in WatchedContract.query.filter_by(expired=True).all()]
    WatchedContract.query.filter_by(expired=False).delete()
    if expired:
        MarketPrice.query.filter(~MarketPrice.contract.in_(expired)).delete(synchronize_session=False)
    else:
        MarketPrice.query.delete()
    db.session.commit()
    flash("Active contracts and prices cleared.", "success")
    return redirect(url_for("prices.index"))


@prices_bp.route("/prices/archive", methods=["POST"])
def archive():
    db.session.execute(
        db.update(MarketPrice).values(
            settlement2=MarketPrice.settlement,
            delta2=MarketPrice.delta,
        )
    )
    db.session.commit()
    flash("Sett-1 and Delta-1 copied to Sett-2 and Delta-2.", "success")
    return redirect(url_for("prices.index"))


def _next_sort_order():
    return (db.session.query(db.func.max(WatchedContract.sort_order)).scalar() or 0) + 1


@prices_bp.route("/prices/contracts/add", methods=["POST"])
def contracts_add():
    contract = (request.form.get("contract") or "").strip().upper().replace(' ', '')
    if contract:
        if not _CONTRACT_RE.match(contract):
            flash(f"'{contract}' is not a valid contract. Expected format: SBH26 or SBH26C1880.", "danger")
        else:
            try:
                db.session.add(WatchedContract(contract=contract, sort_order=_next_sort_order()))
                db.session.commit()
            except IntegrityError:
                db.session.rollback()
                flash(f"{contract} is already in the watchlist.", "warning")
    return redirect(url_for("prices.index"))


@prices_bp.route("/prices/contracts/bulk_add", methods=["POST"])
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
            db.session.add(WatchedContract(contract=contract, sort_order=_next_sort_order()))
            db.session.commit()
            added += 1
        except IntegrityError:
            db.session.rollback()
            duplicates.append(contract)
    if added:
        flash(f"Added {added} contract(s).", "success")
    if duplicates:
        flash(f"Already in watchlist: {', '.join(duplicates)}.", "warning")
    if invalid:
        flash(f"Invalid format (expected SBH26 or SBH26C1880): {', '.join(invalid)}.", "danger")
    return redirect(url_for("prices.index"))


@prices_bp.route("/prices/contracts/list")
def contracts_list():
    contracts = (WatchedContract.query
                 .filter_by(expired=False)
                 .order_by(WatchedContract.sort_order, WatchedContract.created_at)
                 .all())
    return jsonify({"contracts": [wc.contract for wc in contracts]})


@prices_bp.route("/prices/contracts/reorder", methods=["POST"])
def contracts_reorder():
    ids = request.json.get("ids", [])
    for i, id_ in enumerate(ids):
        WatchedContract.query.filter_by(id=id_).update({"sort_order": i})
    db.session.commit()
    return jsonify({"ok": True})


@prices_bp.route("/prices/contracts/delete/<int:id>", methods=["POST"])
def contracts_delete(id):
    wc = WatchedContract.query.get_or_404(id)
    MarketPrice.query.filter_by(contract=wc.contract).delete()
    db.session.delete(wc)
    db.session.commit()
    return redirect(url_for("prices.index"))


@prices_bp.route("/prices/contracts/update_sett2/<int:id>", methods=["POST"])
def contracts_update_sett2(id):
    wc = WatchedContract.query.get_or_404(id)
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

    mp = MarketPrice.query.filter_by(contract=wc.contract).first()
    if mp is None:
        mp = MarketPrice(contract=wc.contract, fetched_at=datetime.utcnow())
        db.session.add(mp)
    if "settlement2" in data:
        mp.settlement2 = _parse(s2)
    if "delta2" in data:
        mp.delta2 = _parse(d2)
    db.session.commit()
    return jsonify({"ok": True, "settlement2": mp.settlement2, "delta2": mp.delta2})

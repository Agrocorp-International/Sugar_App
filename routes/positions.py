from flask import Blueprint, render_template, request, jsonify, redirect, url_for, flash
from routes.strategy_warnings import get_warning_groups
from sqlalchemy import cast, Date
from models.db import db, TradePosition, MarketPrice
from services.request_cache import get_all_market_prices

positions_bp = Blueprint("positions", __name__)

PAGE_SIZE = 40

LOT_MULTIPLIERS = {
    'ICE Raw Sugar': 1120,  # cents/lb × 1120 → USD per lot
    'LDN Sugar #5': 50,     # USD/MT × 50 → USD per lot
}

# Same multipliers keyed by contract prefix (SB=Raws, SW=Whites)
LOT_MULTIPLIERS_BY_PREFIX = {'SB': 1120, 'SW': 50}


def build_contract_key(data):
    """Return the price-lookup key for a trade.

    Futures: Contract__c alone, e.g. 'SBH27'
    Options: Contract__c + Put_Call_2__c + strike×100 as int,
             e.g. 'SBJ26' + 'C' + int(14.50×100) = 'SBJ26C1450'
    This matches ICE XL codes after stripping spaces (e.g. 'SB J26C1450' → 'SBJ26C1450').
    """
    ref = (data.get('Contract__c') or '').replace(' ', '')
    put_call = data.get('Put_Call_2__c')
    strike = data.get('Strike__c')
    if put_call and strike is not None:
        return f"{ref}{put_call[0]}{int(round(strike * 100))}"
    return ref


def compute_maps(positions, source='sett1'):
    """Return (pnl_map, settlement_map, delta_map) dicts keyed by sf_id.

    ``source`` selects sett-1 (default) or live prices/deltas, with silent
    fallback to sett-1 for any contract whose live value is missing.
    The naming of ``settlement_map`` and ``settlement2_map`` is preserved
    for backward compatibility — when source='live', the values are live
    prices but the dict name still says settlement.
    """
    from services.price_source import resolve_price, resolve_delta
    market = {mp.contract: mp for mp in get_all_market_prices()}
    _latest = TradePosition.query.order_by(
        cast(TradePosition.data["Trade_Date__c"].as_string(), Date).desc()
    ).first()
    latest_date = _latest.data.get("Trade_Date__c") if _latest else None
    pnl_map = {}
    pnl_change_map = {}
    settlement_map = {}
    settlement2_map = {}
    delta_map = {}
    for pos in positions:
        key = build_contract_key(pos.data)
        mp = market.get(key)
        mkt = resolve_price(mp, source)
        mkt2 = mp.settlement2 if mp else None
        # Options with no uploaded settlement default to 0
        is_option = bool(pos.data.get('Put_Call_2__c') and pos.data.get('Strike__c') is not None)
        if mkt is None and is_option:
            mkt = 0
        settlement_map[pos.sf_id] = mkt
        settlement2_map[pos.sf_id] = mkt2
        is_futures = not (pos.data.get('Put_Call_2__c') and pos.data.get('Strike__c') is not None)
        delta_val = resolve_delta(mp, source)
        if delta_val is not None:
            delta_map[pos.sf_id] = delta_val
        elif is_futures:
            delta_map[pos.sf_id] = 1.0  # futures delta always 1.0
        else:
            delta_map[pos.sf_id] = None
        trade_price = pos.data.get('Price__c')
        long_ = pos.data.get('Long__c') or 0
        short_ = pos.data.get('Short__c') or 0
        mult = LOT_MULTIPLIERS.get(pos.data.get('Commodity_Name__c', ''))
        if mkt is not None and trade_price is not None and mult:
            pnl_map[pos.sf_id] = (mkt - trade_price) * (long_ + short_) * mult
        else:
            pnl_map[pos.sf_id] = None
        trade_date = pos.data.get('Trade_Date__c')
        if trade_date == latest_date:
            commission = pos.data.get('Broker_Commission__c') or 0
            pnl_change_map[pos.sf_id] = pnl_map[pos.sf_id] + commission if pnl_map[pos.sf_id] is not None else None
        elif mkt is not None and mkt2 is not None and mult:
            pnl_change_map[pos.sf_id] = (mkt - mkt2) * (long_ + short_) * mult
        else:
            pnl_change_map[pos.sf_id] = None
    return pnl_map, pnl_change_map, settlement_map, settlement2_map, delta_map


_SF_STATUS_MAP = {'Unrealised': 'Open', 'Realised': 'Closed'}


@positions_bp.route("/positions")
def index():
    from sqlalchemy import text as sa_text
    from services.price_source import get_price_source
    price_source = get_price_source()
    page = request.args.get("page", 1, type=int)
    date_filter = request.args.get("date_filter", "").strip()
    contract_filter = request.args.get("contract_filter", "").strip()
    price_filter = request.args.get("price_filter", "").strip()
    book_filter = request.args.get("book_filter", "").strip()
    put_call_filter = request.args.get("put_call_filter", "").strip()
    strike_filter = request.args.get("strike_filter", "").strip()
    instrument_filter = request.args.get("instrument_filter", "").strip()
    spread_filter = request.args.get("spread_filter", "").strip()
    status_filter = request.args.get("status_filter", "").strip()
    contract_xl_filter = request.args.get("contract_xl_filter", "").strip()
    trade_id_filter = request.args.get("trade_id_filter", "").strip()
    query = TradePosition.query.order_by(
        cast(TradePosition.data["Trade_Date__c"].as_string(), Date).desc()
    )
    if date_filter:
        query = query.filter(
            cast(TradePosition.data["Trade_Date__c"].as_string(), Date) == date_filter
        )
    if contract_filter:
        query = query.filter(
            TradePosition.data["Contract__c"].as_string().ilike(f"%{contract_filter}%")
        )
    if price_filter:
        try:
            price_val = float(price_filter)
            query = query.filter(
                TradePosition.data["Price__c"].as_float() == price_val
            )
        except ValueError:
            pass
    if book_filter:
        if book_filter == "__empty__":
            query = query.filter(
                sa_text("(data->>'Book__c' IS NULL OR data->>'Book__c' = '')")
            )
        else:
            query = query.filter(
                TradePosition.data["Book__c"].as_string().ilike(f"%{book_filter}%")
            )
    if put_call_filter:
        if put_call_filter == "__empty__":
            query = query.filter(
                sa_text("(data->>'Put_Call_2__c' IS NULL OR data->>'Put_Call_2__c' = '')")
            )
        else:
            query = query.filter(
                TradePosition.data["Put_Call_2__c"].as_string() == put_call_filter
            )
    if strike_filter:
        try:
            strike_val = float(strike_filter)
            query = query.filter(
                TradePosition.data["Strike__c"].as_float() == strike_val
            )
        except ValueError:
            pass
    if instrument_filter:
        if instrument_filter == "__empty__":
            query = query.filter(TradePosition.instrument == None)
        else:
            query = query.filter(TradePosition.instrument == instrument_filter)
    if spread_filter:
        if spread_filter == "__empty__":
            query = query.filter(TradePosition.spread == None)
        else:
            query = query.filter(TradePosition.spread == spread_filter)
    if status_filter:
        query = query.filter(
            TradePosition.data["Realised__c"].as_string() == status_filter
        )
    if contract_xl_filter:
        query = query.filter(TradePosition.contract_xl.ilike(f"%{contract_xl_filter}%"))
    if trade_id_filter:
        query = query.filter(
            TradePosition.data["Trade_Key__c"].as_string() == trade_id_filter
        )
    # Compute total PNL across all filtered rows (for display above table)
    all_filtered = query.all()
    invalid_strategy_count = len(get_warning_groups())
    _all_pnl_map, _, _, _, _all_delta_map = compute_maps(all_filtered, price_source)
    total_pnl = 0
    total_net_pnl = 0
    total_position = None
    total_spread_pos = None
    for _pos in all_filtered:
        _pnl = _all_pnl_map.get(_pos.sf_id)
        if _pnl is not None:
            _comm = float(_pos.data.get('Broker_Commission__c') or 0)
            total_pnl += _pnl
            total_net_pnl += _pnl + _comm
        _delta = _all_delta_map.get(_pos.sf_id)
        _lots = float(_pos.data.get('Long__c') or 0) + float(_pos.data.get('Short__c') or 0)
        if _delta is not None:
            total_position = (total_position or 0) + _delta * _lots
            _instrument = (_pos.instrument or '').strip()
            if _instrument == 'Spread':
                _spread = (_pos.spread or '').strip()
                if _spread:
                    _contract_last3 = (_pos.data.get('Contract__c') or '').replace(' ', '')[-3:]
                    if _contract_last3 != _spread[-3:]:
                        total_spread_pos = (total_spread_pos or 0) + _delta * _lots
    pagination = query.paginate(page=page, per_page=PAGE_SIZE, error_out=False)
    pnl_map, pnl_change_map, settlement_map, settlement2_map, delta_map = compute_maps(pagination.items, price_source)
    book_options = [r[0] for r in db.session.execute(
        sa_text("SELECT DISTINCT data->>'Book__c' FROM sugar_trade_positions WHERE data->>'Book__c' IS NOT NULL AND data->>'Book__c' != '' ORDER BY 1")
    ).fetchall()]
    contract_options = [r[0] for r in db.session.execute(
        sa_text("SELECT DISTINCT data->>'Contract__c' FROM sugar_trade_positions WHERE data->>'Contract__c' IS NOT NULL AND data->>'Contract__c' != '' ORDER BY 1")
    ).fetchall()]
    put_call_options = [r[0] for r in db.session.execute(
        sa_text("SELECT DISTINCT data->>'Put_Call_2__c' FROM sugar_trade_positions WHERE data->>'Put_Call_2__c' IS NOT NULL AND data->>'Put_Call_2__c' != '' ORDER BY 1")
    ).fetchall()]
    strike_options = [r[0] for r in db.session.execute(
        sa_text("SELECT DISTINCT CAST(data->>'Strike__c' AS NUMERIC) FROM sugar_trade_positions WHERE data->>'Strike__c' IS NOT NULL AND data->>'Strike__c' != '' ORDER BY 1")
    ).fetchall()]
    instrument_options = [r[0] for r in db.session.execute(
        sa_text("SELECT DISTINCT instrument FROM sugar_trade_positions WHERE instrument IS NOT NULL AND instrument != '' ORDER BY 1")
    ).fetchall()]
    spread_options = [r[0] for r in db.session.execute(
        sa_text("SELECT DISTINCT spread FROM sugar_trade_positions WHERE spread IS NOT NULL AND spread != '' ORDER BY 1")
    ).fetchall()]
    status_options = [r[0] for r in db.session.execute(
        sa_text("SELECT DISTINCT data->>'Realised__c' FROM sugar_trade_positions WHERE data->>'Realised__c' IS NOT NULL AND data->>'Realised__c' != '' ORDER BY 1")
    ).fetchall()]
    trade_id_options = sorted(
        [r[0] for r in db.session.execute(
            sa_text("SELECT DISTINCT data->>'Trade_Key__c' FROM sugar_trade_positions WHERE data->>'Trade_Key__c' IS NOT NULL AND data->>'Trade_Key__c' != ''")
        ).fetchall()],
        key=lambda x: (0, int(x)) if x.lstrip('-').isdigit() else (1, x)
    )
    return render_template("positions.html", pagination=pagination,
                           pnl_map=pnl_map, pnl_change_map=pnl_change_map,
                           settlement_map=settlement_map, settlement2_map=settlement2_map,
                           delta_map=delta_map, date_filter=date_filter,
                           contract_filter=contract_filter, price_filter=price_filter,
                           book_filter=book_filter, book_options=book_options,
                           contract_options=contract_options,
                           put_call_filter=put_call_filter, put_call_options=put_call_options,
                           strike_filter=strike_filter, strike_options=strike_options,
                           instrument_filter=instrument_filter, instrument_options=instrument_options,
                           spread_filter=spread_filter, spread_options=spread_options,
                           contract_xl_filter=contract_xl_filter,
                           status_filter=status_filter, status_options=status_options,
                           trade_id_filter=trade_id_filter, trade_id_options=trade_id_options,
                           total_pnl=total_pnl, total_net_pnl=total_net_pnl,
                           total_position=total_position, total_spread_pos=total_spread_pos,
                           invalid_strategy_count=invalid_strategy_count,
                           price_source=price_source)


ALLOWED_FIELDS = {
    "Trade_Date__c", "Book__c", "Trader__c", "Broker_Name__c", "Contract__c",
    "Contract_type__c", "Strategy__c", "Long__c", "Short__c", "Price__c",
    "Strike__c", "Put_Call_2__c", "Broker_Commission__c", "Realised__c",
    "Trade_Code__c", "Trade_Key__c", "Trade_Group__c",
    "New_AGP__r.Name", "New_AGS__r.Name",
}
CONTRACT_REF_FIELDS = {"New_AGP__r.Name", "New_AGS__r.Name"}
NUMERIC_FIELDS = {"Long__c", "Short__c", "Price__c", "Strike__c", "Broker_Commission__c"}
STRATEGY_FIELDS = {"instrument", "spread", "contract_xl", "book_parsed"}
BOOK_TO_SF = {"Alpha": "Spec", "Whites": "Hedge", "Raws": "Hedge"}


def _validate_strategy_component(value):
    """Normalize and validate a single Strategy__c component.
    Returns stripped string (may be empty) or raises ValueError.
    """
    v = (value or '').strip()
    if '-' in v:
        raise ValueError(f"Strategy component cannot contain a hyphen: '{v}'")
    return v


@positions_bp.route("/positions/api/update", methods=["POST"])
def api_update():
    data = request.get_json()
    if not data:
        return jsonify({"error": "No JSON body"}), 400
    changes = data.get("changes", [])
    if not changes:
        return jsonify({"ok": True, "updated": 0})
    push_to_sf = data.get("push_to_sf", False)
    try:
        changes_by_record = {}
        strategy_touched_pos = {}  # sf_id -> TradePosition, for reconstruction pass
        contract_xl_touched_ids = set()  # sf_ids where contract_xl was edited

        for change in changes:
            sf_id = change.get("sf_id")
            field = change.get("field", "").strip()
            value = change.get("value")

            pos = TradePosition.query.filter_by(sf_id=sf_id).first()
            if not pos:
                continue

            if field in STRATEGY_FIELDS:
                try:
                    clean = _validate_strategy_component(value)
                except ValueError as e:
                    return jsonify({"error": str(e)}), 400
                setattr(pos, field, clean if clean else None)
                strategy_touched_pos[sf_id] = pos  # cache — no second query later
                if field == "contract_xl":
                    contract_xl_touched_ids.add(sf_id)

            elif field in ALLOWED_FIELDS:
                new_data = dict(pos.data)
                if field in NUMERIC_FIELDS:
                    new_data[field] = float(value) if value not in (None, "") else None
                else:
                    new_data[field] = value
                pos.data = new_data
                if sf_id not in changes_by_record:
                    changes_by_record[sf_id] = {}
                changes_by_record[sf_id][field] = new_data[field]

            else:
                return jsonify({"error": f"Unknown field: {field}"}), 400

        # Reconstruct Strategy__c from the 4 parsed columns.
        # Happens on every save (including local-only) before the commit.
        # Track which sf_ids had book_parsed changed (for Book__c mapping).
        book_touched_ids = {c["sf_id"] for c in changes if c.get("field") == "book_parsed"}
        for sf_id, pos in strategy_touched_pos.items():
            new_strategy = "{}-{}-{}-{}".format(
                pos.instrument or '',
                pos.spread or '',
                pos.contract_xl or '',
                pos.book_parsed or '',
            )
            new_data = dict(pos.data)
            new_data['Strategy__c'] = new_strategy
            if sf_id in book_touched_ids and pos.book_parsed in BOOK_TO_SF:
                new_data['Book__c'] = BOOK_TO_SF[pos.book_parsed]
            pos.data = new_data
            if sf_id not in changes_by_record:
                changes_by_record[sf_id] = {}
            changes_by_record[sf_id]['Strategy__c'] = new_strategy
            if sf_id in book_touched_ids and pos.book_parsed in BOOK_TO_SF:
                changes_by_record[sf_id]['Book__c'] = new_data['Book__c']

        # Validate Long/Short mutual exclusivity before committing
        for pos in db.session.dirty:
            if not isinstance(pos, TradePosition):
                continue
            long_ = pos.data.get("Long__c")
            short_ = pos.data.get("Short__c")
            if long_ not in (None, 0, 0.0, "") and short_ not in (None, 0, 0.0, ""):
                db.session.rollback()
                return jsonify({"error": f"Record {pos.sf_id}: Long and Short cannot both be filled."}), 400
        if not push_to_sf:
            db.session.commit()
            return jsonify({"ok": True, "updated": len(changes), "sf_errors": []})

        # push_to_sf=True: attempt SF first, only commit to DB if all succeed
        sf_errors = []
        try:
            from services.salesforce import get_sf_connection
            sf = get_sf_connection()
            for rec_id, rec_fields in changes_by_record.items():
                trade_fields = {k: v for k, v in rec_fields.items() if k not in CONTRACT_REF_FIELDS}
                ref_fields = {k: v for k, v in rec_fields.items() if k in CONTRACT_REF_FIELDS}
                # Auto-derive SF_Ref from contract_xl and merge AGP/AGS into trade_fields
                sf_ref_local_update = None  # (target_lookup, clear_lookup, target_name_field, clear_name_field, mc_id, sf_ref)
                if rec_id in contract_xl_touched_ids:
                    pos = strategy_touched_pos.get(rec_id)
                    xl_val = pos.contract_xl if pos else ""
                    sf_ref = (xl_val or "").split("_")[0].replace(" ", "")
                    if sf_ref:
                        if sf_ref[:3].upper() == "AGS":
                            target_lookup, clear_lookup = "New_AGS__c", "New_AGP__c"
                            target_name_field, clear_name_field = "New_AGS__r.Name", "New_AGP__r.Name"
                        else:
                            target_lookup, clear_lookup = "New_AGP__c", "New_AGS__c"
                            target_name_field, clear_name_field = "New_AGP__r.Name", "New_AGS__r.Name"
                        try:
                            mc_result = sf.query(f"SELECT Id FROM Master_Contract__c WHERE Name = '{sf_ref}' LIMIT 1")
                        except Exception as e:
                            sf_errors.append(f"{rec_id}: Master Contract lookup for SF_Ref '{sf_ref}' failed: {e}")
                        else:
                            if mc_result["totalSize"] == 0:
                                sf_errors.append(f"{rec_id}: Master Contract '{sf_ref}' (derived from XL Ref) not found in Salesforce")
                            else:
                                mc_id = mc_result["records"][0]["Id"]
                                trade_fields[target_lookup] = mc_id
                                trade_fields[clear_lookup] = None
                                sf_ref_local_update = (target_lookup, clear_lookup, target_name_field, clear_name_field, mc_id, sf_ref)
                    else:
                        # XL_Ref cleared — null out both AGP and AGS
                        trade_fields["New_AGP__c"] = None
                        trade_fields["New_AGS__c"] = None
                        sf_ref_local_update = ("New_AGP__c", "New_AGS__c", "New_AGP__r.Name", "New_AGS__r.Name", None, None)
                if trade_fields:
                    try:
                        sf.Futur__c.update(rec_id, trade_fields)
                    except Exception as e:
                        sf_errors.append(f"{rec_id}: {e}")
                    else:
                        # Update local DB with AGP/AGS if SF push succeeded
                        if sf_ref_local_update:
                            target_lookup, clear_lookup, target_name_field, clear_name_field, mc_id, sf_ref = sf_ref_local_update
                            ref_pos = TradePosition.query.filter_by(sf_id=rec_id).first()
                            if ref_pos:
                                new_data = dict(ref_pos.data)
                                new_data[target_lookup] = mc_id
                                new_data[clear_lookup] = None
                                new_data[target_name_field] = sf_ref
                                new_data[clear_name_field] = None
                                ref_pos.data = new_data
                for ref_field, ref_value in ref_fields.items():
                    new_name = str(ref_value).strip() if ref_value else ""
                    if not new_name:
                        sf_errors.append(f"{rec_id}: contract reference cannot be empty")
                        continue
                    # Determine which lookup field to use based on name prefix
                    if new_name.upper().startswith("AGS"):
                        target_lookup, clear_lookup = "New_AGS__c", "New_AGP__c"
                        target_name_field, clear_name_field = "New_AGS__r.Name", "New_AGP__r.Name"
                    else:
                        target_lookup, clear_lookup = "New_AGP__c", "New_AGS__c"
                        target_name_field, clear_name_field = "New_AGP__r.Name", "New_AGS__r.Name"
                    # Look up Master_Contract__c ID by Name
                    try:
                        mc_result = sf.query(f"SELECT Id FROM Master_Contract__c WHERE Name = '{new_name}' LIMIT 1")
                    except Exception as e:
                        sf_errors.append(f"{rec_id}: Master Contract lookup failed: {e}")
                        continue
                    if mc_result["totalSize"] == 0:
                        sf_errors.append(f"{rec_id}: Master Contract '{new_name}' not found in Salesforce")
                        continue
                    mc_id = mc_result["records"][0]["Id"]
                    # Update the Futur__c lookup fields
                    try:
                        sf.Futur__c.update(rec_id, {target_lookup: mc_id, clear_lookup: None})
                    except Exception as e:
                        sf_errors.append(f"{rec_id}: {e}")
                        continue
                    # Update local DB JSON with new ID and name
                    ref_pos = TradePosition.query.filter_by(sf_id=rec_id).first()
                    if ref_pos:
                        new_data = dict(ref_pos.data)
                        new_data[target_lookup] = mc_id
                        new_data[clear_lookup] = None
                        new_data[target_name_field] = new_name
                        new_data[clear_name_field] = None
                        ref_pos.data = new_data
        except Exception as e:
            sf_errors.append(f"SF connection failed: {e}")

        if sf_errors:
            db.session.rollback()
            return jsonify({"ok": False, "error": "Salesforce update failed:\n" + "\n".join(sf_errors)}), 400

        db.session.commit()
        return jsonify({"ok": True, "updated": len(changes), "sf_errors": []})
    except Exception as e:
        db.session.rollback()
        return jsonify({"error": str(e)}), 500


@positions_bp.route("/positions/delete/<sf_id>", methods=["POST"])
def delete(sf_id):
    pos = TradePosition.query.filter_by(sf_id=sf_id).first_or_404()
    try:
        db.session.delete(pos)
        db.session.commit()
        flash("Position deleted.", "success")
    except Exception as e:
        db.session.rollback()
        flash(f"Failed to delete: {e}", "danger")
    return redirect(url_for("positions.index"))

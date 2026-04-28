from flask import Blueprint, render_template, request, jsonify, redirect, url_for, flash, current_app
from sqlalchemy import cast, Date, or_
from models.db import db
from models.cotton import CottonTradePosition
from services.request_cache import get_all_cotton_market_prices
from urllib.parse import urlencode

cotton_positions_bp = Blueprint("cotton_positions", __name__)


def _multi_arg(name):
    """Read a multi-value query param and strip blanks."""
    return [v.strip() for v in request.args.getlist(name) if v.strip()]

PAGE_SIZE = 40

# ICE Cotton #2: 50,000 lbs × cents/lb ÷ 100 = 500 USD per lot per cent.
LOT_MULTIPLIERS = {'Cotton': 500}
LOT_MULTIPLIERS_BY_PREFIX = {'CT': 500}


def build_contract_key(data):
    """Return the price-lookup key for a cotton trade.

    Futures: Contract__c alone (e.g. 'CTH26').
    Options: Contract__c + Put_Call_2__c + strike×100 as int (e.g. 'CTH26C8000').
    """
    ref = (data.get('Contract__c') or '').replace(' ', '')
    put_call = data.get('Put_Call_2__c')
    strike = data.get('Strike__c')
    if put_call and strike is not None:
        return f"{ref}{put_call[0]}{int(round(strike * 100))}"
    return ref


def compute_maps(positions, source='sett1'):
    """Return (pnl_map, pnl_change_map, settlement_map, settlement2_map, delta_map).

    Mirrors the sugar version but queries CottonMarketPrice and uses cotton's
    lot multipliers.
    """
    from services.price_source import resolve_price, resolve_delta
    market = {mp.contract: mp for mp in get_all_cotton_market_prices()}
    _latest = CottonTradePosition.query.order_by(
        cast(CottonTradePosition.data["Trade_Date__c"].as_string(), Date).desc()
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
        is_option = bool(pos.data.get('Put_Call_2__c') and pos.data.get('Strike__c') is not None)
        if mkt is None and is_option:
            mkt = 0
        settlement_map[pos.sf_id] = mkt
        settlement2_map[pos.sf_id] = mkt2
        is_futures = not is_option
        delta_val = resolve_delta(mp, source)
        if delta_val is not None:
            delta_map[pos.sf_id] = delta_val
        elif is_futures:
            delta_map[pos.sf_id] = 1.0
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
            commission = pos.commission
            pnl_change_map[pos.sf_id] = pnl_map[pos.sf_id] + commission if pnl_map[pos.sf_id] is not None else None
        elif mkt is not None and mkt2 is not None and mult:
            pnl_change_map[pos.sf_id] = (mkt - mkt2) * (long_ + short_) * mult
        else:
            pnl_change_map[pos.sf_id] = None
    return pnl_map, pnl_change_map, settlement_map, settlement2_map, delta_map


@cotton_positions_bp.route("/positions")
def index():
    from sqlalchemy import text as sa_text
    from services.price_source import get_price_source
    price_source = get_price_source()
    page = request.args.get("page", 1, type=int)
    date_filter        = request.args.get("date_filter", "").strip()
    price_filter       = request.args.get("price_filter", "").strip()
    contract_xl_filter = request.args.get("contract_xl_filter", "").strip()
    contract_filter    = _multi_arg("contract_filter")
    book_filter        = _multi_arg("book_filter")
    region_filter      = _multi_arg("region_filter")
    put_call_filter    = _multi_arg("put_call_filter")
    strike_filter      = _multi_arg("strike_filter")
    instrument_filter  = _multi_arg("instrument_filter")
    spread_filter      = _multi_arg("spread_filter")
    status_filter      = _multi_arg("status_filter")
    trade_id_filter    = _multi_arg("trade_id_filter")

    def page_url(page_num):
        params = request.args.to_dict(flat=False)
        params["page"] = [page_num]
        return "?" + urlencode(params, doseq=True)

    query = CottonTradePosition.query.order_by(
        cast(CottonTradePosition.data["Trade_Date__c"].as_string(), Date).desc()
    )
    if date_filter:
        query = query.filter(
            cast(CottonTradePosition.data["Trade_Date__c"].as_string(), Date) == date_filter
        )
    if contract_filter:
        query = query.filter(
            CottonTradePosition.data["Contract__c"].as_string().in_(contract_filter)
        )
    if price_filter:
        try:
            price_val = float(price_filter)
            query = query.filter(CottonTradePosition.data["Price__c"].as_float() == price_val)
        except ValueError:
            pass
    if book_filter:
        exact = [v for v in book_filter if v != "__empty__"]
        clauses = []
        if exact:
            clauses.append(CottonTradePosition.data["Book__c"].as_string().in_(exact))
        if "__empty__" in book_filter:
            clauses.append(sa_text("(data->>'Book__c' IS NULL OR data->>'Book__c' = '')"))
        query = query.filter(or_(*clauses))
    if region_filter:
        exact = [v for v in region_filter if v != "__empty__"]
        clauses = []
        if exact:
            clauses.append(CottonTradePosition.region.in_(exact))
        if "__empty__" in region_filter:
            clauses.append(CottonTradePosition.region == None)
        query = query.filter(or_(*clauses))
    if put_call_filter:
        exact = [v for v in put_call_filter if v != "__empty__"]
        clauses = []
        if exact:
            clauses.append(CottonTradePosition.data["Put_Call_2__c"].as_string().in_(exact))
        if "__empty__" in put_call_filter:
            clauses.append(sa_text("(data->>'Put_Call_2__c' IS NULL OR data->>'Put_Call_2__c' = '')"))
        query = query.filter(or_(*clauses))
    if strike_filter:
        strike_vals = []
        for v in strike_filter:
            try:
                strike_vals.append(float(v))
            except ValueError:
                pass
        if strike_vals:
            query = query.filter(
                CottonTradePosition.data["Strike__c"].as_float().in_(strike_vals)
            )
    if instrument_filter:
        exact = [v for v in instrument_filter if v != "__empty__"]
        clauses = []
        if exact:
            clauses.append(CottonTradePosition.instrument.in_(exact))
        if "__empty__" in instrument_filter:
            clauses.append(CottonTradePosition.instrument == None)
        query = query.filter(or_(*clauses))
    if spread_filter:
        exact = [v for v in spread_filter if v != "__empty__"]
        clauses = []
        if exact:
            clauses.append(CottonTradePosition.spread.in_(exact))
        if "__empty__" in spread_filter:
            clauses.append(CottonTradePosition.spread == None)
        query = query.filter(or_(*clauses))
    if status_filter:
        query = query.filter(
            CottonTradePosition.data["Realised__c"].as_string().in_(status_filter)
        )
    if contract_xl_filter:
        query = query.filter(CottonTradePosition.contract_xl.ilike(f"%{contract_xl_filter}%"))
    if trade_id_filter:
        query = query.filter(
            CottonTradePosition.data["Trade_Key__c"].as_string().in_(trade_id_filter)
        )

    # Totals across filtered rows
    all_filtered = query.all()
    _all_pnl_map, _, _, _, _all_delta_map = compute_maps(all_filtered, price_source)
    total_pnl = 0
    total_net_pnl = 0
    total_position = None
    total_spread_pos = None
    for _pos in all_filtered:
        _pnl = _all_pnl_map.get(_pos.sf_id)
        if _pnl is not None:
            _comm = _pos.commission
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
    pnl_map, pnl_change_map, settlement_map, settlement2_map, delta_map = compute_maps(
        pagination.items, price_source
    )

    # Distinct option values for filter dropdowns
    def _distinct_json(field):
        return [r[0] for r in db.session.execute(sa_text(
            f"SELECT DISTINCT data->>'{field}' FROM cotton_trade_positions "
            f"WHERE data->>'{field}' IS NOT NULL AND data->>'{field}' != '' ORDER BY 1"
        )).fetchall()]

    def _distinct_col(col):
        return [r[0] for r in db.session.execute(sa_text(
            f"SELECT DISTINCT {col} FROM cotton_trade_positions "
            f"WHERE {col} IS NOT NULL AND {col} != '' ORDER BY 1"
        )).fetchall()]

    book_options       = _distinct_json("Book__c")
    contract_options   = _distinct_json("Contract__c")
    put_call_options   = _distinct_json("Put_Call_2__c")
    strike_options     = [r[0] for r in db.session.execute(sa_text(
        "SELECT DISTINCT CAST(data->>'Strike__c' AS NUMERIC) FROM cotton_trade_positions "
        "WHERE data->>'Strike__c' IS NOT NULL AND data->>'Strike__c' != '' ORDER BY 1"
    )).fetchall()]
    instrument_options = _distinct_col("instrument")
    spread_options     = _distinct_col("spread")
    region_options     = _distinct_col("region")
    status_options     = _distinct_json("Realised__c")
    trade_id_options   = sorted(
        _distinct_json("Trade_Key__c"),
        key=lambda x: (0, int(x)) if x.lstrip('-').isdigit() else (1, x)
    )

    return render_template(
        "cotton/positions.html",
        pagination=pagination,
        pnl_map=pnl_map, pnl_change_map=pnl_change_map,
        settlement_map=settlement_map, settlement2_map=settlement2_map,
        delta_map=delta_map,
        date_filter=date_filter, contract_filter=contract_filter, price_filter=price_filter,
        book_filter=book_filter, book_options=book_options,
        region_filter=region_filter, region_options=region_options,
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
        invalid_strategy_count=0,
        price_source=price_source,
        page_url=page_url,
    )


# ── Inline edit endpoint ─────────────────────────────────────────────────────

ALLOWED_FIELDS = {
    "Trade_Date__c", "Book__c", "Trader__c", "Broker_Name__c", "Contract__c",
    "Contract_type__c", "Strategy__c", "Long__c", "Short__c", "Price__c",
    "Strike__c", "Put_Call_2__c", "Broker_Commission__c", "Realised__c",
    "Trade_Code__c", "Trade_Key__c", "Trade_Group__c",
    "New_AGP__r.Name", "New_AGS__r.Name",
}
CONTRACT_REF_FIELDS = {"New_AGP__r.Name", "New_AGS__r.Name"}
NUMERIC_FIELDS = {"Long__c", "Short__c", "Price__c", "Strike__c", "Broker_Commission__c"}
# Cotton has 5 editable parsed strategy columns, plus a non-editable BF=fee component.
STRATEGY_FIELDS = {"instrument", "spread", "contract_xl", "book_parsed", "region"}


def resolve_cotton_book_to_sf(book_parsed, contract_xl):
    """Return the Salesforce Book__c value for a cotton trade.

    Mapping:
      "Alpha"        → "Spec"
      "Alt Physical" → ""
      "Physical"     → "Hedge" if contract_xl starts with AGP/AGS, else ""
      anything else  → None (no auto-update of Book__c)
    """
    if book_parsed == "Alpha":
        return "Spec"
    if book_parsed == "Alt Physical":
        return ""
    if book_parsed == "Physical":
        xl = (contract_xl or "").upper().lstrip()
        if xl.startswith("AGP") or xl.startswith("AGS"):
            return "Hedge"
        return ""
    return None


def _validate_strategy_component(value):
    v = (value or '').strip()
    if '-' in v:
        raise ValueError(f"Strategy component cannot contain a hyphen: '{v}'")
    return v


def _strategy_bf_component(pos):
    """Return the non-editable BF=... Strategy__c component for a cotton position."""
    if pos.bf_parsed is not None:
        return f"BF={pos.bf_parsed:.2f}"
    parts = ((pos.data or {}).get("Strategy__c") or "").split("-", 5)
    if len(parts) == 6:
        bf_component = parts[5].strip()
        if bf_component.startswith("BF="):
            return bf_component
    return "BF=0.00"


@cotton_positions_bp.route("/positions/api/update", methods=["POST"])
def api_update():
    data = request.get_json()
    if not data:
        return jsonify({"error": "No JSON body"}), 400
    changes = data.get("changes", [])
    if not changes:
        return jsonify({"ok": True, "updated": 0})
    push_to_sf = data.get("push_to_sf", False)

    # Guardrail: cotton SF push-back stays disabled until BOOK_TO_SF is confirmed.
    if push_to_sf and not current_app.config.get("COTTON_SF_PUSH_ENABLED", False):
        return jsonify({
            "error": "Cotton SF push-back not yet enabled — confirm BOOK_TO_SF mapping first"
        }), 400

    try:
        changes_by_record = {}
        strategy_touched_pos = {}
        contract_xl_touched_ids = set()

        for change in changes:
            sf_id = change.get("sf_id")
            field = change.get("field", "").strip()
            value = change.get("value")

            pos = CottonTradePosition.query.filter_by(sf_id=sf_id).first()
            if not pos:
                continue

            if field in STRATEGY_FIELDS:
                try:
                    clean = _validate_strategy_component(value)
                except ValueError as e:
                    return jsonify({"error": str(e)}), 400
                setattr(pos, field, clean if clean else None)
                strategy_touched_pos[sf_id] = pos
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

        # Reconstruct Strategy__c as 6-part cotton format, preserving the
        # non-editable brokerage fee component.
        # Recompute Book__c whenever book_parsed OR contract_xl changed, since
        # the "Physical" mapping depends on whether XL Ref starts with AGP/AGS.
        book_recompute_ids = (
            {c["sf_id"] for c in changes if c.get("field") == "book_parsed"}
            | contract_xl_touched_ids
        )
        for sf_id, pos in strategy_touched_pos.items():
            new_strategy = "{}-{}-{}-{}-{}-{}".format(
                pos.instrument or '',
                pos.spread or '',
                pos.contract_xl or '',
                pos.book_parsed or '',
                pos.region or '',
                _strategy_bf_component(pos),
            )
            new_data = dict(pos.data)
            new_data['Strategy__c'] = new_strategy
            new_book_sf = None
            if sf_id in book_recompute_ids:
                new_book_sf = resolve_cotton_book_to_sf(pos.book_parsed, pos.contract_xl)
                if new_book_sf is not None:
                    new_data['Book__c'] = new_book_sf
            pos.data = new_data
            if sf_id not in changes_by_record:
                changes_by_record[sf_id] = {}
            changes_by_record[sf_id]['Strategy__c'] = new_strategy
            if new_book_sf is not None:
                changes_by_record[sf_id]['Book__c'] = new_book_sf

        # Validate Long/Short mutual exclusivity
        for pos in db.session.dirty:
            if not isinstance(pos, CottonTradePosition):
                continue
            long_ = pos.data.get("Long__c")
            short_ = pos.data.get("Short__c")
            if long_ not in (None, 0, 0.0, "") and short_ not in (None, 0, 0.0, ""):
                db.session.rollback()
                return jsonify({
                    "error": f"Record {pos.sf_id}: Long and Short cannot both be filled."
                }), 400

        if not push_to_sf:
            db.session.commit()
            return jsonify({"ok": True, "updated": len(changes), "sf_errors": []})

        # push_to_sf=True: attempt SF first, only commit to DB if all succeed.
        # Mirrors routes/positions.py api_update push path.
        sf_errors = []
        try:
            from services.salesforce import get_sf_connection
            sf = get_sf_connection()
            for rec_id, rec_fields in changes_by_record.items():
                trade_fields = {k: v for k, v in rec_fields.items() if k not in CONTRACT_REF_FIELDS}
                ref_fields = {k: v for k, v in rec_fields.items() if k in CONTRACT_REF_FIELDS}
                sf_ref_local_update = None
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
                        trade_fields["New_AGP__c"] = None
                        trade_fields["New_AGS__c"] = None
                        sf_ref_local_update = ("New_AGP__c", "New_AGS__c", "New_AGP__r.Name", "New_AGS__r.Name", None, None)
                if trade_fields:
                    try:
                        sf.Futur__c.update(rec_id, trade_fields)
                    except Exception as e:
                        sf_errors.append(f"{rec_id}: {e}")
                    else:
                        if sf_ref_local_update:
                            target_lookup, clear_lookup, target_name_field, clear_name_field, mc_id, sf_ref = sf_ref_local_update
                            ref_pos = CottonTradePosition.query.filter_by(sf_id=rec_id).first()
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
                    if new_name.upper().startswith("AGS"):
                        target_lookup, clear_lookup = "New_AGS__c", "New_AGP__c"
                        target_name_field, clear_name_field = "New_AGS__r.Name", "New_AGP__r.Name"
                    else:
                        target_lookup, clear_lookup = "New_AGP__c", "New_AGS__c"
                        target_name_field, clear_name_field = "New_AGP__r.Name", "New_AGS__r.Name"
                    try:
                        mc_result = sf.query(f"SELECT Id FROM Master_Contract__c WHERE Name = '{new_name}' LIMIT 1")
                    except Exception as e:
                        sf_errors.append(f"{rec_id}: Master Contract lookup failed: {e}")
                        continue
                    if mc_result["totalSize"] == 0:
                        sf_errors.append(f"{rec_id}: Master Contract '{new_name}' not found in Salesforce")
                        continue
                    mc_id = mc_result["records"][0]["Id"]
                    try:
                        sf.Futur__c.update(rec_id, {target_lookup: mc_id, clear_lookup: None})
                    except Exception as e:
                        sf_errors.append(f"{rec_id}: {e}")
                        continue
                    ref_pos = CottonTradePosition.query.filter_by(sf_id=rec_id).first()
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


@cotton_positions_bp.route("/positions/delete/<sf_id>", methods=["POST"])
def delete(sf_id):
    pos = CottonTradePosition.query.filter_by(sf_id=sf_id).first_or_404()
    try:
        db.session.delete(pos)
        db.session.commit()
        flash("Cotton position deleted.", "success")
    except Exception as e:
        db.session.rollback()
        flash(f"Failed to delete: {e}", "danger")
    return redirect(url_for("cotton_positions.index"))

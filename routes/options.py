import math
from datetime import date, datetime, timedelta
from flask import Blueprint, render_template, request, jsonify
from sqlalchemy import cast, Date
from models.db import (
    db, TradePosition, MarketPrice,
    SimLoadedFuture, SimLoadedOption, SimStack,
)
from routes.positions import compute_maps
from routes.prices import _build_expiry_map, _OPTION_RE
from services.tradestation import (
    _black76_price, _black76_delta, _black76_gamma, _black76_vega, _black76_theta,
    _RISK_FREE_RATE_FALLBACK as _RISK_FREE_RATE, _fetch_sofr,
)
from routes.info import _workday, _HOLIDAY_DATES, _parse_futures, _RAW_FUTURES
from routes.positions import build_contract_key, LOT_MULTIPLIERS_BY_PREFIX as _MULTIPLIERS
from services.iv_utils import calculate_scenario_iv
from services.request_cache import get_all_positions

options_bp = Blueprint("options", __name__)


def _spec_options_query():
    """Return all unrealised Spec positions (options + futures)."""
    all_pos = get_all_positions()
    return [
        p for p in all_pos
        if p.data.get('Book__c') == 'Spec'
        and p.data.get('Realised__c') == 'Unrealised'
    ]


def _compute_greeks(legs_pos, price_map, as_of, source='sett1'):
    """Compute dollar Greeks per leg using Black-76 with IV (live or sett-1 per source).
    Returns (greeks_map, excluded_count) where greeks_map = {sf_id: {delta, gamma, vega, theta}}.
    """
    from services.price_source import resolve_price, resolve_iv
    expiry_map = _build_expiry_map()
    today = as_of
    result = {}
    excluded = 0

    for p in legs_pos:
        d = p.data
        key = build_contract_key(d)
        multiplier = _MULTIPLIERS.get(
            (d.get('Contract__c') or '').replace(' ', '')[:2].upper(), 1
        )
        net_lots = float(d.get('Long__c') or 0) + float(d.get('Short__c') or 0)

        m = _OPTION_RE.match(key)
        if not m:
            # Futures leg: delta = net_lots (position), all other Greeks = 0
            result[p.sf_id] = {
                'delta': net_lots,
                'gamma': 0.0,
                'vega':  0.0,
                'theta': 0.0,
            }
            continue

        underlying_code, pc, strike_cents = m.group(1), m.group(2), m.group(3)
        K = int(strike_cents) / 100.0
        is_call = (pc == 'C')

        underlying_mp = price_map.get(underlying_code)
        F = resolve_price(underlying_mp, source)
        if F is None:
            excluded += 1
            continue

        option_mp = price_map.get(key)
        sigma = resolve_iv(option_mp, source)
        if sigma is None or sigma <= 0:
            excluded += 1
            continue

        expiry = expiry_map.get(underlying_code)
        if expiry is None:
            excluded += 1
            continue
        T = (expiry - _workday(today, -1, _HOLIDAY_DATES)).days / 365.0
        if T <= 0:
            excluded += 1
            continue

        try:
            delta = _black76_delta(F, K, T, _RISK_FREE_RATE, sigma, is_call)
            gamma = _black76_gamma(F, K, T, _RISK_FREE_RATE, sigma)
            vega  = _black76_vega(F, K, T, _RISK_FREE_RATE, sigma)
            theta = _black76_theta(F, K, T, _RISK_FREE_RATE, sigma, is_call)
        except (ValueError, ZeroDivisionError):
            excluded += 1
            continue

        position = delta * net_lots
        result[p.sf_id] = {
            'delta': position,
            'gamma': gamma * net_lots,
            'vega':  vega  * net_lots * 1120 / 100,
            'theta': theta * net_lots * 1120,
        }

    return result, excluded


def _build_groups(legs_pos, price_map, expiry_map, greeks_map, source='sett1'):
    """Aggregate individual positions into per-contract-key dicts for the display table."""
    from services.price_source import resolve_iv
    pnl_map, _, settlement_map, _, delta_map = compute_maps(legs_pos, source)
    groups = {}
    for p in legs_pos:
        d = p.data
        key = build_contract_key(d)
        underlying = (d.get('Contract__c') or '').replace(' ', '')
        if key not in groups:
            option_mp = price_map.get(key)
            expiry_k = expiry_map.get(underlying)
            sigma_k = resolve_iv(option_mp, source)
            groups[key] = {
                'contract_key': key,
                'Put_Call_2__c': d.get('Put_Call_2__c'),
                'Strike__c': d.get('Strike__c'),
                'Contract__c': d.get('Contract__c'),
                'long': 0.0, 'short': 0.0,
                'price_x_lots': 0.0, 'total_lots': 0.0,
                'pnl': None, 'position': None, 'settlement': None,
                'expiry': expiry_k,
                'iv': sigma_k,
                'delta': 0.0, 'gamma': 0.0, 'vega': 0.0, 'theta': 0.0,
            }
        long_lots = float(d.get('Long__c') or 0)
        short_lots = float(d.get('Short__c') or 0)
        net = long_lots + short_lots
        groups[key]['long'] += long_lots
        groups[key]['short'] += short_lots
        groups[key]['price_x_lots'] += float(d.get('Price__c') or 0) * abs(net)
        groups[key]['total_lots'] += abs(net)

        leg_pnl = pnl_map.get(p.sf_id)
        if leg_pnl is not None:
            groups[key]['pnl'] = (groups[key]['pnl'] or 0) + leg_pnl

        leg_delta = delta_map.get(p.sf_id)
        if leg_delta is not None:
            groups[key]['position'] = (groups[key]['position'] or 0) + leg_delta * net

        sett = settlement_map.get(p.sf_id)
        if sett is not None:
            groups[key]['settlement'] = sett

        leg_greeks = greeks_map.get(p.sf_id)
        if leg_greeks:
            groups[key]['delta'] += leg_greeks['delta']
            groups[key]['gamma'] += leg_greeks['gamma']
            groups[key]['vega']  += leg_greeks['vega']
            groups[key]['theta'] += leg_greeks['theta']

    for g in groups.values():
        g['avg_price'] = (g['price_x_lots'] / g['total_lots']) if g['total_lots'] else 0.0
    return groups


@options_bp.route("/options")
def index():
    from services.price_source import get_price_source, resolve_price
    price_source = get_price_source()

    all_opts = _spec_options_query()

    trade_ids = sorted(
        set(str(p.data['Trade_Key__c']) for p in all_opts if p.data.get('Trade_Key__c')),
        key=lambda x: (0, int(x)) if x.lstrip('-').isdigit() else (1, x)
    )

    # As-of date for T calculation (defaults to latest trade date, same as dashboard)
    try:
        as_of_date = date.fromisoformat(request.args.get('as_of_date', ''))
    except ValueError:
        _latest = TradePosition.query.order_by(
            cast(TradePosition.data["Trade_Date__c"].as_string(), Date).desc()
        ).first()
        latest_str = _latest.data.get("Trade_Date__c") if _latest else None
        try:
            as_of_date = date.fromisoformat(latest_str) if latest_str else date.today()
        except ValueError:
            as_of_date = date.today()

    # SB futures reference month list (no spaces, e.g. "SBH26")
    sb_futures = [
        {'code': f['contract'].replace(' ', ''), 'label': f['contract'], 'expiry': f['expiry']}
        for f in _parse_futures(_RAW_FUTURES)
        if f['expiry'] is not None
    ]
    # Keep only non-expired contracts
    sb_futures = [f for f in sb_futures if f['expiry'] >= as_of_date]
    # Default: front month = smallest expiry; fallback to last
    default_ref_month = (
        min(sb_futures, key=lambda f: f['expiry'])['code']
        if sb_futures else ''
    )
    ref_month = request.args.get('ref_month', default_ref_month) or default_ref_month

    # Preserve slot positions: keep empty strings for unfilled slots
    selected_slots = request.args.getlist('trade_id')
    selected = [t for t in selected_slots if t]
    # No selection = all trade IDs
    if not selected:
        selected = trade_ids[:]
        selected_slots = []

    # All positions for selected trade IDs, on or before the as-of date
    def _trade_date(p):
        try:
            return date.fromisoformat(p.data.get('Trade_Date__c', ''))
        except ValueError:
            return date.min

    legs_pos = [
        p for p in all_opts
        if str(p.data.get('Trade_Key__c')) in selected
        and _trade_date(p) <= as_of_date
    ]

    # Price map for Greek computation
    all_keys = set()
    for p in legs_pos:
        d = p.data
        key = build_contract_key(d)
        underlying = (d.get('Contract__c') or '').replace(' ', '')
        all_keys.add(key)
        all_keys.add(underlying)
    if ref_month:
        all_keys.add(ref_month)
    for f in sb_futures:
        all_keys.add(f['code'])
    price_map = {mp.contract: mp for mp in MarketPrice.query.filter(
        MarketPrice.contract.in_(all_keys)).all()}

    ref_month_mp = price_map.get(ref_month)
    ref_month_settlement = resolve_price(ref_month_mp, price_source)

    # Settlement lookup for all SB futures (for dynamic ref-month switching in JS)
    sb_sett_map = {}
    for f in sb_futures:
        mp = price_map.get(f['code'])
        sb_sett_map[f['code']] = resolve_price(mp, price_source)

    greeks_map, excluded_count = _compute_greeks(legs_pos, price_map, as_of_date, price_source)

    # Aggregate portfolio Greeks (delta computed from position after groups are built)
    portfolio = {'delta': 0.0, 'gamma': 0.0, 'vega': 0.0, 'theta': 0.0}
    for g in greeks_map.values():
        for k in ('gamma', 'vega', 'theta'):
            portfolio[k] += g[k]

    # Legs table (grouped, using compute_maps for PNL consistency)
    expiry_map = _build_expiry_map()
    grouped_legs_raw = list(_build_groups(legs_pos, price_map, expiry_map, greeks_map, price_source).values())
    _pc_order = {None: 0, '': 0, 'Call': 1, 'Put': 2}
    grouped_legs = sorted(grouped_legs_raw,
                          key=lambda g: (_pc_order.get(g.get('Put_Call_2__c'), 3),
                                         g.get('contract_key', '')))

    # Portfolio delta = sum of positions (futures: net_lots, options: delta × net_lots)
    portfolio['delta'] = sum(g['position'] or 0 for g in grouped_legs)

    latest_expiry = max(
        (g['expiry'] for g in grouped_legs if g['expiry'] is not None),
        default=None
    )

    # Simulator: all unrealised trade IDs (Hedge + Spec)
    all_unrealised = _all_unrealised()
    sim_trade_ids = sorted(
        set(str(p.data['Trade_Key__c']) for p in all_unrealised if p.data.get('Trade_Key__c')),
        key=lambda x: (0, int(x)) if x.lstrip('-').isdigit() else (1, x)
    )

    # Fetch live SOFR for payoff tab default
    sofr_rate, sofr_date = _fetch_sofr()
    sofr_rate_pct = round(sofr_rate * 100, 2)

    return render_template("options.html",
                           trade_ids=trade_ids,
                           selected=selected,
                           selected_slots=selected_slots,
                           as_of_date=as_of_date,
                           portfolio=portfolio,
                           excluded_count=excluded_count,
                           grouped_legs=grouped_legs,
                           latest_expiry=latest_expiry,
                           sb_futures=sb_futures,
                           ref_month=ref_month,
                           ref_month_settlement=ref_month_settlement,
                           sim_trade_ids=sim_trade_ids,
                           sofr_rate_pct=sofr_rate_pct,
                           sofr_date=sofr_date,
                           sb_sett_map=sb_sett_map,
                           price_source=price_source)


@options_bp.route("/options/payoff", methods=["POST"])
def payoff():
    from services.price_source import get_price_source, resolve_price, resolve_iv
    price_source = get_price_source()
    body = request.get_json(force=True)
    try:
        selected     = body["trade_ids"]
        as_of_date   = date.fromisoformat(body["as_of_date"])
        input_date   = date.fromisoformat(body["input_date"])
        dates        = [date.fromisoformat(d) for d in body["dates"]]
        strikes      = [float(s) for s in body["strikes"]]
        spot_call_iv = float(body["spot_call_iv"])   # % e.g. 24.0
        spot_put_iv  = float(body["spot_put_iv"])
        skew_c       = float(body["skew_c"])
        r            = float(body["r_pct"]) / 100.0  # % → decimal
        exp_param    = float(body["exp_param"])
        ref_month_sett = float(body.get("ref_month_sett") or 0)
        # Optional detail point for per-leg breakdown
        _ds = body.get("detail_strike")
        _dd = body.get("detail_date")
        detail_strike = float(_ds) if _ds is not None else None
        detail_date   = date.fromisoformat(_dd) if _dd else None
    except (KeyError, ValueError) as e:
        return jsonify({"error": str(e)}), 400

    if not dates or not strikes:
        return jsonify({"error": "dates and strikes must be non-empty"}), 400
    if not (0 <= spot_call_iv <= 200) or not (0 <= spot_put_iv <= 200):
        return jsonify({"error": "IV must be between 0 and 200%"}), 400
    if skew_c < 0:
        return jsonify({"error": "skew_c must be >= 0"}), 400

    all_opts = _spec_options_query()

    def _trade_date(p):
        try:    return date.fromisoformat(p.data.get('Trade_Date__c', ''))
        except: return date.min

    legs_pos = [
        p for p in all_opts
        if str(p.data.get('Trade_Key__c')) in selected
        and _trade_date(p) <= as_of_date
    ]
    if not legs_pos:
        return jsonify({"error": "No matching positions found."}), 404

    all_keys = set()
    for p in legs_pos:
        all_keys.add(build_contract_key(p.data))
        all_keys.add((p.data.get('Contract__c') or '').replace(' ', ''))
    price_map  = {mp.contract: mp for mp in MarketPrice.query.filter(
        MarketPrice.contract.in_(all_keys)).all()}
    expiry_map = _build_expiry_map()

    # Precompute per-leg static data outside the grid loop
    leg_data = []
    for p in legs_pos:
        d          = p.data
        ckey       = build_contract_key(d)
        underlying = (d.get('Contract__c') or '').replace(' ', '')
        put_call   = d.get('Put_Call_2__c')
        net_lots    = float(d.get('Long__c') or 0) + float(d.get('Short__c') or 0)
        avg_price   = float(d.get('Price__c') or 0)
        price_total = avg_price * net_lots
        mult        = _MULTIPLIERS.get(underlying[:2].upper(), 1)

        # Each position's underlying settlement (for delta-based scenario pricing)
        underlying_mp = price_map.get(underlying)
        _u_resolved = resolve_price(underlying_mp, price_source)
        underlying_sett = _u_resolved if _u_resolved is not None else ref_month_sett

        if put_call:
            K          = float(d.get('Strike__c') or 0)
            is_call    = (put_call or '')[:1].upper() == 'C'
            expiry     = expiry_map.get(underlying)
            mp         = price_map.get(ckey)
            _iv = resolve_iv(mp, price_source)
            cur_iv_pct = (_iv * 100.0) if _iv else None
            leg_data.append({
                'is_option': True, 'is_call': is_call, 'K': K,
                'avg_price': avg_price, 'price_total': price_total,
                'net_lots': net_lots, 'mult': mult,
                'expiry': expiry, 'cur_iv_pct': cur_iv_pct,
                'underlying_sett': underlying_sett,
                'contract_key': ckey, 'option_type': put_call,
            })
        else:
            leg_data.append({
                'is_option': False,
                'avg_price': avg_price, 'price_total': price_total,
                'net_lots': net_lots, 'mult': mult,
                'underlying_sett': underlying_sett,
                'contract_key': ckey, 'option_type': 'Fut',
            })

    grid = []
    leg_details = []  # per-leg breakdown for the detail point
    for scenario_F in strikes:
        row = []
        for scenario_date in dates:
            portfolio_pnl = 0.0
            # Check if this is the detail point
            is_detail = (
                detail_strike is not None
                and detail_date is not None
                and scenario_F == detail_strike
                and scenario_date == detail_date
            )
            for leg in leg_data:
                # Delta-based scenario price: shift each position's own
                # underlying settlement by the same delta as the scenario
                # moves from the reference month settlement.
                price_delta = scenario_F - ref_month_sett
                leg_scenario_F = leg['underlying_sett'] + price_delta

                if not leg['is_option']:
                    leg_pnl = (leg_scenario_F - leg['avg_price']) * leg['net_lots'] * leg['mult']
                    portfolio_pnl += leg_pnl
                    if is_detail:
                        leg_details.append({
                            'contract_key': leg['contract_key'],
                            'option_type': leg['option_type'],
                            'net_lots': leg['net_lots'],
                            'trade_price': leg['avg_price'],
                            'price_total': leg['price_total'],
                            'scenario_F': round(leg_scenario_F, 4),
                            'scenario_value': round(leg_scenario_F, 4),
                            'scenario_iv': None,
                            'T': None,
                            'leg_pnl': round(leg_pnl, 2),
                        })
                    continue

                if leg['expiry'] is None:
                    continue

                T = (leg['expiry'] - scenario_date).days / 365.0
                pricing_iv = None
                iv_debug = None

                if T <= 0:
                    # At/past expiry: intrinsic value
                    val = max(leg_scenario_F - leg['K'], 0) if leg['is_call'] else max(leg['K'] - leg_scenario_F, 0)
                elif leg['cur_iv_pct'] is None:
                    # No IV data: option value = 0 (matches Excel IFERROR behaviour)
                    val = 0.0
                else:
                    pricing_iv = calculate_scenario_iv(
                        option_type='C' if leg['is_call'] else 'P',
                        current_iv_pct=leg['cur_iv_pct'],
                        strike=leg['K'],
                        current_spot=leg['underlying_sett'],
                        valuation_date=input_date,
                        expiry_date=leg['expiry'],
                        scenario_date=scenario_date,
                        scenario_price=leg_scenario_F,
                        spot_call_iv=spot_call_iv,
                        spot_put_iv=spot_put_iv,
                        skew=skew_c,
                        exp_param=exp_param,
                        debug=is_detail,
                    )
                    if is_detail and isinstance(pricing_iv, dict):
                        iv_debug = pricing_iv
                        pricing_iv = iv_debug['scenario_iv_decimal']
                    if pricing_iv is None:
                        val = 0.0
                    else:
                        try:
                            val = _black76_price(leg_scenario_F, leg['K'], T, r, pricing_iv, leg['is_call'])
                        except (ValueError, ZeroDivisionError):
                            val = 0.0

                leg_pnl = (val - leg['avg_price']) * leg['net_lots'] * leg['mult']
                portfolio_pnl += leg_pnl

                if is_detail:
                    detail_row = {
                        'contract_key': leg['contract_key'],
                        'option_type': leg['option_type'],
                        'net_lots': leg['net_lots'],
                        'trade_price': leg['avg_price'],
                        'price_total': leg['price_total'],
                        'scenario_F': round(leg_scenario_F, 4),
                        'scenario_value': round(val, 4),
                        'scenario_iv': round(pricing_iv * 100, 2) if pricing_iv else None,
                        'T': round(T, 6),
                        'leg_pnl': round(leg_pnl, 2),
                    }
                    if iv_debug:
                        detail_row['iv_debug'] = {
                            k: round(v, 4) if isinstance(v, float) else v
                            for k, v in iv_debug.items()
                        }
                    leg_details.append(detail_row)

            row.append(round(portfolio_pnl, 2))
        grid.append(row)

    # Group leg_details by contract_key
    if leg_details:
        grouped = {}
        for ld in leg_details:
            key = ld['contract_key']
            if key not in grouped:
                grouped[key] = {
                    'contract_key': key,
                    'option_type': ld['option_type'],
                    'net_lots': 0.0,
                    'price_total_sum': 0.0,
                    'scenario_F': ld['scenario_F'],
                    'scenario_value': ld['scenario_value'],
                    'scenario_iv': ld.get('scenario_iv'),
                    'T': ld.get('T'),
                    'leg_pnl': 0.0,
                    'iv_debug': ld.get('iv_debug'),
                }
            g = grouped[key]
            g['net_lots'] += ld['net_lots']
            g['price_total_sum'] += ld['price_total']
            g['leg_pnl'] += ld['leg_pnl']

        for g in grouped.values():
            g['trade_price'] = round(g['price_total_sum'] / g['net_lots'], 4) if g['net_lots'] != 0 else 0.0
            g['leg_pnl'] = round(g['leg_pnl'], 2)
            del g['price_total_sum']

        leg_details = list(grouped.values())

    result = {"dates": [d.isoformat() for d in dates], "strikes": strikes, "grid": grid}
    if leg_details:
        result["leg_details"] = leg_details
    return jsonify(result)


# ── Simulator endpoints ──────────────────────────────────────────────────────

def _all_unrealised():
    """Return all unrealised positions (both Hedge and Spec books)."""
    return [
        p for p in get_all_positions()
        if p.data.get('Realised__c') == 'Unrealised'
    ]


@options_bp.route("/options/sim/load-positions", methods=["POST"])
def sim_load_positions():
    from services.price_source import get_price_source, resolve_price, resolve_iv
    price_source = get_price_source()
    body = request.get_json(force=True) or {}
    trade_ids = body.get("trade_ids", [])  # list of Trade_Key__c values

    # Fetch live SOFR rate (falls back to _RISK_FREE_RATE on failure)
    sofr_rate, _ = _fetch_sofr()
    risk_free_rate = sofr_rate if sofr_rate else _RISK_FREE_RATE

    all_pos = _all_unrealised()

    # Accept as_of_date from request, fall back to latest trade date
    _as_of_str = body.get("as_of_date")
    if _as_of_str:
        try:
            as_of_date = date.fromisoformat(_as_of_str)
        except ValueError:
            as_of_date = date.today()
    else:
        _latest = TradePosition.query.order_by(
            cast(TradePosition.data["Trade_Date__c"].as_string(), Date).desc()
        ).first()
        _latest_str = _latest.data.get("Trade_Date__c") if _latest else None
        try:
            as_of_date = date.fromisoformat(_latest_str) if _latest_str else date.today()
        except ValueError:
            as_of_date = date.today()

    # Filter by trade IDs AND trade date (same as Payoff tab)
    def _trade_date(p):
        try:
            return date.fromisoformat(p.data.get('Trade_Date__c', ''))
        except ValueError:
            return date.min

    if trade_ids:
        tid_set = set(str(t) for t in trade_ids)
        all_pos = [
            p for p in all_pos
            if str(p.data.get('Trade_Key__c', '')) in tid_set
            and _trade_date(p) <= as_of_date
        ]
    else:
        all_pos = [p for p in all_pos if _trade_date(p) <= as_of_date]

    today = as_of_date

    expiry_map = _build_expiry_map()
    all_keys = set()
    for p in all_pos:
        key = build_contract_key(p.data)
        underlying = (p.data.get('Contract__c') or '').replace(' ', '')
        all_keys.add(key)
        all_keys.add(underlying)
    price_map = {mp.contract: mp for mp in MarketPrice.query.filter(
        MarketPrice.contract.in_(all_keys)).all()} if all_keys else {}

    # ── Compute Greeks using the SAME functions as the Payoff tab ────────
    greeks_map, _ = _compute_greeks(all_pos, price_map, today, price_source)
    _, _, settlement_map, _, delta_map = compute_maps(all_pos, price_source)

    # ── Aggregate into contract-key groups (same as _build_groups) ───────
    fut_groups = {}
    opt_groups = {}
    for p in all_pos:
        d = p.data
        key = build_contract_key(d)
        underlying = (d.get('Contract__c') or '').replace(' ', '')
        net = float(d.get('Long__c') or 0) + float(d.get('Short__c') or 0)
        avg_p = float(d.get('Price__c') or 0)
        commodity = d.get('Commodity_Name__c', '')
        prefix = underlying[:2].upper()
        mult = _MULTIPLIERS.get(prefix, 1)

        leg_greeks = greeks_map.get(p.sf_id, {})
        leg_delta_from_mp = delta_map.get(p.sf_id)  # MarketPrice.delta (same as Payoff)

        m = _OPTION_RE.match(key)
        if not m:
            # Futures
            if key not in fut_groups:
                mp = price_map.get(key)
                sett = resolve_price(mp, price_source)
                exp = expiry_map.get(key)
                fut_groups[key] = {
                    'contract': key, 'commodity': commodity,
                    'net_lots': 0, 'price_x_lots': 0, 'total_lots': 0,
                    'settlement': sett, 'expiry_date': exp, 'point_value': mult,
                }
            fut_groups[key]['net_lots'] += net
            fut_groups[key]['price_x_lots'] += avg_p * abs(net)
            fut_groups[key]['total_lots'] += abs(net)
        else:
            # Options
            underlying_code = m.group(1)
            pc = m.group(2)
            strike = int(m.group(3)) / 100.0
            is_call = (pc == 'C')

            if key not in opt_groups:
                mp = price_map.get(key)
                u_mp = price_map.get(underlying_code)
                _sett_resolved = resolve_price(mp, price_source)
                sett = _sett_resolved if _sett_resolved is not None else 0
                u_price = resolve_price(u_mp, price_source)
                iv = resolve_iv(mp, price_source)
                exp = expiry_map.get(underlying_code)
                opt_groups[key] = {
                    'contract': key, 'underlying': underlying_code,
                    'commodity': commodity, 'put_call': 'Call' if is_call else 'Put',
                    'strike': strike, 'net_lots': 0, 'price_x_lots': 0, 'total_lots': 0,
                    'settlement': sett, 'underlying_price': u_price, 'iv': iv,
                    'expiry_date': exp, 'point_value': mult, 'is_call': is_call,
                    # Greeks accumulators (sum per-leg, same as Payoff)
                    'delta': 0, 'gamma': 0, 'vega': 0, 'theta': 0,
                    'position': 0,  # delta from MarketPrice (same as Payoff)
                }
            opt_groups[key]['net_lots'] += net
            opt_groups[key]['price_x_lots'] += avg_p * abs(net)
            opt_groups[key]['total_lots'] += abs(net)
            # Sum per-leg Greeks (same as _build_groups in Payoff)
            opt_groups[key]['delta'] += leg_greeks.get('delta', 0)
            opt_groups[key]['gamma'] += leg_greeks.get('gamma', 0)
            opt_groups[key]['vega'] += leg_greeks.get('vega', 0)
            opt_groups[key]['theta'] += leg_greeks.get('theta', 0)
            if leg_delta_from_mp is not None:
                opt_groups[key]['position'] += leg_delta_from_mp * net

    # Clear previous sim data
    SimLoadedFuture.query.delete()
    SimLoadedOption.query.delete()
    db.session.commit()

    # Save futures
    futures_out = []
    for g in fut_groups.values():
        avg = g['price_x_lots'] / g['total_lots'] if g['total_lots'] else 0
        sett = g['settlement'] or 0
        row = SimLoadedFuture(
            contract=g['contract'], commodity=g['commodity'],
            net_lots=g['net_lots'], avg_price=round(avg, 4),
            settlement=sett,
            lower_limit=round(sett * 0.8, 2), upper_limit=round(sett * 1.2, 2),
            expiry_date=g['expiry_date'], point_value=g['point_value'],
        )
        db.session.add(row)
        db.session.flush()
        futures_out.append({
            'id': row.id, 'contract': row.contract, 'commodity': row.commodity,
            'net_lots': row.net_lots, 'avg_price': row.avg_price,
            'settlement': row.settlement,
            'lower_limit': row.lower_limit, 'upper_limit': row.upper_limit,
            'expiry_date': row.expiry_date.isoformat() if row.expiry_date else None,
            'point_value': row.point_value,
        })

    # Save options with per-leg-summed Greeks
    options_out = []
    greeks_summary = {'delta': 0, 'gamma': 0, 'vega': 0, 'theta': 0}
    for g in opt_groups.values():
        avg = g['price_x_lots'] / g['total_lots'] if g['total_lots'] else 0
        iv = g['iv']
        u_price = g['underlying_price']
        exp = g['expiry_date']
        sett = g['settlement'] or 0
        net = g['net_lots']
        mult = g['point_value']
        delta_v = g['delta']
        gamma_v = g['gamma']
        vega_v = g['vega']
        theta_v = g['theta']

        row = SimLoadedOption(
            contract=g['contract'], underlying=g['underlying'],
            commodity=g['commodity'], put_call=g['put_call'],
            strike=g['strike'], net_lots=net, avg_price=round(avg, 4),
            settlement=sett, underlying_price=u_price or 0,
            iv=iv or 0,
            iv_lower=round((iv or 0) * 0.8, 6),
            iv_upper=round((iv or 0) * 1.2, 6),
            expiry_date=exp, point_value=mult, r=risk_free_rate,
            delta=round(delta_v, 6), gamma=round(gamma_v, 6),
            vega=round(vega_v, 2), theta=round(theta_v, 2),
        )
        db.session.add(row)
        db.session.flush()

        # Portfolio Greeks: gamma/vega/theta from _compute_greeks (same as Payoff)
        greeks_summary['gamma'] += gamma_v
        greeks_summary['vega'] += vega_v
        greeks_summary['theta'] += theta_v
        # Portfolio delta: from MarketPrice.delta (same as Payoff)
        greeks_summary['delta'] += g['position']

        options_out.append({
            'id': row.id, 'contract': row.contract, 'underlying': row.underlying,
            'commodity': row.commodity, 'put_call': row.put_call,
            'strike': row.strike, 'net_lots': row.net_lots, 'avg_price': row.avg_price,
            'settlement': row.settlement, 'underlying_price': row.underlying_price,
            'iv': row.iv, 'iv_lower': row.iv_lower, 'iv_upper': row.iv_upper,
            'expiry_date': row.expiry_date.isoformat() if row.expiry_date else None,
            'point_value': row.point_value,
            'delta': round(delta_v, 4), 'gamma': round(gamma_v, 6),
            'vega': round(vega_v, 2), 'theta': round(theta_v, 2),
        })

    # Add futures delta to summary (net_lots, same as Payoff)
    for g in fut_groups.values():
        greeks_summary['delta'] += g['net_lots']

    db.session.commit()

    return jsonify({
        'futures': futures_out,
        'options': options_out,
        'greeks_summary': {k: round(v, 4) for k, v in greeks_summary.items()},
    })


@options_bp.route("/options/sim/futures/<int:fid>", methods=["PUT"])
def sim_update_future(fid):
    row = SimLoadedFuture.query.get_or_404(fid)
    body = request.get_json(force=True)
    for field in ('net_lots', 'lower_limit', 'upper_limit', 'settlement', 'avg_price'):
        if field in body:
            setattr(row, field, float(body[field]))
    db.session.commit()
    return jsonify({'ok': True})


@options_bp.route("/options/sim/options/<int:oid>", methods=["PUT"])
def sim_update_option(oid):
    row = SimLoadedOption.query.get_or_404(oid)
    body = request.get_json(force=True)
    for field in ('net_lots', 'iv', 'iv_lower', 'iv_upper', 'settlement',
                  'underlying_price', 'avg_price', 'strike'):
        if field in body:
            setattr(row, field, float(body[field]))
    # Recompute Greeks if IV or underlying changed
    if row.iv and row.iv > 0 and row.underlying_price and row.expiry_date:
        T = (row.expiry_date - _workday(date.today(), -1, _HOLIDAY_DATES)).days / 365.0
        is_call = row.put_call == 'Call'
        if T > 0:
            try:
                row.delta = _black76_delta(row.underlying_price, row.strike, T, row.r, row.iv, is_call) * row.net_lots
                row.gamma = _black76_gamma(row.underlying_price, row.strike, T, row.r, row.iv) * row.net_lots
                row.vega = _black76_vega(row.underlying_price, row.strike, T, row.r, row.iv) * row.net_lots * 1120 / 100
                row.theta = _black76_theta(row.underlying_price, row.strike, T, row.r, row.iv, is_call) * row.net_lots * 1120
            except (ValueError, ZeroDivisionError):
                pass
    db.session.commit()
    return jsonify({
        'ok': True,
        'delta': round(row.delta, 4), 'gamma': round(row.gamma, 6),
        'vega': round(row.vega, 2), 'theta': round(row.theta, 2),
    })


@options_bp.route("/options/sim/futures/<int:fid>", methods=["DELETE"])
def sim_delete_future(fid):
    row = SimLoadedFuture.query.get_or_404(fid)
    db.session.delete(row)
    db.session.commit()
    return jsonify({'ok': True})


@options_bp.route("/options/sim/options/<int:oid>", methods=["DELETE"])
def sim_delete_option(oid):
    row = SimLoadedOption.query.get_or_404(oid)
    db.session.delete(row)
    db.session.commit()
    return jsonify({'ok': True})


@options_bp.route("/options/sim/reset-limits", methods=["POST"])
def sim_reset_limits():
    for f in SimLoadedFuture.query.all():
        f.lower_limit = round(f.settlement * 0.8, 2)
        f.upper_limit = round(f.settlement * 1.2, 2)
    for o in SimLoadedOption.query.all():
        o.iv_lower = round(o.iv * 0.8, 6)
        o.iv_upper = round(o.iv * 1.2, 6)
    db.session.commit()
    return jsonify({'ok': True})


@options_bp.route("/options/sim/run", methods=["POST"])
def sim_run():
    from services.price_source import get_price_source, resolve_price
    price_source = get_price_source()
    body = request.get_json(force=True)
    x_axis = body.get('x_axis', 'Price')  # 'Price' or 'Volatility'
    commodity_code = body.get('commodity_code')
    overlay_dates = body.get('overlay_dates', [])
    n_points = int(body.get('n_points', 20))
    # User-supplied risk-free rate (in %), falls back to per-option r if not provided
    _r_pct = body.get('r_pct')
    user_r = (float(_r_pct) / 100.0) if _r_pct is not None else None

    if not commodity_code:
        return jsonify({'error': 'commodity_code required'}), 400
    if not overlay_dates:
        return jsonify({'error': 'overlay_dates required'}), 400

    futures = SimLoadedFuture.query.all()
    options = SimLoadedOption.query.all()

    # Find the anchor future for the x-axis
    # If not in loaded positions, look up MarketPrice and create a virtual anchor
    anchor = next((f for f in futures if f.contract == commodity_code), None)
    if anchor:
        current_price = anchor.settlement
        lower_limit = anchor.lower_limit
        upper_limit = anchor.upper_limit
    else:
        mp = MarketPrice.query.filter_by(contract=commodity_code).first()
        current_price = resolve_price(mp, price_source)
        if not current_price:
            return jsonify({'error': f'No price found for {commodity_code}'}), 404
        lower_limit = round(current_price * 0.8, 4)
        upper_limit = round(current_price * 1.2, 4)

    # Build futures lookup by contract code
    fut_map = {f.contract: f for f in futures}

    # Precompute initial option prices from current IV (same as GTB)
    # so that IV edits in the UI are reflected in simulation P&L
    today = date.today()
    initial_prices = {}
    for o in options:
        if not o.expiry_date or not o.iv or o.iv <= 0 or not o.underlying_price:
            initial_prices[o.id] = o.settlement or 0
            continue
        T_init = max((o.expiry_date - today).days / 365.0, 1 / 365.0)
        is_call = o.put_call == 'Call'
        r_init = user_r if user_r is not None else o.r
        try:
            initial_prices[o.id] = _black76_price(
                o.underlying_price, o.strike, T_init, r_init, o.iv, is_call)
        except (ValueError, ZeroDivisionError):
            initial_prices[o.id] = o.settlement or 0

    # Get base vol limits for volatility mode
    base_vol = 0.20
    vol_lower_limit = base_vol * 0.8
    vol_upper_limit = base_vol * 1.2
    for o in options:
        if o.underlying == commodity_code and o.iv and o.iv > 0:
            base_vol = o.iv
            vol_lower_limit = o.iv_lower or (base_vol * 0.8)
            vol_upper_limit = o.iv_upper or (base_vol * 1.2)
            break

    # Generate X-axis range (same as GTB: 20 left + center + 20 right = 41 points)
    if x_axis == 'Price':
        x_values = []
        interval_left = (current_price - lower_limit) / n_points
        interval_right = (upper_limit - current_price) / n_points
        for i in range(n_points):
            x_values.append(lower_limit + i * interval_left)
        x_values.append(current_price)
        for i in range(n_points):
            x_values.append(current_price + (i + 1) * interval_right)
        # Round to tick size (0.01 for sugar)
        tick_size = 0.01
        x_values = [round(round(p / tick_size) * tick_size, 4) for p in x_values]
    else:
        x_values = []
        interval_left = (base_vol - vol_lower_limit) / n_points
        interval_right = (vol_upper_limit - base_vol) / n_points
        for i in range(n_points):
            x_values.append(vol_lower_limit + i * interval_left)
        x_values.append(base_vol)
        for i in range(n_points):
            x_values.append(base_vol + (i + 1) * interval_right)

    results = {}
    for date_str in overlay_dates:
        scenario_date = date.fromisoformat(date_str)

        profit_arr = []
        delta_arr = []
        gamma_arr = []
        vega_arr = []
        theta_arr = []

        for x_val in x_values:
            total_profit = 0
            total_delta = 0
            total_gamma = 0
            total_vega = 0
            total_theta = 0

            # position_ratio: where in X-axis range (0=lower, 1=upper)
            if x_axis == 'Price':
                position_ratio = ((x_val - lower_limit) / (upper_limit - lower_limit)
                                  if upper_limit != lower_limit else 0.5)
            else:
                position_ratio = ((x_val - vol_lower_limit) / (vol_upper_limit - vol_lower_limit)
                                  if vol_upper_limit != vol_lower_limit else 0.5)

            # Futures P&L — piecewise scaling anchored at each commodity's own price
            for f in futures:
                if f.net_lots == 0:
                    continue
                if x_axis == 'Price':
                    # Piecewise: when x-axis commodity is at current_price,
                    # this commodity is at its own theo price
                    if x_val <= current_price:
                        seg_ratio = ((x_val - lower_limit) / (current_price - lower_limit)
                                     if current_price != lower_limit else 1.0)
                        fut_price = f.lower_limit + seg_ratio * (f.settlement - f.lower_limit)
                    else:
                        seg_ratio = ((x_val - current_price) / (upper_limit - current_price)
                                     if upper_limit != current_price else 1.0)
                        fut_price = f.settlement + seg_ratio * (f.upper_limit - f.settlement)
                else:
                    fut_price = f.settlement

                pnl = (fut_price - f.settlement) * f.net_lots * f.point_value
                total_profit += pnl
                total_delta += f.net_lots

            # Options P&L and Greeks
            for o in options:
                if o.net_lots == 0 or not o.expiry_date:
                    continue

                T = max((o.expiry_date - scenario_date).days / 365.0, 1 / 365.0)
                is_call = o.put_call == 'Call'

                # Resolve this option's underlying futures limits
                uf = fut_map.get(o.underlying)
                if uf:
                    opt_lower = uf.lower_limit
                    opt_upper = uf.upper_limit
                    opt_theo = uf.settlement
                else:
                    opt_lower = o.underlying_price * 0.8
                    opt_upper = o.underlying_price * 1.2
                    opt_theo = o.underlying_price

                # Piecewise underlying price scaling (same as futures)
                if x_axis == 'Price':
                    if x_val <= current_price:
                        seg_ratio = ((x_val - lower_limit) / (current_price - lower_limit)
                                     if current_price != lower_limit else 1.0)
                        o_underlying = opt_lower + seg_ratio * (opt_theo - opt_lower)
                    else:
                        seg_ratio = ((x_val - current_price) / (upper_limit - current_price)
                                     if upper_limit != current_price else 1.0)
                        o_underlying = opt_theo + seg_ratio * (opt_upper - opt_theo)
                else:
                    o_underlying = o.underlying_price

                # Piecewise IV scaling
                opt_vol_mid = o.iv or 0.20
                opt_vol_lower = o.iv_lower or (opt_vol_mid * 0.8)
                opt_vol_upper = o.iv_upper or (opt_vol_mid * 1.2)

                if x_axis == 'Price' and opt_upper != opt_lower:
                    if o_underlying <= opt_theo:
                        ratio = ((o_underlying - opt_lower) / (opt_theo - opt_lower)
                                 if opt_theo != opt_lower else 1.0)
                        scenario_iv = opt_vol_lower + ratio * (opt_vol_mid - opt_vol_lower)
                    else:
                        ratio = ((o_underlying - opt_theo) / (opt_upper - opt_theo)
                                 if opt_upper != opt_theo else 1.0)
                        scenario_iv = opt_vol_mid + ratio * (opt_vol_upper - opt_vol_mid)
                else:
                    # Volatility mode: scale each option's own vol bounds proportionally
                    scenario_iv = opt_vol_lower + position_ratio * (opt_vol_upper - opt_vol_lower)

                scenario_iv = max(scenario_iv, 0.001)

                r_use = user_r if user_r is not None else o.r
                try:
                    val = _black76_price(o_underlying, o.strike, T, r_use, scenario_iv, is_call)
                    d = _black76_delta(o_underlying, o.strike, T, r_use, scenario_iv, is_call) * o.net_lots
                    g = _black76_gamma(o_underlying, o.strike, T, r_use, scenario_iv) * o.net_lots
                    v = _black76_vega(o_underlying, o.strike, T, r_use, scenario_iv) / 100 * o.net_lots * o.point_value
                    th = _black76_theta(o_underlying, o.strike, T, r_use, scenario_iv, is_call) * o.net_lots * o.point_value
                except (ValueError, ZeroDivisionError):
                    continue

                initial_price = initial_prices.get(o.id, o.settlement or 0)
                pnl = (val - initial_price) * o.net_lots * o.point_value
                total_profit += pnl
                total_delta += d
                total_gamma += g
                total_vega += v
                total_theta += th

            profit_arr.append(round(total_profit, 4))
            delta_arr.append(round(total_delta, 4))
            gamma_arr.append(round(total_gamma, 4))
            vega_arr.append(round(total_vega, 4))
            theta_arr.append(round(total_theta, 4))

        results[date_str] = {
            'x_values': [round(v * 100, 2) for v in x_values] if x_axis == 'Volatility' else x_values,
            'profit': profit_arr,
            'delta': delta_arr,
            'gamma': gamma_arr,
            'vega': vega_arr,
            'theta': theta_arr,
        }

    return jsonify({
        'results': results,
        'commodity_code': commodity_code,
        'current_price': current_price,
        'x_axis': x_axis,
    })


@options_bp.route("/options/sim/theta-decay", methods=["POST"])
def sim_theta_decay():
    options = SimLoadedOption.query.all()
    if not options:
        return jsonify({'error': 'No options loaded'}), 400

    valid = [o for o in options if o.expiry_date and o.iv and o.iv > 0 and o.underlying_price]
    if not valid:
        return jsonify({'error': 'No valid options with IV/price data'}), 400

    latest_expiry = max(o.expiry_date for o in valid)
    today = date.today()

    # Build contract labels (same format as GTB)
    contract_labels = {}
    for o in valid:
        pc = 'C' if o.put_call == 'Call' else 'P'
        sign = '+' if o.net_lots >= 0 else ''
        contract_labels[o.id] = f"{o.underlying} K={o.strike:.2f} {pc} ({sign}{int(o.net_lots)})"

    # Collect expiry dates with their contracts
    expiry_contracts = {}
    for o in valid:
        iso = o.expiry_date.isoformat()
        if iso not in expiry_contracts:
            expiry_contracts[iso] = set()
        expiry_contracts[iso].add(o.underlying)

    dates_out = []
    daily_theta = []
    cumulative = 0
    cumulative_theta = []
    contract_theta = {contract_labels[o.id]: [] for o in valid}

    current = today
    while current <= latest_expiry:
        day_theta = 0
        for o in valid:
            T = (o.expiry_date - current).days / 365.0
            if T <= 0:
                contract_theta[contract_labels[o.id]].append(0)
                continue
            is_call = o.put_call == 'Call'
            try:
                th = _black76_theta(o.underlying_price, o.strike, T, o.r, o.iv, is_call)
                ct = th * o.net_lots * 1120
            except (ValueError, ZeroDivisionError):
                ct = 0
            day_theta += ct
            contract_theta[contract_labels[o.id]].append(round(ct, 2))

        dates_out.append(current.isoformat())
        daily_theta.append(round(day_theta, 2))
        cumulative += day_theta
        cumulative_theta.append(round(cumulative, 2))
        current += timedelta(days=1)

    positions_expiring = [
        {'date': d, 'contracts': sorted(list(c))}
        for d, c in sorted(expiry_contracts.items())
    ]

    return jsonify({
        'dates': dates_out,
        'daily_theta': daily_theta,
        'cumulative_theta': cumulative_theta,
        'contract_theta': contract_theta,
        'positions_expiring': positions_expiring,
        'latest_expiry': latest_expiry.isoformat(),
    })


@options_bp.route("/options/sim/stacks", methods=["GET"])
def sim_list_stacks():
    stacks = SimStack.query.order_by(SimStack.created_at.desc()).all()
    return jsonify([{
        'id': s.id, 'label': s.label, 'x_axis': s.x_axis,
        'commodity_code': s.commodity_code,
        'data': s.data,
        'created_at': s.created_at.isoformat() if s.created_at else None,
    } for s in stacks])


@options_bp.route("/options/sim/stacks", methods=["POST"])
def sim_save_stack():
    body = request.get_json(force=True)
    stack = SimStack(
        label=body.get('label', 'Untitled'),
        x_axis=body.get('x_axis'),
        commodity_code=body.get('commodity_code'),
        data=body.get('data'),
    )
    db.session.add(stack)
    db.session.commit()
    return jsonify({'id': stack.id, 'ok': True})


@options_bp.route("/options/sim/stacks/<int:sid>", methods=["DELETE"])
def sim_delete_stack(sid):
    row = SimStack.query.get_or_404(sid)
    db.session.delete(row)
    db.session.commit()
    return jsonify({'ok': True})

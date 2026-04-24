from collections import defaultdict
from flask import Blueprint, render_template, request
from sqlalchemy import cast, Date
from models.db import TradePosition, MarketPrice
from routes.positions import build_contract_key, LOT_MULTIPLIERS
from services.price_source import get_price_source, resolve_price, resolve_delta
from services.request_cache import get_all_positions, get_all_market_prices

summary_bp = Blueprint("summary", __name__)


def _pos_pnl_change(mkt, mkt2, mult, ls, trade_price, commission, trade_date, latest_date):
    """Per-position PnL Change: Net PnL if on latest trade date, else settlement diff."""
    if trade_date == latest_date:
        if mkt is not None and trade_price is not None and mult:
            return (mkt - trade_price) * ls * mult + commission
        return None
    if mkt is not None and mkt2 is not None and mult:
        return (mkt - mkt2) * ls * mult
    return None


@summary_bp.route("/summary")
def index():
    price_source = get_price_source()

    market = {mp.contract: mp for mp in get_all_market_prices()}
    prices = {k: resolve_price(mp, price_source) for k, mp in market.items()}
    prices2 = {k: mp.settlement2 for k, mp in market.items()}
    _latest = TradePosition.query.order_by(
        cast(TradePosition.data["Trade_Date__c"].as_string(), Date).desc()
    ).first()
    latest_date = _latest.data.get("Trade_Date__c") if _latest else None

    all_positions = get_all_positions()

    # Collect unique filter options
    books = sorted({pos.data.get('Book__c') or '' for pos in all_positions} - {''})
    statuses = sorted({pos.data.get('Realised__c') or '' for pos in all_positions} - {''})
    trade_codes = sorted({pos.data.get('Trade_Code__c') or '' for pos in all_positions} - {''})

    def _multi(name):
        return [v.strip() for v in request.args.getlist(name) if v.strip()]

    selected_book = _multi('book')
    selected_status = _multi('status')
    selected_book_pivot = _multi('book_pivot')
    selected_status_pivot = _multi('status_pivot')
    selected_book_trader = _multi('book_trader')
    selected_status_trader = _multi('status_trader')
    selected_tradecode_trader = _multi('tradecode_trader')
    selected_book_spread = _multi('book_spread')
    selected_status_spread = _multi('status_spread')
    selected_book_openst1 = _multi('book_openst1')
    selected_tradecode_openst1 = _multi('tradecode_openst1')
    active_tab = request.args.get('tab', 'contract')

    # ── By Contract ──────────────────────────────────────────────────────
    net_lots = defaultdict(int)
    pnl_map = defaultdict(lambda: None)
    pnl_change_map = defaultdict(lambda: None)
    commission_map = defaultdict(float)
    delta_map = {}

    for pos in all_positions:
        if selected_book and pos.data.get('Book__c') not in selected_book:
            continue
        if selected_status and pos.data.get('Realised__c') not in selected_status:
            continue

        key = build_contract_key(pos.data)
        if not key:
            continue
        long_ = pos.data.get('Long__c') or 0
        short_ = pos.data.get('Short__c') or 0
        ls = long_ + short_
        net_lots[key] += ls
        commission = pos.commission
        commission_map[key] += commission
        if key not in delta_map:
            is_option = bool(pos.data.get('Put_Call_2__c') and pos.data.get('Strike__c') is not None)
            d_val = resolve_delta(market.get(key), price_source)
            if d_val is not None:
                delta_map[key] = d_val
            elif is_option:
                delta_map[key] = 0
            else:
                delta_map[key] = 1.0

        mkt = prices.get(key)
        mkt2 = prices2.get(key)
        is_option = bool(pos.data.get('Put_Call_2__c') and pos.data.get('Strike__c') is not None)
        if mkt is None and is_option:
            mkt = 0
        trade_price = pos.data.get('Price__c')
        mult = LOT_MULTIPLIERS.get(pos.data.get('Commodity_Name__c', ''))
        if mkt is not None and trade_price is not None and mult:
            pnl_map[key] = (pnl_map[key] or 0) + (mkt - trade_price) * ls * mult
        pnl_ch = _pos_pnl_change(mkt, mkt2, mult, ls, trade_price, commission, pos.data.get('Trade_Date__c'), latest_date)
        if pnl_ch is not None:
            pnl_change_map[key] = (pnl_change_map[key] or 0) + pnl_ch

    summary = [
        {
            'contract': key,
            'net_lots': net_lots[key],
            'delta': delta_map.get(key),
            'pnl': pnl_map[key],
            'pnl_change': pnl_change_map[key],
            'commission': commission_map[key],
        }
        for key in sorted(net_lots.keys())
    ]

    # ── By Strategy ──────────────────────────────────────────────────────
    strat_keys = set()
    strat_spread_pos = defaultdict(lambda: None)
    strat_pnl = defaultdict(lambda: None)
    strat_pnl_change = defaultdict(lambda: None)
    strat_commission = defaultdict(float)

    for pos in all_positions:
        if selected_book and pos.data.get('Book__c') not in selected_book:
            continue
        if selected_status and pos.data.get('Realised__c') not in selected_status:
            continue

        spread_contract = (pos.spread or '').strip()
        if not spread_contract:
            continue
        strat_keys.add(spread_contract)

        contract_key = build_contract_key(pos.data)
        long_ = pos.data.get('Long__c') or 0
        short_ = pos.data.get('Short__c') or 0
        ls = long_ + short_

        instrument = (pos.instrument or '').strip()
        if instrument == 'Spread':
            contract_last3 = (pos.data.get('Contract__c') or '').replace(' ', '')[-3:]
            spread_last3 = spread_contract[-3:]
            if contract_last3 == spread_last3:
                sp_contrib = 0.0
            else:
                mp = market.get(contract_key)
                is_option_sp = bool(pos.data.get('Put_Call_2__c') and pos.data.get('Strike__c') is not None)
                d_val = resolve_delta(mp, price_source)
                if d_val is not None:
                    delta = d_val
                elif is_option_sp:
                    delta = 0
                else:
                    delta = 1.0
                sp_contrib = ls * delta
            if sp_contrib is not None:
                strat_spread_pos[spread_contract] = (strat_spread_pos[spread_contract] or 0) + sp_contrib

        mkt = prices.get(contract_key)
        mkt2 = prices2.get(contract_key)
        is_option = bool(pos.data.get('Put_Call_2__c') and pos.data.get('Strike__c') is not None)
        if mkt is None and is_option:
            mkt = 0
        trade_price = pos.data.get('Price__c')
        commission = pos.commission
        mult = LOT_MULTIPLIERS.get(pos.data.get('Commodity_Name__c', ''))
        if mkt is not None and trade_price is not None and mult:
            strat_pnl[spread_contract] = (strat_pnl[spread_contract] or 0) + (mkt - trade_price) * ls * mult
        pnl_ch = _pos_pnl_change(mkt, mkt2, mult, ls, trade_price, commission, pos.data.get('Trade_Date__c'), latest_date)
        if pnl_ch is not None:
            strat_pnl_change[spread_contract] = (strat_pnl_change[spread_contract] or 0) + pnl_ch
        strat_commission[spread_contract] += commission

    strategy_summary = [
        {
            'strategy': s,
            'spread_pos': strat_spread_pos[s],
            'pnl': strat_pnl[s],
            'pnl_change': strat_pnl_change[s],
            'commission': strat_commission[s],
        }
        for s in sorted(strat_keys)
    ]
    strat_total_spread_pos = sum(r['spread_pos'] for r in strategy_summary if r['spread_pos'] is not None) or None
    strat_total_pnl = sum(r['pnl'] for r in strategy_summary if r['pnl'] is not None) or None
    strat_total_pnl_change = sum(r['pnl_change'] for r in strategy_summary if r['pnl_change'] is not None) or None
    strat_total_commission = sum(r['commission'] for r in strategy_summary)

    # ── By Trade Code ────────────────────────────────────────────────────
    grp_lots = defaultdict(int)
    grp_pnl = defaultdict(lambda: None)
    grp_pnl_change = defaultdict(lambda: None)
    grp_commission = defaultdict(float)
    grp_delta = {}
    grp_price_x_lots = defaultdict(float)
    grp_abs_lots = defaultdict(float)
    grp_settlement = {}

    for pos in all_positions:
        if selected_book_pivot and pos.data.get('Book__c') not in selected_book_pivot:
            continue
        if selected_status_pivot and pos.data.get('Realised__c') not in selected_status_pivot:
            continue

        contract_key = build_contract_key(pos.data)
        if not contract_key:
            continue
        trade_code = pos.data.get('Trade_Code__c') or ''
        if not trade_code:
            continue
        trade_id = pos.data.get('Trade_Key__c') or ''
        group = pos.data.get('Trade_Group__c') or ''
        gkey = (trade_code, trade_id, group, contract_key)

        long_ = pos.data.get('Long__c') or 0
        short_ = pos.data.get('Short__c') or 0
        ls = long_ + short_
        grp_lots[gkey] += ls
        commission = pos.commission
        grp_commission[gkey] += commission
        is_option = bool(pos.data.get('Put_Call_2__c') and pos.data.get('Strike__c') is not None)
        if gkey not in grp_delta:
            if contract_key in market and market[contract_key].delta is not None:
                grp_delta[gkey] = market[contract_key].delta
            elif is_option:
                grp_delta[gkey] = 0
            else:
                grp_delta[gkey] = 1.0

        mkt = prices.get(contract_key)
        mkt2 = prices2.get(contract_key)
        if mkt is None and is_option:
            mkt = 0
        trade_price = pos.data.get('Price__c')
        if trade_price is not None:
            grp_price_x_lots[gkey] += trade_price * abs(ls)
            grp_abs_lots[gkey] += abs(ls)
        if gkey not in grp_settlement:
            grp_settlement[gkey] = prices.get(contract_key)
        mult = LOT_MULTIPLIERS.get(pos.data.get('Commodity_Name__c', ''))
        if mkt is not None and trade_price is not None and mult:
            grp_pnl[gkey] = (grp_pnl[gkey] or 0) + (mkt - trade_price) * ls * mult
        pnl_ch = _pos_pnl_change(mkt, mkt2, mult, ls, trade_price, commission, pos.data.get('Trade_Date__c'), latest_date)
        if pnl_ch is not None:
            grp_pnl_change[gkey] = (grp_pnl_change[gkey] or 0) + pnl_ch

    pivot_data = {}
    for gkey in sorted(grp_lots.keys()):
        tc, ti, gr, ck = gkey
        net = grp_lots[gkey]
        avg_price = (grp_price_x_lots[gkey] / grp_abs_lots[gkey]) if (grp_abs_lots[gkey] and net != 0) else 0
        sett = (grp_settlement.get(gkey) or 0) if net != 0 else 0
        pivot_data.setdefault(tc, {}).setdefault(ti, {}).setdefault(gr, []).append({
            'contract': ck,
            'net_lots': net,
            'delta': grp_delta.get(gkey),
            'trade_price': avg_price,
            'settlement': sett,
            'pnl': grp_pnl[gkey],
            'pnl_change': grp_pnl_change[gkey],
            'commission': grp_commission[gkey],
            '_pxl': grp_price_x_lots[gkey],
            '_sxl': (grp_settlement.get(gkey) or 0) * grp_abs_lots[gkey],
            '_abs': grp_abs_lots[gkey],
        })

    def _wavg(pxl, abs_l):
        return pxl / abs_l if abs_l else 0

    pivot_rows = []
    for tc, ti_map in pivot_data.items():
        sub_lots = 0; sub_pnl = None; sub_pnl_change = None; sub_commission = 0; sub_pos = None
        sub_pxl = 0; sub_sxl = 0; sub_abs = 0
        for ti, gr_map in ti_map.items():
            for gr, contracts in gr_map.items():
                for row in contracts:
                    sub_lots += row['net_lots']
                    sub_commission += row['commission']
                    sub_pxl += row['_pxl']; sub_sxl += row['_sxl']; sub_abs += row['_abs']
                    if row['pnl'] is not None:
                        sub_pnl = (sub_pnl or 0) + row['pnl']
                    if row['pnl_change'] is not None:
                        sub_pnl_change = (sub_pnl_change or 0) + row['pnl_change']
                    if row['delta'] is not None:
                        sub_pos = (sub_pos or 0) + row['delta'] * row['net_lots']
        pivot_rows.append({
            'level': 0, 'label': tc or '—',
            'net_lots': sub_lots, 'pnl': sub_pnl, 'pnl_change': sub_pnl_change,
            'commission': sub_commission, 'position': sub_pos,
            'trade_price': _wavg(sub_pxl, sub_abs), 'settlement': _wavg(sub_sxl, sub_abs),
        })
        for ti, gr_map in ti_map.items():
            ti_lots = 0; ti_pnl = None; ti_pnl_change = None; ti_commission = 0; ti_pos = None
            ti_pxl = 0; ti_sxl = 0; ti_abs = 0
            for gr, contracts in gr_map.items():
                for row in contracts:
                    ti_lots += row['net_lots']
                    ti_commission += row['commission']
                    ti_pxl += row['_pxl']; ti_sxl += row['_sxl']; ti_abs += row['_abs']
                    if row['pnl'] is not None:
                        ti_pnl = (ti_pnl or 0) + row['pnl']
                    if row['pnl_change'] is not None:
                        ti_pnl_change = (ti_pnl_change or 0) + row['pnl_change']
                    if row['delta'] is not None:
                        ti_pos = (ti_pos or 0) + row['delta'] * row['net_lots']
            pivot_rows.append({'level': 1, 'label': ti or '—',
                               'net_lots': ti_lots, 'pnl': ti_pnl, 'pnl_change': ti_pnl_change,
                               'commission': ti_commission, 'position': ti_pos,
                               'trade_price': _wavg(ti_pxl, ti_abs), 'settlement': _wavg(ti_sxl, ti_abs)})
            for gr, contracts in gr_map.items():
                gr_lots = sum(r['net_lots'] for r in contracts)
                gr_pnl_vals = [r['pnl'] for r in contracts if r['pnl'] is not None]
                gr_pnl = sum(gr_pnl_vals) if gr_pnl_vals else None
                gr_pnl_change_vals = [r['pnl_change'] for r in contracts if r['pnl_change'] is not None]
                gr_pnl_change = sum(gr_pnl_change_vals) if gr_pnl_change_vals else None
                gr_commission = sum(r['commission'] for r in contracts)
                gr_pos_vals = [r['delta'] * r['net_lots'] for r in contracts if r['delta'] is not None]
                gr_pos = sum(gr_pos_vals) if gr_pos_vals else None
                g_pxl = sum(r['_pxl'] for r in contracts)
                g_sxl = sum(r['_sxl'] for r in contracts)
                g_abs = sum(r['_abs'] for r in contracts)
                pivot_rows.append({'level': 2, 'label': gr or '—',
                                   'net_lots': gr_lots, 'pnl': gr_pnl, 'pnl_change': gr_pnl_change,
                                   'commission': gr_commission, 'position': gr_pos,
                                   'trade_price': _wavg(g_pxl, g_abs), 'settlement': _wavg(g_sxl, g_abs)})
                for row in contracts:
                    pivot_rows.append({'level': 3, **row})

    # ── By Trader ────────────────────────────────────────────────────────
    tdr_lots = defaultdict(int)
    tdr_pnl = defaultdict(lambda: None)
    tdr_pnl_change = defaultdict(lambda: None)
    tdr_commission = defaultdict(float)
    tdr_delta = {}

    for pos in all_positions:
        if selected_book_trader and pos.data.get('Book__c') not in selected_book_trader:
            continue
        if selected_status_trader and pos.data.get('Realised__c') not in selected_status_trader:
            continue
        if selected_tradecode_trader and pos.data.get('Trade_Code__c') not in selected_tradecode_trader:
            continue

        trader = pos.data.get('Trader__c') or ''
        trade_id = pos.data.get('Trade_Key__c') or ''
        group = pos.data.get('Trade_Group__c') or ''
        contract_key = build_contract_key(pos.data)
        tkey = (trader, trade_id, group, contract_key)

        long_ = pos.data.get('Long__c') or 0
        short_ = pos.data.get('Short__c') or 0
        ls = long_ + short_
        tdr_lots[tkey] += ls
        commission = pos.commission
        tdr_commission[tkey] += commission
        is_option = bool(pos.data.get('Put_Call_2__c') and pos.data.get('Strike__c') is not None)
        if tkey not in tdr_delta:
            if contract_key in market and market[contract_key].delta is not None:
                tdr_delta[tkey] = market[contract_key].delta
            elif is_option:
                tdr_delta[tkey] = 0
            else:
                tdr_delta[tkey] = 1.0

        mkt = prices.get(contract_key)
        mkt2 = prices2.get(contract_key)
        if mkt is None and is_option:
            mkt = 0
        trade_price = pos.data.get('Price__c')
        mult = LOT_MULTIPLIERS.get(pos.data.get('Commodity_Name__c', ''))
        if mkt is not None and trade_price is not None and mult:
            tdr_pnl[tkey] = (tdr_pnl[tkey] or 0) + (mkt - trade_price) * ls * mult
        pnl_ch = _pos_pnl_change(mkt, mkt2, mult, ls, trade_price, commission, pos.data.get('Trade_Date__c'), latest_date)
        if pnl_ch is not None:
            tdr_pnl_change[tkey] = (tdr_pnl_change[tkey] or 0) + pnl_ch

    trader_data = {}
    for tkey in sorted(tdr_lots.keys()):
        tr, ti, gr, ck = tkey
        trader_data.setdefault(tr, {}).setdefault(ti, {}).setdefault(gr, []).append({
            'contract': ck,
            'net_lots': tdr_lots[tkey],
            'delta': tdr_delta.get(tkey),
            'pnl': tdr_pnl[tkey],
            'pnl_change': tdr_pnl_change[tkey],
            'commission': tdr_commission[tkey],
        })

    trader_rows = []
    for tr, ti_map in trader_data.items():
        sub_lots = 0; sub_pnl = None; sub_pnl_change = None; sub_commission = 0; sub_pos = None
        for ti, gr_map in ti_map.items():
            for gr, contracts in gr_map.items():
                for row in contracts:
                    sub_lots += row['net_lots']
                    sub_commission += row['commission']
                    if row['pnl'] is not None:
                        sub_pnl = (sub_pnl or 0) + row['pnl']
                    if row['pnl_change'] is not None:
                        sub_pnl_change = (sub_pnl_change or 0) + row['pnl_change']
                    if row['delta'] is not None:
                        sub_pos = (sub_pos or 0) + row['delta'] * row['net_lots']
        trader_rows.append({
            'level': 0, 'label': tr or '—',
            'net_lots': sub_lots, 'pnl': sub_pnl, 'pnl_change': sub_pnl_change,
            'commission': sub_commission, 'position': sub_pos,
        })
        for ti, gr_map in ti_map.items():
            ti_lots = 0; ti_pnl = None; ti_pnl_change = None; ti_commission = 0; ti_pos = None
            for gr, contracts in gr_map.items():
                for row in contracts:
                    ti_lots += row['net_lots']
                    ti_commission += row['commission']
                    if row['pnl'] is not None:
                        ti_pnl = (ti_pnl or 0) + row['pnl']
                    if row['pnl_change'] is not None:
                        ti_pnl_change = (ti_pnl_change or 0) + row['pnl_change']
                    if row['delta'] is not None:
                        ti_pos = (ti_pos or 0) + row['delta'] * row['net_lots']
            trader_rows.append({'level': 1, 'label': ti or '—',
                                'net_lots': ti_lots, 'pnl': ti_pnl, 'pnl_change': ti_pnl_change,
                                'commission': ti_commission, 'position': ti_pos})
            for gr, contracts in gr_map.items():
                gr_lots = sum(r['net_lots'] for r in contracts)
                gr_pnl_vals = [r['pnl'] for r in contracts if r['pnl'] is not None]
                gr_pnl = sum(gr_pnl_vals) if gr_pnl_vals else None
                gr_pnl_change_vals = [r['pnl_change'] for r in contracts if r['pnl_change'] is not None]
                gr_pnl_change = sum(gr_pnl_change_vals) if gr_pnl_change_vals else None
                gr_commission = sum(r['commission'] for r in contracts)
                gr_pos_vals = [r['delta'] * r['net_lots'] for r in contracts if r['delta'] is not None]
                gr_pos = sum(gr_pos_vals) if gr_pos_vals else None
                trader_rows.append({'level': 2, 'label': gr or '—',
                                    'net_lots': gr_lots, 'pnl': gr_pnl, 'pnl_change': gr_pnl_change,
                                    'commission': gr_commission, 'position': gr_pos})
                for row in contracts:
                    trader_rows.append({'level': 3, **row})

    trader_total_lots = sum(r['net_lots'] for r in trader_rows if r['level'] == 3)
    trader_total_pnl = sum(r['pnl'] for r in trader_rows if r['level'] == 3 and r['pnl'] is not None) or None
    trader_total_pnl_change = sum(r['pnl_change'] for r in trader_rows if r['level'] == 3 and r['pnl_change'] is not None) or None
    trader_total_commission = sum(r['commission'] for r in trader_rows if r['level'] == 3)
    trader_has_delta = any(r['delta'] is not None for r in trader_rows if r['level'] == 3)
    trader_total_pos = sum(r['delta'] * r['net_lots'] for r in trader_rows if r['level'] == 3 and r['delta'] is not None) if trader_has_delta else None

    # ── By Spread ────────────────────────────────────────────────────────
    sprd_lots = defaultdict(int)
    sprd_pnl = defaultdict(lambda: None)
    sprd_pnl_change = defaultdict(lambda: None)
    sprd_commission = defaultdict(float)
    sprd_delta = {}
    sprd_spread_pos = defaultdict(lambda: None)
    sprd_price_x_lots = defaultdict(float)
    sprd_abs_lots = defaultdict(float)
    sprd_sett_x_lots = defaultdict(float)

    for pos in all_positions:
        if selected_book_spread and pos.data.get('Book__c') not in selected_book_spread:
            continue
        if selected_status_spread and pos.data.get('Realised__c') not in selected_status_spread:
            continue

        spread = (pos.spread or '').strip()
        if not spread:
            continue
        trade_code = pos.data.get('Trade_Code__c') or ''
        if not trade_code:
            continue
        trade_id = pos.data.get('Trade_Key__c') or ''
        group = pos.data.get('Trade_Group__c') or ''
        skey = (trade_code, trade_id, group, spread)

        contract_key = build_contract_key(pos.data)
        long_ = pos.data.get('Long__c') or 0
        short_ = pos.data.get('Short__c') or 0
        ls = long_ + short_
        sprd_lots[skey] += ls
        commission = pos.commission
        sprd_commission[skey] += commission
        is_option = bool(pos.data.get('Put_Call_2__c') and pos.data.get('Strike__c') is not None)
        if skey not in sprd_delta:
            if contract_key in market and market[contract_key].delta is not None:
                sprd_delta[skey] = market[contract_key].delta
            elif is_option:
                sprd_delta[skey] = 0
            else:
                sprd_delta[skey] = 1.0

        mkt = prices.get(contract_key)
        mkt2 = prices2.get(contract_key)
        if mkt is None and is_option:
            mkt = 0
        trade_price = pos.data.get('Price__c')
        if trade_price is not None:
            sprd_price_x_lots[skey] += trade_price * abs(ls)
            sprd_abs_lots[skey] += abs(ls)
        sett_px = prices.get(contract_key)
        if sett_px is not None:
            sprd_sett_x_lots[skey] += sett_px * abs(ls)
        mult = LOT_MULTIPLIERS.get(pos.data.get('Commodity_Name__c', ''))
        if mkt is not None and trade_price is not None and mult:
            sprd_pnl[skey] = (sprd_pnl[skey] or 0) + (mkt - trade_price) * ls * mult
        pnl_ch = _pos_pnl_change(mkt, mkt2, mult, ls, trade_price, commission, pos.data.get('Trade_Date__c'), latest_date)
        if pnl_ch is not None:
            sprd_pnl_change[skey] = (sprd_pnl_change[skey] or 0) + pnl_ch

        instrument = (pos.instrument or '').strip()
        if instrument == 'Spread':
            contract_last3 = (pos.data.get('Contract__c') or '').replace(' ', '')[-3:]
            spread_last3 = spread[-3:]
            if contract_last3 == spread_last3:
                sp_contrib = 0.0
            else:
                mp = market.get(contract_key)
                is_option_sp = bool(pos.data.get('Put_Call_2__c') and pos.data.get('Strike__c') is not None)
                d_val = resolve_delta(mp, price_source)
                if d_val is not None:
                    delta = d_val
                elif is_option_sp:
                    delta = 0
                else:
                    delta = 1.0
                sp_contrib = ls * delta
            if sp_contrib is not None:
                sprd_spread_pos[skey] = (sprd_spread_pos[skey] or 0) + sp_contrib

    spread_data = {}
    for skey in sorted(sprd_lots.keys()):
        tc, ti, gr, sp = skey
        abs_l = sprd_abs_lots[skey]
        avg_price = (sprd_price_x_lots[skey] / abs_l) if abs_l else 0
        avg_sett = (sprd_sett_x_lots[skey] / abs_l) if abs_l else 0
        spread_data.setdefault(tc, {}).setdefault(ti, {}).setdefault(gr, []).append({
            'spread': sp,
            'net_lots': sprd_lots[skey],
            'delta': sprd_delta.get(skey),
            'pnl': sprd_pnl[skey],
            'pnl_change': sprd_pnl_change[skey],
            'commission': sprd_commission[skey],
            'spread_pos': sprd_spread_pos.get(skey),
            'trade_price': avg_price,
            'settlement': avg_sett,
            '_pxl': sprd_price_x_lots[skey],
            '_sxl': sprd_sett_x_lots[skey],
            '_abs': abs_l,
        })

    spread_rows = []
    for tc, ti_map in spread_data.items():
        sub_lots = 0; sub_pnl = None; sub_pnl_change = None; sub_commission = 0; sub_spread_pos = None
        for ti, gr_map in ti_map.items():
            for gr, spreads in gr_map.items():
                for row in spreads:
                    sub_lots += row['net_lots']
                    sub_commission += row['commission']
                    if row['pnl'] is not None:
                        sub_pnl = (sub_pnl or 0) + row['pnl']
                    if row['pnl_change'] is not None:
                        sub_pnl_change = (sub_pnl_change or 0) + row['pnl_change']
                    if row['spread_pos'] is not None:
                        sub_spread_pos = (sub_spread_pos or 0) + row['spread_pos']
        spread_rows.append({
            'level': 0, 'label': tc or '—',
            'net_lots': sub_lots, 'pnl': sub_pnl, 'pnl_change': sub_pnl_change,
            'commission': sub_commission, 'spread_pos': sub_spread_pos,
        })
        for ti, gr_map in ti_map.items():
            ti_lots = 0; ti_pnl = None; ti_pnl_change = None; ti_commission = 0; ti_sp = None
            for gr, spreads in gr_map.items():
                for row in spreads:
                    ti_lots += row['net_lots']
                    ti_commission += row['commission']
                    if row['pnl'] is not None:
                        ti_pnl = (ti_pnl or 0) + row['pnl']
                    if row['pnl_change'] is not None:
                        ti_pnl_change = (ti_pnl_change or 0) + row['pnl_change']
                    if row['spread_pos'] is not None:
                        ti_sp = (ti_sp or 0) + row['spread_pos']
            spread_rows.append({'level': 1, 'label': ti or '—',
                                'net_lots': ti_lots, 'pnl': ti_pnl, 'pnl_change': ti_pnl_change,
                                'commission': ti_commission, 'spread_pos': ti_sp})
            for gr, spreads in gr_map.items():
                gr_lots = sum(r['net_lots'] for r in spreads)
                gr_pnl_vals = [r['pnl'] for r in spreads if r['pnl'] is not None]
                gr_pnl = sum(gr_pnl_vals) if gr_pnl_vals else None
                gr_pnl_change_vals = [r['pnl_change'] for r in spreads if r['pnl_change'] is not None]
                gr_pnl_change = sum(gr_pnl_change_vals) if gr_pnl_change_vals else None
                gr_commission = sum(r['commission'] for r in spreads)
                gr_sp_vals = [r['spread_pos'] for r in spreads if r['spread_pos'] is not None]
                gr_sp = sum(gr_sp_vals) if gr_sp_vals else None
                spread_rows.append({'level': 2, 'label': gr or '—',
                                    'net_lots': gr_lots, 'pnl': gr_pnl, 'pnl_change': gr_pnl_change,
                                    'commission': gr_commission, 'spread_pos': gr_sp})
                for row in spreads:
                    spread_rows.append({'level': 3, **row})

    # ── Open ST1 Pivot ───────────────────────────────────────────────────
    # 5-level drill-down: Realised__c → Trader → Trade ID → Trade Group → Contract
    o_lots = defaultdict(int)
    o_pnl = defaultdict(lambda: None)
    o_pnl_change = defaultdict(lambda: None)
    o_commission = defaultdict(float)
    o_delta = {}
    o_price_x_lots = defaultdict(float)
    o_abs_lots = defaultdict(float)
    o_settlement = {}

    for pos in all_positions:
        if selected_book_openst1 and pos.data.get('Book__c') not in selected_book_openst1:
            continue
        if selected_tradecode_openst1 and pos.data.get('Trade_Code__c') not in selected_tradecode_openst1:
            continue

        contract_key = build_contract_key(pos.data)
        if not contract_key:
            continue
        realised = pos.data.get('Realised__c') or ''
        trader = pos.data.get('Trader__c') or ''
        trade_id = pos.data.get('Trade_Key__c') or ''
        group = pos.data.get('Trade_Group__c') or ''
        okey = (realised, trader, trade_id, group, contract_key)

        long_ = pos.data.get('Long__c') or 0
        short_ = pos.data.get('Short__c') or 0
        ls = long_ + short_
        o_lots[okey] += ls
        commission = pos.commission
        o_commission[okey] += commission
        is_option = bool(pos.data.get('Put_Call_2__c') and pos.data.get('Strike__c') is not None)
        if okey not in o_delta:
            if contract_key in market and market[contract_key].delta is not None:
                o_delta[okey] = market[contract_key].delta
            elif is_option:
                o_delta[okey] = 0
            else:
                o_delta[okey] = 1.0

        mkt = prices.get(contract_key)
        mkt2 = prices2.get(contract_key)
        if mkt is None and is_option:
            mkt = 0
        trade_price = pos.data.get('Price__c')
        if trade_price is not None:
            o_price_x_lots[okey] += trade_price * abs(ls)
            o_abs_lots[okey] += abs(ls)
        if okey not in o_settlement:
            o_settlement[okey] = prices.get(contract_key)
        mult = LOT_MULTIPLIERS.get(pos.data.get('Commodity_Name__c', ''))
        if mkt is not None and trade_price is not None and mult:
            o_pnl[okey] = (o_pnl[okey] or 0) + (mkt - trade_price) * ls * mult
        pnl_ch = _pos_pnl_change(mkt, mkt2, mult, ls, trade_price, commission, pos.data.get('Trade_Date__c'), latest_date)
        if pnl_ch is not None:
            o_pnl_change[okey] = (o_pnl_change[okey] or 0) + pnl_ch

    def _wavg_o(pxl, abs_l):
        return pxl / abs_l if abs_l else 0

    openst1_data = {}
    for okey in sorted(o_lots.keys()):
        rl, tr, ti, gr, ck = okey
        net = o_lots[okey]
        avg_price = (o_price_x_lots[okey] / o_abs_lots[okey]) if (o_abs_lots[okey] and net != 0) else 0
        sett = (o_settlement.get(okey) or 0) if net != 0 else 0
        openst1_data.setdefault(rl, {}).setdefault(tr, {}).setdefault(ti, {}).setdefault(gr, []).append({
            'contract': ck,
            'net_lots': net,
            'delta': o_delta.get(okey),
            'trade_price': avg_price,
            'settlement': sett,
            'pnl': o_pnl[okey],
            'pnl_change': o_pnl_change[okey],
            'commission': o_commission[okey],
            '_pxl': o_price_x_lots[okey],
            '_sxl': (o_settlement.get(okey) or 0) * o_abs_lots[okey],
            '_abs': o_abs_lots[okey],
        })

    def _agg_init():
        return {'lots': 0, 'pnl': None, 'pnl_change': None, 'commission': 0, 'pos': None,
                'pxl': 0, 'sxl': 0, 'abs': 0}

    def _agg_add(a, row):
        a['lots'] += row['net_lots']
        a['commission'] += row['commission']
        a['pxl'] += row['_pxl']; a['sxl'] += row['_sxl']; a['abs'] += row['_abs']
        if row['pnl'] is not None:
            a['pnl'] = (a['pnl'] or 0) + row['pnl']
        if row['pnl_change'] is not None:
            a['pnl_change'] = (a['pnl_change'] or 0) + row['pnl_change']
        if row['delta'] is not None:
            a['pos'] = (a['pos'] or 0) + row['delta'] * row['net_lots']

    def _agg_row(level, label, a):
        return {'level': level, 'label': label,
                'net_lots': a['lots'], 'pnl': a['pnl'], 'pnl_change': a['pnl_change'],
                'commission': a['commission'], 'position': a['pos'],
                'trade_price': _wavg_o(a['pxl'], a['abs']),
                'settlement': _wavg_o(a['sxl'], a['abs'])}

    openst1_rows = []
    for rl, tr_map in openst1_data.items():
        l0 = _agg_init()
        for tr, ti_map in tr_map.items():
            for ti, gr_map in ti_map.items():
                for gr, contracts in gr_map.items():
                    for row in contracts:
                        _agg_add(l0, row)
        openst1_rows.append(_agg_row(0, rl or '—', l0))
        for tr, ti_map in tr_map.items():
            l1 = _agg_init()
            for ti, gr_map in ti_map.items():
                for gr, contracts in gr_map.items():
                    for row in contracts:
                        _agg_add(l1, row)
            openst1_rows.append(_agg_row(1, tr or '—', l1))
            for ti, gr_map in ti_map.items():
                l2 = _agg_init()
                for gr, contracts in gr_map.items():
                    for row in contracts:
                        _agg_add(l2, row)
                openst1_rows.append(_agg_row(2, ti or '—', l2))
                for gr, contracts in gr_map.items():
                    l3 = _agg_init()
                    for row in contracts:
                        _agg_add(l3, row)
                    openst1_rows.append(_agg_row(3, gr or '—', l3))
                    for row in contracts:
                        openst1_rows.append({'level': 4, **row})

    openst1_total_lots = sum(r['net_lots'] for r in openst1_rows if r['level'] == 4)
    openst1_total_pnl = sum(r['pnl'] for r in openst1_rows if r['level'] == 4 and r['pnl'] is not None) or None
    openst1_total_pnl_change = sum(r['pnl_change'] for r in openst1_rows if r['level'] == 4 and r['pnl_change'] is not None) or None
    openst1_total_commission = sum(r['commission'] for r in openst1_rows if r['level'] == 4)
    openst1_has_delta = any(r['delta'] is not None for r in openst1_rows if r['level'] == 4)
    openst1_total_pos = sum(r['delta'] * r['net_lots'] for r in openst1_rows if r['level'] == 4 and r['delta'] is not None) if openst1_has_delta else None

    spread_total_pnl = sum(r['pnl'] for r in spread_rows if r['level'] == 3 and r['pnl'] is not None) or None
    spread_total_pnl_change = sum(r['pnl_change'] for r in spread_rows if r['level'] == 3 and r['pnl_change'] is not None) or None
    spread_total_commission = sum(r['commission'] for r in spread_rows if r['level'] == 3)
    spread_has_spread_pos = any(r['spread_pos'] is not None for r in spread_rows if r['level'] == 3)
    spread_total_spread_pos = sum(r['spread_pos'] for r in spread_rows if r['level'] == 3 and r['spread_pos'] is not None) if spread_has_spread_pos else None

    pivot_total_lots = sum(r['net_lots'] for r in pivot_rows if r['level'] == 3)
    pivot_total_pnl = sum(r['pnl'] for r in pivot_rows if r['level'] == 3 and r['pnl'] is not None) or None
    pivot_total_pnl_change = sum(r['pnl_change'] for r in pivot_rows if r['level'] == 3 and r['pnl_change'] is not None) or None
    pivot_total_commission = sum(r['commission'] for r in pivot_rows if r['level'] == 3)
    pivot_has_delta = any(r['delta'] is not None for r in pivot_rows if r['level'] == 3)
    pivot_total_pos = sum(r['delta'] * r['net_lots'] for r in pivot_rows if r['level'] == 3 and r['delta'] is not None) if pivot_has_delta else None

    return render_template("summary.html", summary=summary,
                           strategy_summary=strategy_summary,
                           strat_total_spread_pos=strat_total_spread_pos,
                           strat_total_pnl=strat_total_pnl,
                           strat_total_pnl_change=strat_total_pnl_change,
                           strat_total_commission=strat_total_commission,
                           spread_rows=spread_rows,
                           spread_total_pnl=spread_total_pnl,
                           spread_total_pnl_change=spread_total_pnl_change,
                           spread_total_commission=spread_total_commission,
                           spread_total_spread_pos=spread_total_spread_pos,
                           selected_book_spread=selected_book_spread,
                           selected_status_spread=selected_status_spread,
                           pivot_rows=pivot_rows,
                           pivot_total_lots=pivot_total_lots,
                           pivot_total_pnl=pivot_total_pnl,
                           pivot_total_pnl_change=pivot_total_pnl_change,
                           pivot_total_commission=pivot_total_commission,
                           pivot_total_pos=pivot_total_pos,
                           trader_rows=trader_rows,
                           trader_total_lots=trader_total_lots,
                           trader_total_pnl=trader_total_pnl,
                           trader_total_pnl_change=trader_total_pnl_change,
                           trader_total_commission=trader_total_commission,
                           trader_total_pos=trader_total_pos,
                           books=books, statuses=statuses, trade_codes=trade_codes,
                           selected_book=selected_book, selected_status=selected_status,
                           selected_book_pivot=selected_book_pivot,
                           selected_status_pivot=selected_status_pivot,
                           selected_book_trader=selected_book_trader,
                           selected_status_trader=selected_status_trader,
                           selected_tradecode_trader=selected_tradecode_trader,
                           openst1_rows=openst1_rows,
                           openst1_total_lots=openst1_total_lots,
                           openst1_total_pnl=openst1_total_pnl,
                           openst1_total_pnl_change=openst1_total_pnl_change,
                           openst1_total_commission=openst1_total_commission,
                           openst1_total_pos=openst1_total_pos,
                           selected_book_openst1=selected_book_openst1,
                           selected_tradecode_openst1=selected_tradecode_openst1,
                           active_tab=active_tab,
                           price_source=price_source)

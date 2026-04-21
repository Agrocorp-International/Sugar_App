"""Taylor-series PnL attribution for the daily snapshot.

Splits the daily PnL change on the Alpha (Spec) book into:

    Total PnL change = Explained + Model Residual + Other PnL

      Explained      = Delta + Gamma + Vega + Theta       (start-of-period Greeks)
      Model Residual = actual leg revaluation - Explained (smile, vanna, volga, higher-order)
      Other PnL      = total_pnl change - attributed-leg change
                       (physical M2M, FFA, realised closes, whites/raws hedges,
                        new trades opened since snapshot, positions closed since)

Design notes
------------
* Per-leg state (F_prev, σ_prev, TTM_prev, Greeks_prev, value_prev) is frozen
  into the snapshot JSON at snapshot time, capturing the CURRENT market state
  as of that moment. When the next sett-1 arrives, the snapshot's F_prev
  becomes "yesterday" relative to the new settlement.
* σ_prev is back-solved from the option's current settlement price via
  Black-76 bisection. Price is strictly monotonic in σ for all moneyness,
  so this is robust.
* Scope is Alpha book (Book__c='Spec', Realised__c='Unrealised') only.
  Whites/Raws hedge legs, physical M2M, FFA, and realised closes fold into
  Other PnL by subtraction.

Conventions (verified against services/tradestation.py)
-------------------------------------------------------
* Vega: ∂V/∂σ per 1.00 vol (decimal). Use raw Δσ in decimal — no /100.
* Theta: ∂V/∂t per calendar day, already signed (negative for long calls).
  Bucket = theta_prev * Δt_days * lots * mult — **no negation**.
* F, K, ΔF in c/lb; multiplier (1120 for SB, 50 for SW) converts to USD.
"""
from datetime import datetime

from models.db import MarketPrice, TradePosition
from routes.positions import build_contract_key, LOT_MULTIPLIERS_BY_PREFIX
from routes.prices import _OPTION_RE, _build_expiry_map
from services.tradestation import (
    _black76_delta, _black76_gamma, _black76_vega, _black76_theta,
    _implied_vol_bisect, _RISK_FREE_RATE_FALLBACK, _fetch_sofr,
)


# ── Snapshot: build per-leg frozen state ───────────────────────────────────

def build_attribution_legs(snapshotted_at):
    """Return ``(legs, meta)`` for the daily snapshot.

    Iterates unrealised Spec positions, pulls yesterday's market state from
    MarketPrice.settlement2 / delta2, back-solves σ_prev, computes Greeks at
    yesterday's state, and returns a list of frozen leg dicts plus metadata.

    Any leg that can't be priced at yesterday's state (missing settlement2,
    back-solve fails, expiry missing) is omitted — its contribution then lands
    in Other PnL via the render-time subtraction.
    """
    expiry_map = _build_expiry_map()
    price_rows = {mp.contract.replace(" ", "").upper(): mp for mp in MarketPrice.query.all()}
    positions = TradePosition.query.all()

    try:
        rate, _ = _fetch_sofr()
        r = rate if rate is not None else _RISK_FREE_RATE_FALLBACK
    except Exception:
        r = _RISK_FREE_RATE_FALLBACK

    legs = []
    excluded_list = []
    all_spec_sf_ids = []

    def _excluded(p, d, key, is_option, reason):
        excluded_list.append({
            "sf_id": p.sf_id,
            "contract": d.get("Contract__c"),
            "key": key,
            "is_option": is_option,
            "reason": reason,
        })

    for p in positions:
        d = p.data or {}
        if d.get("Book__c") != "Spec":
            continue
        all_spec_sf_ids.append(p.sf_id)
        if d.get("Realised__c") != "Unrealised":
            continue

        key = build_contract_key(d)
        m = _OPTION_RE.match(key)
        is_option = bool(m)
        prefix = (d.get("Contract__c") or "").replace(" ", "")[:2].upper()
        multiplier = LOT_MULTIPLIERS_BY_PREFIX.get(prefix)
        if not multiplier:
            _excluded(p, d, key, is_option, "no_multiplier")
            continue
        lots = float(d.get("Long__c") or 0) + float(d.get("Short__c") or 0)
        if lots == 0:
            continue

        if not m:
            # ── Futures leg: only F_prev needed; delta always 1.0 ──────────
            mp = price_rows.get(key)
            F_prev = mp.settlement if mp else None
            if F_prev is None:
                _excluded(p, d, key, False, "no_F_prev")
                continue
            legs.append({
                "sf_id": p.sf_id,
                "key": key,
                "is_option": False,
                "lots": lots,
                "multiplier": multiplier,
                "F_prev": F_prev,
                "value_prev": F_prev * lots * multiplier,
                "delta_prev": 1.0,
                "gamma_prev": 0.0,
                "vega_prev": 0.0,
                "theta_prev": 0.0,
                "sigma_prev": None,
                "ttm_prev": None,
                "strike": None,
                "is_call": None,
            })
            continue

        # ── Option leg ──────────────────────────────────────────────────────
        underlying_code, pc, strike_cents = m.group(1), m.group(2), m.group(3)
        K = int(strike_cents) / 100.0
        is_call = (pc == "C")

        under_mp = price_rows.get(underlying_code)
        opt_mp = price_rows.get(key)
        F_prev = under_mp.settlement if under_mp else None
        opt_price_prev = opt_mp.settlement if opt_mp else None
        expiry = expiry_map.get(underlying_code)
        if F_prev is None or opt_price_prev is None or expiry is None:
            _excluded(p, d, key, True, "missing_market_data")
            continue

        # TTM at snapshot time, calendar-day based via timestamp.
        expiry_dt = datetime.combine(expiry, datetime.min.time())
        ttm_prev = (expiry_dt - snapshotted_at).total_seconds() / (365.0 * 86400.0)
        if ttm_prev <= 0:
            _excluded(p, d, key, True, "expired")
            continue

        # Back-solve σ_prev from the option's prior settlement price. Price is
        # monotonic in σ regardless of moneyness, so this is robust.
        sigma_prev = _implied_vol_bisect(opt_price_prev, F_prev, K, ttm_prev, r, is_call)
        if sigma_prev is None or sigma_prev <= 0:
            _excluded(p, d, key, True, "sigma_backsolve_failed")
            continue

        try:
            delta_prev = _black76_delta(F_prev, K, ttm_prev, r, sigma_prev, is_call)
            gamma_prev = _black76_gamma(F_prev, K, ttm_prev, r, sigma_prev)
            vega_prev = _black76_vega(F_prev, K, ttm_prev, r, sigma_prev)
            theta_prev = _black76_theta(F_prev, K, ttm_prev, r, sigma_prev, is_call)
        except (ValueError, ZeroDivisionError):
            _excluded(p, d, key, True, "black76_failed")
            continue

        legs.append({
            "sf_id": p.sf_id,
            "key": key,
            "underlying_key": underlying_code,
            "is_option": True,
            "lots": lots,
            "multiplier": multiplier,
            "strike": K,
            "is_call": is_call,
            "F_prev": F_prev,
            "sigma_prev": sigma_prev,
            "ttm_prev": ttm_prev,
            "delta_prev": delta_prev,
            "gamma_prev": gamma_prev,
            "vega_prev": vega_prev,
            "theta_prev": theta_prev,
            "value_prev": opt_price_prev * lots * multiplier,
        })

    meta = {
        "risk_free_rate": r,
        "iv_source": "backsolved_from_current_settlement",
        "excluded_legs": len(excluded_list),
        "excluded_list": excluded_list,
        "expiry_by_underlying": {u: exp.isoformat() for u, exp in expiry_map.items()},
        # Full set of Spec sf_ids that existed at snapshot time. Used by
        # compute_attribution's second pass to precisely identify post-snap
        # new/closed futures that need Delta attribution. Without this, we'd
        # fall back to a Trade_Date__c date filter that misses back-dated
        # trades synced after the snapshot.
        "all_spec_sf_ids": all_spec_sf_ids,
    }
    return legs, meta


# ── Render time: apply Taylor attribution ─────────────────────────────────

def compute_attribution(snapshot, pnl_summary_today):
    """Return attribution dict for the dashboard, or None if snapshot lacks legs.

    Bucket contributions (USD):
      delta = delta_prev * ΔF * lots * mult
      gamma = 0.5 * gamma_prev * ΔF^2 * lots * mult
      vega  = vega_prev * Δσ * lots * mult                 (Δσ in decimal)
      theta = theta_prev * Δt_days * lots * mult           (theta per-day, signed)

    Residual & Other:
      actual_legs = sum(value_today - value_prev) over legs in both snapshots
      model_residual = actual_legs - explained
      other_pnl     = total_pnl_change - actual_legs
    """
    if not snapshot or not snapshot.data:
        return None
    legs = snapshot.data.get("attribution_legs")
    if not legs:
        return None
    meta = snapshot.data.get("attribution_meta") or {}
    r = meta.get("risk_free_rate") or _RISK_FREE_RATE_FALLBACK

    price_rows = {mp.contract.replace(" ", "").upper(): mp for mp in MarketPrice.query.all()}
    expiry_map = _build_expiry_map()
    now = datetime.utcnow()
    snap_time = snapshot.snapshotted_at
    dt_days = (now - snap_time).total_seconds() / 86400.0

    delta_sum = gamma_sum = vega_sum = theta_sum = 0.0
    actual_legs_sum = 0.0
    leg_count = 0
    missing_today = 0

    for leg in legs:
        lots = float(leg["lots"])
        mult = float(leg["multiplier"])
        F_prev = float(leg["F_prev"])

        if not leg.get("is_option"):
            # ── Futures leg ─────────────────────────────────────────────
            mp = price_rows.get(leg["key"])
            F_today = mp.settlement if mp else None
            if F_today is None:
                missing_today += 1
                continue
            dF = F_today - F_prev
            delta_sum += dF * lots * mult           # Δ = 1
            actual_legs_sum += dF * lots * mult
            leg_count += 1
            continue

        # ── Option leg ──────────────────────────────────────────────────
        underlying_key = leg.get("underlying_key")
        opt_mp = price_rows.get(leg["key"])
        under_mp = price_rows.get(underlying_key) if underlying_key else None
        F_today = under_mp.settlement if under_mp else None
        opt_price_today = opt_mp.settlement if opt_mp else None
        expiry = expiry_map.get(underlying_key)
        if F_today is None or opt_price_today is None or expiry is None:
            missing_today += 1
            continue

        K = float(leg["strike"])
        is_call = bool(leg["is_call"])
        sigma_prev = float(leg["sigma_prev"])
        ttm_prev = float(leg["ttm_prev"])

        expiry_dt = datetime.combine(expiry, datetime.min.time())
        ttm_today = (expiry_dt - now).total_seconds() / (365.0 * 86400.0)
        if ttm_today <= 0:
            missing_today += 1
            continue

        # Back-solve σ_today from current option price, same method as σ_prev —
        # keeps Δσ internally consistent and independent of the vendor IV source.
        sigma_today = _implied_vol_bisect(opt_price_today, F_today, K, ttm_today, r, is_call)
        if sigma_today is None or sigma_today <= 0:
            missing_today += 1
            continue

        dF = F_today - F_prev
        dSigma = sigma_today - sigma_prev

        delta_prev = float(leg["delta_prev"])
        gamma_prev = float(leg["gamma_prev"])
        vega_prev = float(leg["vega_prev"])
        theta_prev = float(leg["theta_prev"])

        delta_sum += delta_prev * dF * lots * mult
        gamma_sum += 0.5 * gamma_prev * (dF ** 2) * lots * mult
        vega_sum += vega_prev * dSigma * lots * mult
        theta_sum += theta_prev * dt_days * lots * mult

        value_today = opt_price_today * lots * mult
        actual_legs_sum += value_today - float(leg["value_prev"])
        leg_count += 1

    # Second pass: attribute new/closed FUTURES (post-snapshot activity) into Delta.
    # Futures have Γ=Vega=Θ=0, so their full PnL contribution is Delta. Treating
    # close-side records as new legs with F_prev=exit_price makes the closed-pair
    # math combine correctly with the snapshot leg's revaluation.
    #
    # Filter: only positions that did NOT exist in the Spec book at snapshot
    # time. Pre-existing realised pairs contribute zero to net_alpha_change
    # (settlement cancels within the pair on both days), so including them
    # would double-count their cumulative realised PnL into Delta.
    #
    # Prefer meta["all_spec_sf_ids"] (added 2026-04) — a precise sf_id filter.
    # Fall back to a Trade_Date__c >= as_of_date heuristic for legacy snapshots.
    # The fallback also excludes anything already in the frozen legs so a
    # same-day Unrealised leg isn't double-counted (main loop + second pass).
    all_spec_at_snap = set(meta.get("all_spec_sf_ids") or [])
    snap_sf_ids_from_legs = {leg["sf_id"] for leg in legs}
    as_of_str = snapshot.data.get("as_of_date") if snapshot and snapshot.data else None
    try:
        as_of_date = datetime.strptime(as_of_str[:10], "%Y-%m-%d").date() if as_of_str else None
    except (ValueError, TypeError):
        as_of_date = None
    for p in TradePosition.query.all():
        d = p.data or {}
        if d.get("Book__c") != "Spec":
            continue
        if d.get("Put_Call_2__c") or d.get("Strike__c") is not None:
            continue
        if all_spec_at_snap:
            if p.sf_id in all_spec_at_snap:
                continue
        else:
            if p.sf_id in snap_sf_ids_from_legs:
                continue
            if as_of_date is None:
                continue
            td_str = d.get("Trade_Date__c")
            if not td_str:
                continue
            try:
                td = datetime.strptime(td_str[:10], "%Y-%m-%d").date()
            except (ValueError, TypeError):
                continue
            if td < as_of_date:
                continue
        price = d.get("Price__c")
        lots = float(d.get("Long__c") or 0) + float(d.get("Short__c") or 0)
        if price is None or lots == 0:
            continue
        prefix = (d.get("Contract__c") or "").replace(" ", "")[:2].upper()
        fut_mult = LOT_MULTIPLIERS_BY_PREFIX.get(prefix)
        if not fut_mult:
            continue
        key = build_contract_key(d)
        mp = price_rows.get(key)
        F_today = mp.settlement if mp else None
        if F_today is None:
            missing_today += 1
            continue
        contribution = (F_today - float(price)) * lots * fut_mult
        delta_sum += contribution
        actual_legs_sum += contribution
        leg_count += 1

    explained = delta_sum + gamma_sum + vega_sum + theta_sum
    model_residual = actual_legs_sum - explained

    # Reconcile attribution to the summary table's Net Alpha Daily Chg.
    # Difference vs actual_legs_sum captures day-1 PnL on trades opened or
    # closed since snapshot — the piece Taylor on frozen legs can't see.
    net_alpha_change = None
    position_changes = None
    if (pnl_summary_today
            and pnl_summary_today.get("net_alpha_pnl") is not None
            and snapshot.data.get("net_alpha_pnl") is not None):
        net_alpha_change = pnl_summary_today["net_alpha_pnl"] - snapshot.data["net_alpha_pnl"]
        position_changes = net_alpha_change - actual_legs_sum

    return {
        "delta": delta_sum,
        "gamma": gamma_sum,
        "vega": vega_sum,
        "theta": theta_sum,
        "explained": explained,
        "model_residual": model_residual,
        "actual_attributed": actual_legs_sum,
        "position_changes": position_changes,
        "net_alpha_change": net_alpha_change,
        "leg_count": leg_count,
        "missing_today": missing_today,
        "excluded_at_snapshot": meta.get("excluded_legs", 0),
        "excluded_list": meta.get("excluded_list") or [],
    }

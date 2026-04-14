"""
Scenario IV calculation for the options payoff grid.

Custom house-rule IV model that reproduces the Excel Options sheet logic.
Not a standard market surface model (no sticky delta, SABR, or spline fitting).

Unit contract:
  Inputs:  spot_call_iv, spot_put_iv, current_iv_pct  -> percent (24.0 = 24%)
           skew  -> pct-points per 1.0 price unit (0.20 = 0.20 pct-pts)
  Output:  decimal IV (0.24 = 24%) ready for Black-76
"""
from datetime import date


def calculate_scenario_iv(
    option_type: str,
    current_iv_pct: float,
    strike: float,
    current_spot: float,
    valuation_date: date,
    expiry_date: date,
    scenario_date: date,
    scenario_price: float,
    spot_call_iv: float,
    spot_put_iv: float,
    skew: float,
    exp_param: float,
    debug: bool = False,
):
    """Compute scenario IV for one option leg at one grid point.

    Args:
        option_type:    "C" or "P" (matches Salesforce Put_Call_2__c)
        current_iv_pct: current market IV in percent, e.g. 24.0 = 24%
        strike:         option strike price
        current_spot:   current underlying futures settlement
        valuation_date: as-of / reference date
        expiry_date:    option expiry date
        scenario_date:  scenario grid date
        scenario_price: scenario underlying price (after delta shift)
        spot_call_iv:   base call IV assumption in percent, e.g. 24.0
        spot_put_iv:    base put IV assumption in percent, e.g. 24.0
        skew:           IV bump coefficient in pct-points per 1.0 price unit
        exp_param:      exponential smile parameter, e.g. 1.37
        debug:          if True, return dict with all intermediates

    Returns:
        float:  scenario IV as decimal (e.g. 0.24) ready for Black-76
        dict:   intermediate values + final IV when debug=True
        None:   if inputs are invalid (missing IV, bad exp_param, etc.)

    Example:
        current_iv_pct=24.0 -> return value ~0.24
    """
    # --- Input validation ---
    if option_type not in ("C", "P"):
        raise ValueError(f"option_type must be 'C' or 'P', got {option_type!r}")

    if current_iv_pct is None or exp_param is None or exp_param <= 0:
        return None

    # --- Layer A: base IV by option type ---
    base_iv_pct = spot_call_iv if option_type == "C" else spot_put_iv

    # --- Layer B: target smile using current spot (NOT scenario spot) ---
    smile_adj_pct = (exp_param ** abs(strike - current_spot)) * skew
    target_iv_pct = base_iv_pct + smile_adj_pct

    # --- Time weight (clamped 0-1) ---
    denom_days = (expiry_date - valuation_date).days
    if denom_days <= 0:
        time_weight = 1.0
    else:
        numer_days = (scenario_date - valuation_date).days
        time_weight = max(0.0, min(1.0, numer_days / denom_days))

    # --- Layer C: interpolate + price bump ---
    blended_iv_pct = current_iv_pct + (target_iv_pct - current_iv_pct) * time_weight
    price_bump_pct = abs(scenario_price - current_spot) * skew
    scenario_iv_pct_raw = blended_iv_pct + price_bump_pct

    # --- Floor in pct space, then convert to decimal ---
    scenario_iv_pct_floored = max(scenario_iv_pct_raw, 0.01)
    scenario_iv_decimal = scenario_iv_pct_floored / 100.0

    if debug:
        return {
            "base_iv_pct": base_iv_pct,
            "target_iv_pct": target_iv_pct,
            "current_iv_pct": current_iv_pct,
            "time_weight": time_weight,
            "price_bump_pct": price_bump_pct,
            "scenario_iv_pct_raw": scenario_iv_pct_raw,
            "scenario_iv_pct_floored": scenario_iv_pct_floored,
            "scenario_iv_decimal": scenario_iv_decimal,
        }

    return scenario_iv_decimal

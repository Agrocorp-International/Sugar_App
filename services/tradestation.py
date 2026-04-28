"""
TradeStation market data service for ICE sugar futures and options.

Supported products:
  SB = ICE Sugar No.11 (Raw Sugar)   — TradeStation root: SB
  SW = ICE White Sugar No.5 (London) — TradeStation root: CW

Symbol mapping is fully deterministic via _FUTURES_ROOT_MAP. No runtime
root probing or discovery is performed.

  Futures:  SBK26      -> SBK26
            SWK26      -> CWK26
  Options:  SBK26C1600 -> SBK26 C1600
            SWH26C500  -> CWH26 C500

Fetches PreviousClose (T-2 settlement) for all watched contracts and computes
IV and delta for options using the Black-76 model (bisection IV solver).

Recommended fetch time: after ICE settlement is fully published.
  ICE SB/SW settle ~14:30 ET  =  ~02:30 SGT (next morning).
  Safest to fetch after market close ET or before Singapore market open.
"""

import os
import re
import math
import datetime
import urllib.parse
import requests
from concurrent.futures import ThreadPoolExecutor, as_completed
from zoneinfo import ZoneInfo
from services.exchange_calendar import HOLIDAY_DATES

BASE_URL = "https://api.tradestation.com/v3/marketdata"

_EXCHANGE_TZ = ZoneInfo("America/New_York")
_SGT = ZoneInfo("Asia/Singapore")

_FUTURE_RE = re.compile(r'^(?:S[BW]|CT)[A-Z]\d{2}$')
_OPTION_RE  = re.compile(r'^((?:S[BW]|CT)[A-Z]\d{2})([CP])(\d+)$')
_RISK_FREE_RATE_FALLBACK = 0.045  # fallback if SOFR fetch fails

# Static mapping: internal 2-letter prefix → TradeStation root.
# Used for both futures and options symbol conversion.
# NOTE: CT TradeStation root is UNVERIFIED — must round-trip-test one known
# cotton futures + option symbol before enabling cotton prices fetch.
# If TradeStation uses a different root (e.g. "CT2"), update "CT" value below.
_FUTURES_ROOT_MAP = {
    "SB": "SB",   # ICE Sugar No.11
    "SW": "CW",   # ICE White Sugar No.5 (TradeStation uses CW)
    "CT": "CT",   # ICE Cotton #2 — PLACEHOLDER, verify before go-live
}

# Strike scale factor per product prefix.
# SB: strikes stored as hundredths of c/lb  (1600 → K = 16.00)
# SW: strikes stored as whole USD/ton        (500  → K = 500.0)
# CT: strikes stored as hundredths of c/lb   (8000 → K = 80.00)  — same as SB
_STRIKE_SCALE = {
    "SB": 0.01,
    "SW": 1.0,
    "CT": 0.01,
}


def _contract_prefix(contract):
    """Return the 2-letter product prefix (e.g. SB, SW, CT)."""
    if not contract or len(contract) < 2:
        return ""
    return contract[:2].upper()


def _is_ct_trading_session_open(now_utc):
    """Return True only during the official ICE Cotton trading session.

    Based on ICE product pages for Cotton No. 2 Futures / Options:
      New York trading hours = 9:00 PM to 2:20 PM next day.

    We gate by the session's trade date:
      - 00:00-14:20 ET belongs to the current trade date
      - 21:00-23:59 ET belongs to the next trade date
      - 14:20-21:00 ET is closed (post-close / pre-open only)

    A session is considered closed on weekends and on holiday trade dates.
    """
    now_et = now_utc.astimezone(_EXCHANGE_TZ)
    now_date = now_et.date()
    now_time = now_et.time()

    if now_time < datetime.time(14, 20):
        trade_date = now_date
        return trade_date.weekday() < 5 and trade_date not in HOLIDAY_DATES

    if now_time >= datetime.time(21, 0):
        trade_date = now_date + datetime.timedelta(days=1)
        return trade_date.weekday() < 5 and trade_date not in HOLIDAY_DATES

    return False


def _is_sb_trading_session_open(now_utc):
    """Return True only during the official ICE Sugar No. 11 trading session."""
    now_et = now_utc.astimezone(_EXCHANGE_TZ)
    trade_date = now_et.date()
    now_time = now_et.time()
    if trade_date.weekday() >= 5 or trade_date in HOLIDAY_DATES:
        return False
    return datetime.time(3, 30) <= now_time < datetime.time(13, 0)


def _is_sw_trading_session_open(now_utc):
    """Return True only during the official ICE White Sugar trading session."""
    now_et = now_utc.astimezone(_EXCHANGE_TZ)
    trade_date = now_et.date()
    now_time = now_et.time()
    if trade_date.weekday() >= 5 or trade_date in HOLIDAY_DATES:
        return False
    return datetime.time(3, 45) <= now_time < datetime.time(13, 0)


def _is_live_session_open(contract, now_utc):
    """Return True when the contract's official trading session is open."""
    prefix = _contract_prefix(contract)
    if prefix == "CT":
        return _is_ct_trading_session_open(now_utc)
    if prefix == "SB":
        return _is_sb_trading_session_open(now_utc)
    if prefix == "SW":
        return _is_sw_trading_session_open(now_utc)
    return True


# ── Risk-free rate ────────────────────────────────────────────────────────────

# SOFR updates once per business day; 6-hour TTL is comfortably safe.
_SOFR_TTL_SECONDS = 6 * 60 * 60
_sofr_cache = {}  # {pricing_date_key: (result_tuple, expires_at)}


def _fetch_sofr_uncached(pricing_date=None):
    """Fetch SOFR from FRED public CSV (no caching).
    If pricing_date (datetime.date) is given, returns the rate for that date,
    falling back to the most recent prior date available in FRED.
    Returns (rate_decimal, date_str). Falls back to (_RISK_FREE_RATE_FALLBACK, None) on error."""
    try:
        resp = requests.get(
            "https://fred.stlouisfed.org/graph/fredgraph.csv?id=SOFR",
            timeout=10,
        )
        resp.raise_for_status()
        rates = {}
        for line in resp.text.strip().splitlines():
            if line.startswith("DATE"):
                continue
            parts = line.split(",")
            if len(parts) == 2 and parts[1].strip() not in ("", "."):
                try:
                    rates[parts[0].strip()] = float(parts[1]) / 100
                except ValueError:
                    pass
        if not rates:
            return _RISK_FREE_RATE_FALLBACK, None
        if pricing_date is not None:
            target = pricing_date.strftime("%Y-%m-%d")
            if target in rates:
                return rates[target], target
            prior = sorted(d for d in rates if d <= target)
            if prior:
                closest = prior[-1]
                return rates[closest], closest
        latest = max(rates)
        return rates[latest], latest
    except Exception:
        pass
    return _RISK_FREE_RATE_FALLBACK, None


def _fetch_sofr(pricing_date=None):
    """TTL-cached wrapper around _fetch_sofr_uncached (6-hour refresh)."""
    import time
    key = pricing_date.isoformat() if pricing_date is not None else None
    now = time.time()
    entry = _sofr_cache.get(key)
    if entry is not None and now < entry[1]:
        return entry[0]
    result = _fetch_sofr_uncached(pricing_date)
    _sofr_cache[key] = (result, now + _SOFR_TTL_SECONDS)
    return result


# ── Contract helpers ──────────────────────────────────────────────────────────

def is_option_contract(contract):
    """True if contract is an option (e.g. SBK26C1600 or SWH26C500)."""
    return bool(_OPTION_RE.match(contract))


def is_futures_contract(contract):
    """True if contract is a futures code (e.g. SBK26 or SWH26)."""
    return bool(_FUTURE_RE.match(contract))


def parse_option_contract(contract):
    """
    Parse an option contract string.
    Returns (base, put_call, strike_int) or None if not an option.
      "SBK26C1600" -> ("SBK26", "C", 1600)
      "SWH26C500"  -> ("SWH26", "C", 500)
    """
    m = _OPTION_RE.match(contract)
    if not m:
        return None
    return m.group(1), m.group(2), int(m.group(3))


def get_underlying_contract(contract):
    """
    Return the underlying futures contract for an option.
      "SBK26C1600" -> "SBK26"
    Returns None if contract is not an option.
    """
    parsed = parse_option_contract(contract)
    if parsed is None:
        return None
    return parsed[0]


def to_tradestation_symbol(contract):
    """
    Convert an internal contract code to a TradeStation API symbol.
    Fully deterministic — uses _FUTURES_ROOT_MAP only, no probing.

      SBK26      -> SBK26         (SB root, pass-through)
      SWK26      -> CWK26         (SW root maps to CW)
      SBK26C1600 -> SBK26 C1600
      SWH26C500  -> CWH26 C500

    Returns None for unrecognised or malformed contracts.
    """
    if not contract or not contract.strip():
        return None

    if is_option_contract(contract):
        parsed = parse_option_contract(contract)
        if parsed is None:
            return None
        base, pc, strike = parsed
        prefix = base[:2]        # "SB" or "SW"
        month_year = base[2:]    # "K26"
        ts_root = _FUTURES_ROOT_MAP.get(prefix, prefix)
        return f"{ts_root}{month_year} {pc}{strike}"

    if is_futures_contract(contract):
        prefix = contract[:2]
        ts_root = _FUTURES_ROOT_MAP.get(prefix, prefix)
        return ts_root + contract[2:]

    return None   # unrecognised format


def build_fetch_symbol_map(contracts):
    """
    Build {ts_symbol: internal_contract} for all contracts to fetch.
    For options, automatically adds the underlying futures symbol too.
    Returns (symbol_map, malformed) where malformed is a list of bad contract strings.
    """
    symbol_map = {}
    malformed = []

    for contract in contracts:
        ts_sym = to_tradestation_symbol(contract)
        if ts_sym is None:
            malformed.append(contract)
            continue
        symbol_map[ts_sym] = contract

        # Options also need their underlying futures price for IV/delta computation
        if is_option_contract(contract):
            underlying = get_underlying_contract(contract)
            if underlying and underlying not in symbol_map.values():
                ul_ts_sym = to_tradestation_symbol(underlying)
                if ul_ts_sym and ul_ts_sym not in symbol_map:
                    symbol_map[ul_ts_sym] = underlying

    return symbol_map, malformed


# ── API helpers ───────────────────────────────────────────────────────────────

def get_access_token():
    """
    Obtain an OAuth2 access token using the refresh token flow.
    Raises RuntimeError on failure (caller should abort fetch).
    """
    try:
        resp = requests.post(
            "https://signin.tradestation.com/oauth/token",
            headers={"Content-Type": "application/x-www-form-urlencoded"},
            data=(
                f"grant_type=refresh_token"
                f"&client_id={os.environ['TRADESTATION_CLIENT_ID']}"
                f"&client_secret={os.environ['TRADESTATION_CLIENT_SECRET']}"
                f"&refresh_token={os.environ['TRADESTATION_REFRESH_TOKEN']}"
            ),
            timeout=15,
        )
        resp.raise_for_status()
        token = resp.json().get("access_token")
        if not token:
            raise RuntimeError("No access_token in TradeStation auth response")
        return token
    except requests.RequestException as e:
        raise RuntimeError(f"TradeStation auth failed: {e}") from e


def _batch_quotes(symbols, headers):
    """
    Fetch quotes for a list of TradeStation symbols in batches of 50.
    Returns {ts_symbol: quote_dict} for all symbols that returned data.
    Raises RuntimeError on HTTP failure.
    """
    results = {}
    symbols = list(symbols)
    for i in range(0, len(symbols), 50):
        batch = symbols[i:i + 50]
        try:
            resp = requests.get(
                f"{BASE_URL}/quotes/{','.join(batch)}",
                headers=headers,
                timeout=20,
            )
            resp.raise_for_status()
        except requests.RequestException as e:
            raise RuntimeError(f"TradeStation quotes request failed: {e}") from e

        data = resp.json()
        for q in data.get("Quotes", []):
            sym = q.get("Symbol")
            if sym:
                results[sym] = q

    return results


# ── Time-to-expiry ────────────────────────────────────────────────────────────

def _compute_T_from_last_trading_date(date_str, base=None):
    """
    Compute time-to-expiry T (in years) from a LastTradingDate ISO string.

    base: the date to measure from (defaults to today).

    Returns:
      float >= 0  : years to expiry
      None        : date_str is missing or malformed
    """
    if not date_str:
        return None
    try:
        expiry = datetime.datetime.fromisoformat(
            date_str.replace("Z", "+00:00")
        ).date()
        if base is None:
            base = datetime.date.today()
        days = (expiry - base).days
        return max(days, 0) / 365.0
    except Exception:
        return None


# ── Black-76 helpers (bisection IV solver) ────────────────────────────────────

def _norm_cdf(x):
    return 0.5 * math.erfc(-x / math.sqrt(2))


def _black76_price(F, K, T, r, sigma, is_call):
    sqrt_T = math.sqrt(T)
    d1 = (math.log(F / K) + 0.5 * sigma ** 2 * T) / (sigma * sqrt_T)
    d2 = d1 - sigma * sqrt_T
    disc = math.exp(-r * T)
    if is_call:
        return disc * (F * _norm_cdf(d1) - K * _norm_cdf(d2))
    return disc * (K * _norm_cdf(-d2) - F * _norm_cdf(-d1))


def _black76_delta(F, K, T, r, sigma, is_call):
    sqrt_T = math.sqrt(T)
    d1 = (math.log(F / K) + 0.5 * sigma ** 2 * T) / (sigma * sqrt_T)
    disc = math.exp(-r * T)
    return disc * _norm_cdf(d1) if is_call else -disc * _norm_cdf(-d1)


def _select_option_settlement(q, today_date=None):
    """Select the option settlement field and return diagnostics."""
    trade_time_str = q.get("TradeTime", "")
    try:
        trade_date = datetime.date.fromisoformat(trade_time_str[:10]) if trade_time_str else None
    except (ValueError, TypeError):
        trade_date = None

    if today_date is None:
        today_date = datetime.date.today()

    close = _to_float(q.get("Close"))
    previous_close = _to_float(q.get("PreviousClose"))
    if trade_date and trade_date == today_date:
        return previous_close, "PreviousClose", trade_date, close, previous_close
    return close, "Close", trade_date, close, previous_close


def _solve_iv_delta_variants(market_price, F, K, T, is_call, sofr_rate):
    """Return the main SOFR solve plus convention/rate comparison variants."""
    out = {
        "sofr_rate": sofr_rate,
        "iv": None,
        "delta_discounted": None,
        "delta_undiscounted": None,
        "zero_rate_iv": None,
        "zero_rate_delta_discounted": None,
        "zero_rate_delta_undiscounted": None,
    }
    if not _validate_option_inputs(F, market_price, T):
        return out

    iv = _implied_vol_bisect(market_price, F, K, T, sofr_rate, is_call)
    out["iv"] = iv
    if iv is not None:
        out["delta_discounted"] = _black76_delta(F, K, T, sofr_rate, iv, is_call)
        disc = math.exp(-sofr_rate * T)
        out["delta_undiscounted"] = out["delta_discounted"] / disc if disc else None

    zero_iv = _implied_vol_bisect(market_price, F, K, T, 0.0, is_call)
    out["zero_rate_iv"] = zero_iv
    if zero_iv is not None:
        out["zero_rate_delta_discounted"] = _black76_delta(F, K, T, 0.0, zero_iv, is_call)
        out["zero_rate_delta_undiscounted"] = out["zero_rate_delta_discounted"]
    return out



def _norm_pdf(x):
    return math.exp(-0.5 * x * x) / math.sqrt(2 * math.pi)


def _black76_gamma(F, K, T, r, sigma):
    sqrt_T = math.sqrt(T)
    d1 = (math.log(F / K) + 0.5 * sigma ** 2 * T) / (sigma * sqrt_T)
    return math.exp(-r * T) * _norm_pdf(d1) / (F * sigma * sqrt_T)


def _black76_vega(F, K, T, r, sigma):
    sqrt_T = math.sqrt(T)
    d1 = (math.log(F / K) + 0.5 * sigma ** 2 * T) / (sigma * sqrt_T)
    return F * math.exp(-r * T) * _norm_pdf(d1) * sqrt_T


def _black76_theta(F, K, T, r, sigma, is_call):
    sqrt_T = math.sqrt(T)
    d1 = (math.log(F / K) + 0.5 * sigma ** 2 * T) / (sigma * sqrt_T)
    d2 = d1 - sigma * sqrt_T
    disc = math.exp(-r * T)
    term1 = -F * disc * _norm_pdf(d1) * sigma / (2 * sqrt_T)
    if is_call:
        term2 = r * disc * (F * _norm_cdf(d1) - K * _norm_cdf(d2))
    else:
        term2 = r * disc * (K * _norm_cdf(-d2) - F * _norm_cdf(-d1))
    return (term1 + term2) / 365


def _implied_vol_bisect(market_price, F, K, T, r, is_call, tol=1e-7, max_iter=200):
    """
    Solve for implied volatility using the bisection method.
    Searches for sigma in [1e-4, 10.0] (i.e. 0.01% to 1000% annualised vol).
    Returns None if the root cannot be bracketed or the solver does not converge.
    """
    if T <= 0 or market_price <= 0 or F <= 0 or K <= 0:
        return None

    intrinsic = max(F - K, 0) if is_call else max(K - F, 0)
    if market_price < intrinsic * 0.999:
        return None   # below intrinsic — no real solution

    def f(sigma):
        try:
            return _black76_price(F, K, T, r, sigma, is_call) - market_price
        except (ValueError, ZeroDivisionError):
            return None

    lo, hi = 1e-4, 10.0
    f_lo = f(lo)
    f_hi = f(hi)
    if f_lo is None or f_hi is None:
        return None
    if f_lo * f_hi > 0:
        return None   # root not bracketed in [lo, hi]

    a, b = lo, hi
    fa = f_lo
    for _ in range(max_iter):
        mid = (a + b) / 2
        fm = f(mid)
        if fm is None:
            return None
        if abs(fm) < tol or (b - a) / 2 < tol:
            return mid
        if fa * fm < 0:
            b = mid
        else:
            a = mid
            fa = fm
    return (a + b) / 2


def _validate_option_inputs(F, settlement, T):
    """Return True if inputs are valid for IV/delta computation."""
    return (
        F is not None and F > 0
        and settlement is not None and settlement > 0
        and T is not None and T > 0
    )


# ── Barchart settlement helpers ───────────────────────────────────────────────

def _get_pricing_base_date():
    """Return today's date in SGT if after 6 AM SGT, otherwise yesterday's SGT date."""
    now_sgt = datetime.datetime.now(tz=_SGT)
    if now_sgt.hour >= 6:
        return now_sgt.date()
    return (now_sgt - datetime.timedelta(days=1)).date()


def _bar_date(bar):
    """Parse a barchart TimeStamp into a date in the exchange timezone."""
    ts = bar.get("TimeStamp", "")
    if not ts:
        return None
    try:
        dt = datetime.datetime.fromisoformat(ts.replace("Z", "+00:00"))
        return dt.astimezone(_EXCHANGE_TZ).date()
    except Exception:
        return None


def _fetch_bar_settlement(ts_symbol, target_date, headers):
    """
    Fetch the most recent daily Close for ts_symbol from the barcharts endpoint,
    using the most recent bar strictly before target_date (T-1 settlement).
    Returns (close_float_or_None, bar_date_or_None, reason_str).
    reason_str is None on success, or a short diagnostic string on failure.
    """
    try:
        encoded = urllib.parse.quote(ts_symbol, safe="")
        resp = requests.get(
            f"{BASE_URL}/barcharts/{encoded}",
            headers=headers,
            params={"interval": "1", "unit": "Daily", "barsback": "5"},
            timeout=15,
        )
        if resp.status_code != 200:
            return None, None, f"HTTP {resp.status_code}"
        bars = resp.json().get("Bars", [])
        if not bars:
            return None, None, "empty Bars[]"
        prior = [b for b in bars if (_bar_date(b) or datetime.date.min) < target_date]
        if not prior:
            bar_dates = [str(_bar_date(b)) for b in bars]
            return None, None, f"no bar before {target_date} (got {bar_dates})"
        bar = prior[-1]
        return _to_float(bar.get("Close")), _bar_date(bar), None
    except Exception as e:
        return None, None, str(e)


def _bulk_bar_settlements(ts_symbols, target_date, headers, max_workers=6):
    """
    Fetch T-1 bar settlement concurrently for multiple TradeStation futures symbols.
    Returns {ts_symbol: (close_or_None, bar_date_or_None)}.
    """
    result = {}
    with ThreadPoolExecutor(max_workers=max_workers) as pool:
        futures_map = {
            pool.submit(_fetch_bar_settlement, sym, target_date, headers): sym
            for sym in ts_symbols
        }
        for fut in as_completed(futures_map):
            sym = futures_map[fut]
            try:
                value, bar_dt, _ = fut.result()
                result[sym] = (value, bar_dt)
            except Exception:
                result[sym] = (None, None)
    return result


# ── Main entry point ──────────────────────────────────────────────────────────

def _to_float(val):
    try:
        return float(val)
    except (TypeError, ValueError):
        return None


def fetch_prices(contracts):
    """
    Fetch settlement prices and compute IV/delta for a list of contract codes.

    Args:
        contracts: list of internal watchlist contract strings (non-expired),
                   e.g. ["SBK26", "SWH26", "SBK26C1600", "SWH26C500"]

    Returns:
        (results, errors, sett_date)
          results:   list of dicts {contract, settlement, delta, iv, fetched_at}
          errors:    list of warning strings (missing quotes, IV failures, etc.)
          sett_date: the settlement bar date (datetime.date) used for pricing
    """
    results = []
    errors = []

    if not contracts:
        return results, errors, None

    # 1. Auth — abort on failure
    try:
        token = get_access_token()
    except RuntimeError as e:
        return results, [str(e)], None
    headers = {"Authorization": f"Bearer {token}"}

    # 2. Build symbol map (underlying futures auto-included for all options)
    symbol_map, malformed = build_fetch_symbol_map(contracts)
    for c in malformed:
        errors.append(f"{c}: malformed contract code, skipped")

    if not symbol_map:
        return results, errors, None

    # 3. Batch fetch all quotes
    try:
        quote_map = _batch_quotes(list(symbol_map.keys()), headers)
    except RuntimeError as e:
        return results, errors + [str(e)], None

    now = datetime.datetime.now(datetime.timezone.utc)

    # 4. Process futures contracts — settlement from barcharts (T-1 bar close)
    futures_contracts = [c for c in contracts if is_futures_contract(c)]
    option_contracts = [c for c in contracts if is_option_contract(c)]
    pricing_base_date = _get_pricing_base_date()
    r, _ = _fetch_sofr(pricing_base_date)

    # Include underlying futures for options so bar_closes covers F for IV/delta
    underlying_ts_syms = {
        to_tradestation_symbol(get_underlying_contract(c))
        for c in option_contracts
        if get_underlying_contract(c)
    } - {None}
    futures_ts_syms = list(
        {s for s in (to_tradestation_symbol(c) for c in futures_contracts) if s}
        | underlying_ts_syms
    )
    bar_closes = _bulk_bar_settlements(futures_ts_syms, pricing_base_date, headers)

    for contract in futures_contracts:
        ts_sym = to_tradestation_symbol(contract)
        if ts_sym is None:
            continue
        q = quote_map.get(ts_sym)
        if q is None:
            errors.append(f"{contract}: no quote returned by API, skipped")
            continue
        settlement, _ = bar_closes.get(ts_sym, (None, None))
        if settlement is None:
            errors.append(f"{contract}: barchart close missing, skipped")
            continue
        # Futures live price: prefer current bid/ask mid when available, else Last.
        # Do not gate on TradeTime's UTC date — ICE sessions cross midnight UTC,
        # so a valid overnight quote can look "yesterday" by date even while the
        # market is actively trading or quoting now.
        bid = _to_float(q.get("Bid"))
        ask = _to_float(q.get("Ask"))
        last = _to_float(q.get("Last"))
        if bid and ask and bid > 0 and ask > 0:
            live_price = (bid + ask) / 2
        elif (bid and bid > 0) or (ask and ask > 0):
            live_price = last if (last and last > 0) else None
        else:
            live_price = last if (last and last > 0) else None
        if not _is_live_session_open(contract, now):
            live_price = None
        results.append({
            "contract":   contract,
            "settlement": settlement,
            "delta":      1.0,
            "iv":         None,
            "live_price": live_price,
            "live_iv":    None,
            "live_delta": 1.0 if live_price is not None else None,
            "fetched_at": now,
        })

    # 5. Process options contracts
    for contract in option_contracts:
        parsed = parse_option_contract(contract)
        if parsed is None:
            continue
        base, pc, strike_int = parsed
        prefix = base[:2]
        ts_sym = to_tradestation_symbol(contract)

        q = quote_map.get(ts_sym)
        if q is None:
            errors.append(f"{contract}: no quote returned by API (ts_sym={ts_sym}), skipped")
            continue

        # Determine settlement price based on TradeTime vs today:
        #   TradeTime == today → market open today, Close is live → use PreviousClose
        #   TradeTime != today → market closed → Close is last settlement → use Close
        settlement, _, _, _, _ = _select_option_settlement(q)
        trade_time_str = q.get("TradeTime", "")

        if settlement is None or settlement <= 0:
            errors.append(f"{contract}: no settlement price (TradeTime={trade_time_str}), skipped")
            continue

        # Underlying futures price — bar close (same method as futures Sett-1)
        ul_ts_sym = to_tradestation_symbol(base)
        ul_q = quote_map.get(ul_ts_sym)
        F, sett_bar_date = bar_closes.get(ul_ts_sym, (None, None))
        if ul_q:
            _ul_bid = _to_float(ul_q.get("Bid"))
            _ul_ask = _to_float(ul_q.get("Ask"))
            if _ul_bid and _ul_ask and _ul_bid > 0 and _ul_ask > 0:
                F_live = (_ul_bid + _ul_ask) / 2
            else:
                F_live = _to_float(ul_q.get("Last"))
        else:
            F_live = None

        K = strike_int * _STRIKE_SCALE.get(prefix, 0.01)
        last_trading_date = q.get("LastTradingDate")
        T_sett = _compute_T_from_last_trading_date(last_trading_date, base=sett_bar_date)
        T_live = _compute_T_from_last_trading_date(last_trading_date, base=pricing_base_date)
        is_call = (pc == "C")

        iv = None
        delta = None
        if _validate_option_inputs(F, settlement, T_sett):
            iv = _implied_vol_bisect(settlement, F, K, T_sett, r, is_call)
            if iv is not None:
                try:
                    delta = _black76_delta(F, K, T_sett, r, iv, is_call)
                except (ValueError, ZeroDivisionError):
                    delta = None
            else:
                errors.append(
                    f"{contract}: IV solve did not converge "
                    f"(F={F}, K={K}, T={T_sett:.4f}, sett={settlement}), "
                    f"settlement stored but iv/delta=None"
                )
        else:
            missing = []
            if F is None:
                missing.append("underlying price")
            if settlement is None or settlement <= 0:
                missing.append("option price")
            if T_sett is None or T_sett <= 0:
                missing.append(f"valid T (got {T_sett})")
            errors.append(
                f"{contract}: skipping IV/delta — missing {', '.join(missing)}"
            )

        bid  = _to_float(q.get("Bid"))
        ask  = _to_float(q.get("Ask"))
        last = _to_float(q.get("Last"))
        if bid and ask and bid > 0 and ask > 0:
            live_price = (bid + ask) / 2
        elif (bid and bid > 0) or (ask and ask > 0):
            live_price = last if (last and last > 0) else None
        else:
            live_price = None

        # Live IV and delta — uses today as base for T, live futures mid as F
        live_iv = None
        live_delta = None
        F_live_used = F_live if (F_live and F_live > 0) else F
        if _validate_option_inputs(F_live_used, live_price, T_live):
            live_iv = _implied_vol_bisect(live_price, F_live_used, K, T_live, r, is_call)
            if live_iv is not None:
                try:
                    live_delta = _black76_delta(F_live_used, K, T_live, r, live_iv, is_call)
                except (ValueError, ZeroDivisionError):
                    live_delta = None

        if not _is_live_session_open(contract, now):
            live_price = None
            live_iv = None
            live_delta = None

        results.append({
            "contract":   contract,
            "settlement": settlement,
            "delta":      delta,
            "iv":         iv,
            "live_price": live_price,
            "live_iv":    live_iv,
            "live_delta": live_delta,
            "fetched_at": now,
        })

    # Determine the settlement date from bar_closes (most recent bar date)
    bar_dates = [bd for _, bd in bar_closes.values() if bd is not None]
    sett_date = max(bar_dates) if bar_dates else None

    # Attach sett_date to each result so it gets stored in the DB
    for r in results:
        r["sett_date"] = sett_date

    return results, errors, sett_date


def fetch_cotton_price_diagnostics(contracts, internal_expiry_map=None):
    """Fetch cotton option pricing inputs and comparison variants.

    This is intentionally read-only: it does not update CottonMarketPrice. It
    exists to reconcile app IV/delta against broker/ICE screen conventions.
    """
    diagnostics = []
    errors = []
    internal_expiry_map = internal_expiry_map or {}

    option_contracts = [
        c.strip().upper().replace(" ", "")
        for c in contracts
        if c and is_option_contract(c.strip().upper().replace(" ", ""))
        and c.strip().upper().replace(" ", "").startswith("CT")
    ]
    option_contracts = list(dict.fromkeys(option_contracts))
    if not option_contracts:
        return {"diagnostics": diagnostics, "errors": ["No cotton option contracts supplied."]}

    try:
        token = get_access_token()
    except RuntimeError as e:
        return {"diagnostics": diagnostics, "errors": [str(e)]}
    headers = {"Authorization": f"Bearer {token}"}

    symbol_map, malformed = build_fetch_symbol_map(option_contracts)
    for c in malformed:
        errors.append(f"{c}: malformed contract code, skipped")
    if not symbol_map:
        return {"diagnostics": diagnostics, "errors": errors}

    try:
        quote_map = _batch_quotes(list(symbol_map.keys()), headers)
    except RuntimeError as e:
        return {"diagnostics": diagnostics, "errors": errors + [str(e)]}

    pricing_base_date = _get_pricing_base_date()
    sofr_rate, sofr_date = _fetch_sofr(pricing_base_date)
    underlying_ts_syms = {
        to_tradestation_symbol(get_underlying_contract(c))
        for c in option_contracts
        if get_underlying_contract(c)
    } - {None}
    bar_closes = _bulk_bar_settlements(list(underlying_ts_syms), pricing_base_date, headers)

    for contract in option_contracts:
        parsed = parse_option_contract(contract)
        if parsed is None:
            continue
        base, pc, strike_int = parsed
        prefix = base[:2]
        ts_sym = to_tradestation_symbol(contract)
        ul_ts_sym = to_tradestation_symbol(base)
        q = quote_map.get(ts_sym)
        ul_q = quote_map.get(ul_ts_sym)
        if q is None:
            errors.append(f"{contract}: no option quote returned by API (ts_sym={ts_sym})")
            continue

        settlement, field_used, trade_date, close, previous_close = _select_option_settlement(q)
        F, sett_bar_date = bar_closes.get(ul_ts_sym, (None, None))
        K = strike_int * _STRIKE_SCALE.get(prefix, 0.01)
        last_trading_date = q.get("LastTradingDate")
        expiry_date = None
        if last_trading_date:
            try:
                expiry_date = datetime.datetime.fromisoformat(
                    last_trading_date.replace("Z", "+00:00")
                ).date()
            except (ValueError, TypeError):
                expiry_date = None
        days_to_expiry = (expiry_date - sett_bar_date).days if expiry_date and sett_bar_date else None
        T_sett = _compute_T_from_last_trading_date(last_trading_date, base=sett_bar_date)
        is_call = pc == "C"
        variants = _solve_iv_delta_variants(settlement, F, K, T_sett, is_call, sofr_rate)
        displayed_delta = variants["delta_discounted"]

        if ul_q:
            ul_bid = _to_float(ul_q.get("Bid"))
            ul_ask = _to_float(ul_q.get("Ask"))
            ul_last = _to_float(ul_q.get("Last"))
        else:
            ul_bid = ul_ask = ul_last = None

        internal_expiry = internal_expiry_map.get(base)
        diagnostics.append({
            "contract": contract,
            "tradestation_symbol": ts_sym,
            "underlying_contract": base,
            "underlying_tradestation_symbol": ul_ts_sym,
            "option_type": pc,
            "strike_int": strike_int,
            "strike_scale": _STRIKE_SCALE.get(prefix, 0.01),
            "strike": K,
            "quote": {
                "field_used": field_used,
                "settlement": settlement,
                "close": close,
                "previous_close": previous_close,
                "trade_time": q.get("TradeTime"),
                "trade_date": trade_date.isoformat() if trade_date else None,
                "bid": _to_float(q.get("Bid")),
                "ask": _to_float(q.get("Ask")),
                "last": _to_float(q.get("Last")),
            },
            "underlying": {
                "settlement_used": F,
                "settlement_bar_date": sett_bar_date.isoformat() if sett_bar_date else None,
                "bid": ul_bid,
                "ask": ul_ask,
                "last": ul_last,
                "trade_time": ul_q.get("TradeTime") if ul_q else None,
            },
            "expiry": {
                "tradestation_last_trading_date": last_trading_date,
                "tradestation_expiry_date": expiry_date.isoformat() if expiry_date else None,
                "internal_cotton_expiry_date": internal_expiry.isoformat() if internal_expiry else None,
                "days_to_expiry": days_to_expiry,
                "T": T_sett,
            },
            "rates": {
                "sofr_rate": sofr_rate,
                "sofr_date": sofr_date,
                "zero_rate": 0.0,
            },
            "results": {
                "iv": variants["iv"],
                "iv_pct": variants["iv"] * 100 if variants["iv"] is not None else None,
                "delta_convention": "discounted",
                "displayed_delta": displayed_delta,
                "delta_discounted": variants["delta_discounted"],
                "delta_undiscounted": variants["delta_undiscounted"],
                "zero_rate_iv": variants["zero_rate_iv"],
                "zero_rate_iv_pct": (
                    variants["zero_rate_iv"] * 100
                    if variants["zero_rate_iv"] is not None else None
                ),
                "zero_rate_delta_discounted": variants["zero_rate_delta_discounted"],
                "zero_rate_delta_undiscounted": variants["zero_rate_delta_undiscounted"],
            },
        })

    return {
        "pricing_base_date": pricing_base_date.isoformat(),
        "delta_convention": "discounted",
        "diagnostics": diagnostics,
        "errors": errors,
    }


import logging
_log = logging.getLogger(__name__)


def fetch_futures_expiries(series_codes):
    """
    Fetch LastTradingDate from TradeStation for a list of futures series.

    Args:
        series_codes: e.g. ["SB H26", "SW K26", ...]
                      Format: "<root> <month_code><YY>"

    Returns:
        {series_code: datetime.date | None}
        Empty dict on auth failure.
    """
    if not series_codes:
        return {}

    # Deduplicate
    seen = set()
    unique = []
    for s in series_codes:
        if s not in seen:
            seen.add(s)
            unique.append(s)

    # 1. Auth
    try:
        token = get_access_token()
    except RuntimeError as e:
        _log.warning("fetch_futures_expiries: auth failed — %s", e)
        return {}
    headers = {"Authorization": f"Bearer {token}"}

    # 2. Build symbol map: "SB H26" → "SBH26" → to_tradestation_symbol → "SBH26"
    #                       "SW K26" → "SWK26" → to_tradestation_symbol → "CWK26"
    ts_to_series = {}
    for series in unique:
        parts = series.split()
        if len(parts) != 2:
            _log.warning("fetch_futures_expiries: bad series code %r, skipped", series)
            continue
        internal = parts[0] + parts[1]  # "SBH26" or "SWK26"
        ts_sym = to_tradestation_symbol(internal)
        if ts_sym:
            ts_to_series[ts_sym] = series

    if not ts_to_series:
        return {}

    # 3. Batch-fetch quotes
    try:
        quote_map = _batch_quotes(list(ts_to_series.keys()), headers)
    except RuntimeError as e:
        _log.warning("fetch_futures_expiries: quote fetch failed — %s", e)
        return {}

    # 4. Extract LastTradingDate
    result = {}
    for ts_sym, series in ts_to_series.items():
        q = quote_map.get(ts_sym)
        if q is None:
            result[series] = None
            continue
        ltd = q.get("LastTradingDate")
        if not ltd:
            result[series] = None
            continue
        try:
            dt = datetime.datetime.fromisoformat(ltd.replace("Z", "+00:00"))
            result[series] = dt.date()
        except (ValueError, TypeError) as e:
            _log.warning(
                "fetch_futures_expiries: malformed LastTradingDate %r for %s — %s",
                ltd, ts_sym, e,
            )
            result[series] = None

    # Fill in any series not in result
    for series in unique:
        if series not in result:
            result[series] = None

    # 5. Log summary
    total = len(unique)
    resolved_count = sum(1 for v in result.values() if v is not None)
    fell_back = [s for s, v in result.items() if v is None]
    _log.info(
        "fetch_futures_expiries: %d/%d series resolved. Fallback: %s",
        resolved_count, total, fell_back or "none",
    )

    return result

"""Neon Markets API client — sugar-only port of GTB's neon_service/neon.py.

Fetches intraday trades (today + yesterday SGT) and reshapes them into
Salesforce-style dicts so the rest of the app (positions page, dashboards,
reports) keeps working unchanged.
"""
import hashlib
import logging
import re
import time
from datetime import datetime
from zoneinfo import ZoneInfo

import requests
from flask import current_app

log = logging.getLogger(__name__)

_WS_RE = re.compile(r"\s+")


def _norm(s: str | None) -> str:
    if not s:
        return ""
    return _WS_RE.sub(" ", s.strip()).upper()


def build_dedup_key(trade_date: str, contract: str, account: str,
                    price: float, long_qty: float, short_qty: float,
                    put_call: str | None = None,
                    strike: float | None = None) -> str:
    """Canonical key for cross-source trade dedup.

    Both SF sync and Neon sync compute this and store in
    TradePosition.dedup_key, so the cross-source lookup is a single indexed
    equality instead of comparing six JSON-text values. Rounding noise
    (18.5 vs 18.50) and type noise (1 vs 1.0) is normalised away.

    Long/short are abs()'d because SF stores sells as negative Short__c
    while Neon emits them as positive — direction is encoded by which
    field is non-zero, not by the sign of the value itself.
    """
    norm = (
        (trade_date or "").strip(),
        _norm(contract),
        _norm(account),
        f"{float(price or 0):.4f}",
        f"{abs(float(long_qty or 0)):.2f}",
        f"{abs(float(short_qty or 0)):.2f}",
        _norm(put_call),
        "" if strike is None else f"{float(strike):.4f}",
    )
    return hashlib.sha1("|".join(norm).encode()).hexdigest()[:32]


class NeonClient:
    TOKEN_URL    = "https://login.neon.markets/oauth/token"
    ACCOUNTS_URL = "https://neonapi.neon.markets/rest/portfolio/v1/accounts"
    TRADES_URL   = "https://neonapi.neon.markets/rest/portfolio/v1/trades"

    # Sugar-only. Normalised keys (uppercased, whitespace collapsed).
    NEON_SUGAR_MAP = {
        "SUGAR NO.11":      ("SB", "ICE Raw Sugar"),   # multiplier 1120
        "NO.5 WHITE SUGAR": ("SW", "LDN Sugar #5"),    # multiplier 50
    }

    # Sugar accounts we pull for. Anything else → "unmapped_acct" skip.
    ALLOWED_SUGAR_ACCOUNTS = {"08290CA", "LSU15001"}

    # Neon-side account IDs that resolve to the same SF account. Neon sometimes
    # emits more than one numeric id for a single Agrocorp trading account.
    NEON_ACCOUNT_ALIASES = {
        "11108290": "08290CA",
        "11118290": "08290CA",
    }

    FUTURES_MONTH_CODES = {
        "Jan": "F", "Feb": "G", "Mar": "H", "Apr": "J",
        "May": "K", "Jun": "M", "Jul": "N", "Aug": "Q",
        "Sep": "U", "Oct": "V", "Nov": "X", "Dec": "Z",
    }
    FUTURES_MONTH_CODES_NUMERIC = {
        "01": "F", "02": "G", "03": "H", "04": "J",
        "05": "K", "06": "M", "07": "N", "08": "Q",
        "09": "U", "10": "V", "11": "X", "12": "Z",
    }

    def __init__(self):
        self.client_id     = current_app.config.get("NEON_CLIENT_ID")
        self.client_secret = current_app.config.get("NEON_CLIENT_SECRET")
        if not self.client_id or not self.client_secret:
            raise RuntimeError(
                "NEON_CLIENT_ID / NEON_CLIENT_SECRET not set in environment"
            )
        self.token = None
        self.token_expires_at = 0.0
        self.account_lst: list[str] = []
        # One-shot diagnostic logging: surface the first rejected raw value
        # per bucket so we can fix the transform / commodity map without
        # flooding logs when there are many non-sugar trades.
        self._logged_non_sugar = False
        self._logged_unmapped_acct = False

    # ── auth ──────────────────────────────────────────────────────────────

    def _token_is_valid(self) -> bool:
        return bool(self.token) and time.time() < self.token_expires_at

    def get_token(self) -> None:
        r = requests.post(
            self.TOKEN_URL,
            headers={"Content-Type": "application/json"},
            json={
                "client_id":     self.client_id,
                "client_secret": self.client_secret,
                "audience":      "https://app.neon.markets/api",
                "grant_type":    "client_credentials",
            },
            timeout=30,
        )
        r.raise_for_status()
        data = r.json()
        self.token = data["access_token"]
        self.token_expires_at = time.time() + data["expires_in"]

    def get_accounts(self) -> list[str]:
        if not self._token_is_valid():
            self.get_token()
        r = requests.get(
            self.ACCOUNTS_URL,
            headers={
                "Content-Type":  "application/json",
                "Authorization": f"Bearer {self.token}",
            },
            timeout=30,
        )
        r.raise_for_status()
        data = r.json()
        self.account_lst = [a["accountNumber"]["value"] for a in data.get("accounts", [])]
        return self.account_lst

    def connect(self) -> None:
        self.get_token()
        self.get_accounts()

    # ── fetch ─────────────────────────────────────────────────────────────

    def get_trades(self, trading_day: str) -> dict:
        if not self._token_is_valid():
            self.get_token()
        if not self.account_lst:
            self.get_accounts()
        r = requests.get(
            self.TRADES_URL,
            headers={
                "Content-Type":  "application/json",
                "Authorization": f"Bearer {self.token}",
            },
            params={"accounts": ",".join(self.account_lst), "date": trading_day},
            timeout=60,
        )
        r.raise_for_status()
        return r.json()

    # ── transform ─────────────────────────────────────────────────────────

    @classmethod
    def _transform_account(cls, acct: str | None) -> str | None:
        """Map Neon account id to SF format. Returns None if we don't handle it.

        Rules:
          - Explicit alias via NEON_ACCOUNT_ALIASES (covers observed variants
            like 11108290 / 11118290 both resolving to 08290CA)
          - "LSU*" → passthrough (sugar)
          - anything else → None (caller buckets as unmapped_acct)
        """
        if not acct:
            return None
        a = acct.strip()
        if a in cls.NEON_ACCOUNT_ALIASES:
            return cls.NEON_ACCOUNT_ALIASES[a]
        if a.upper().startswith("LSU"):
            return a
        return None

    def preprocess_trade(self, trade: dict, trading_day: str) -> tuple[dict | None, str | None]:
        """Reshape one Neon trade into an SF-style dict.

        Returns (mapped_dict, None) on success, or (None, reason) where
        reason is one of {"non_sugar", "unmapped_acct", "other"}.
        """
        try:
            taxonomy = trade["product"]["taxonomy"][0]["productQualifier"]
            is_option = (taxonomy == "ExchangeTradedOption")

            if is_option:
                payout = trade["product"]["economicTerms"]["payout"][0]["OptionPayout"]
                contract_identifier = payout["underlier"]["Product"]["TransferableProduct"]\
                    ["Instrument"]["ListedDerivative"]["identifier"][2]["identifier"]["value"]
            else:
                contract_identifier = trade["product"]["economicTerms"]["payout"][0]\
                    ["SettlementPayout"]["underlier"]["Product"]["TransferableProduct"]\
                    ["Instrument"]["ListedDerivative"]["identifier"][2]["identifier"]["value"]

            sugar_entry = self.NEON_SUGAR_MAP.get(_norm(contract_identifier))
            if sugar_entry is None:
                if not self._logged_non_sugar:
                    log.warning("Neon non_sugar: commodity=%r (normalised=%r)",
                                contract_identifier, _norm(contract_identifier))
                    self._logged_non_sugar = True
                return None, "non_sugar"
            sf_code, sf_commodity_name = sugar_entry

            raw_acct = trade["account"][0]["accountNumber"]["value"]
            mapped_acct = self._transform_account(raw_acct)
            if mapped_acct is None or mapped_acct not in self.ALLOWED_SUGAR_ACCOUNTS:
                if not self._logged_unmapped_acct:
                    log.warning("Neon unmapped_acct: raw=%r → transform=%r (allowed=%s)",
                                raw_acct, mapped_acct, sorted(self.ALLOWED_SUGAR_ACCOUNTS))
                    self._logged_unmapped_acct = True
                return None, "unmapped_acct"

            uid = trade["tradeIdentifier"][0]["assignedIdentifier"][0]["identifier"]["value"]
            lot = trade.get("tradeLot", [{}])[0]
            quantity = lot["priceQuantity"][0]["quantity"][0]["value"]["value"]
            price    = lot["priceQuantity"][0]["price"][0]["value"]["value"]

            # Neon sends negative qty for sells; buySell/direction can override
            # but are often empty — trust the sign when they're absent.
            buy_sell  = (lot.get("buySell")  or "").upper()
            direction = (lot.get("direction") or "").upper()
            if buy_sell == "SELL" or direction == "SELL":
                quantity = -abs(quantity)
            elif buy_sell == "BUY" or direction == "BUY":
                quantity = abs(quantity)

            qty = int(quantity)
            long_qty  = qty if qty > 0 else 0
            short_qty = -qty if qty < 0 else 0

            if is_option:
                option_type = payout["optionType"]
                put_call = "Call" if option_type.upper() == "CALL" else "Put"
                strike = float(payout["strike"]["strikePrice"]["value"])
                option_expiry = payout["exerciseTerms"]["expirationDate"][0]\
                    ["adjustableDate"]["unadjustedDate"]
                mm = option_expiry.split("-")[1]
                yy = option_expiry.split("-")[0][-2:]
                contract = f"{sf_code}{self.FUTURES_MONTH_CODES_NUMERIC[mm]}{yy}"
                contract_type = "Option"
            else:
                put_call = None
                strike = None
                delivery = trade["product"]["economicTerms"]["payout"][0]\
                    ["SettlementPayout"]["deliveryTerm"]
                # "Jan 2027" / "Jan-27" style — take first 3 chars + last 2
                contract = f"{sf_code}{self.FUTURES_MONTH_CODES[delivery[:3]]}{delivery[-2:]}"
                contract_type = "Futures"

            return {
                "Id":                   f"NEON_{uid}",
                "Name":                 f"NEON-{uid[:12]}",
                "Trade_Date__c":        trading_day,
                "Contract__c":          contract,
                "Long__c":              long_qty,
                "Short__c":             short_qty,
                "Price__c":             float(price),
                "Put_Call_2__c":        put_call,
                "Strike__c":            strike,
                "Commodity_Name__c":    sf_commodity_name,
                "Account_No__c":        mapped_acct,
                "Contract_type__c":     contract_type,
                "Broker_Name__c":       "Neon",
                "Strategy__c":          None,
                "Trader__c":            None,
                "Book__c":              None,
                "Realised__c":          None,
                "Broker_Commission__c": None,
            }, None
        except (KeyError, IndexError, TypeError, ValueError) as e:
            log.warning("Neon preprocess_trade failed: %s", e)
            return None, "other"

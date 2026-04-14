from flask_sqlalchemy import SQLAlchemy
from datetime import datetime

db = SQLAlchemy()


class TradePosition(db.Model):
    __tablename__ = "sugar_trade_positions"

    sf_id = db.Column(db.String(18), primary_key=True)
    name = db.Column(db.String(255))
    data = db.Column(db.JSON)  # All Salesforce fields stored as JSON
    last_synced_at = db.Column(db.DateTime, default=datetime.utcnow)
    contract_xl = db.Column(db.String(100), nullable=True)
    instrument = db.Column(db.String(100), nullable=True)   # Strategy__c part [0]
    spread = db.Column(db.String(100), nullable=True)        # Strategy__c part [1]
    book_parsed = db.Column(db.String(100), nullable=True)   # Strategy__c part [3]

    def __repr__(self):
        return f"<TradePosition {self.sf_id} {self.name}>"


class SyncLog(db.Model):
    __tablename__ = "sugar_sync_logs"

    id = db.Column(db.Integer, primary_key=True, autoincrement=True)
    synced_at = db.Column(db.DateTime, default=datetime.utcnow)
    record_count = db.Column(db.Integer, default=0)
    status = db.Column(db.String(20), default="success")  # success / error
    message = db.Column(db.Text)

    def __repr__(self):
        return f"<SyncLog {self.synced_at} {self.record_count} records>"


class AutoTagRun(db.Model):
    __tablename__ = "sugar_auto_tag_runs"

    id = db.Column(db.Integer, primary_key=True, autoincrement=True)
    ran_at = db.Column(db.DateTime, default=datetime.utcnow, nullable=False)
    filename = db.Column(db.String(255))
    start_date = db.Column(db.Date)
    end_date = db.Column(db.Date)
    excel_row_count = db.Column(db.Integer, default=0)
    matched_count = db.Column(db.Integer, default=0)
    unmatched_excel_count = db.Column(db.Integer, default=0)
    unmatched_sf_count = db.Column(db.Integer, default=0)
    sf_created = db.Column(db.Integer, default=0)
    sf_updated = db.Column(db.Integer, default=0)
    sf_errors = db.Column(db.Integer, default=0)
    status = db.Column(db.String(20))   # "success" | "partial" | "error"
    error_sample = db.Column(db.JSON)   # first ~30 errors for triage

    def __repr__(self):
        return f"<AutoTagRun {self.ran_at} {self.status}>"


class MarketPrice(db.Model):
    __tablename__ = "sugar_market_prices"

    id = db.Column(db.Integer, primary_key=True, autoincrement=True)
    contract = db.Column(db.String(50), unique=True)  # e.g. 'SBK25' or 'SBK25 C 18.00'
    settlement = db.Column(db.Float)    # Settlement-1 price
    delta = db.Column(db.Float)         # Delta-1
    iv = db.Column(db.Float)            # IV-1 (implied volatility, stored as decimal e.g. 0.20 = 20%)
    settlement2 = db.Column(db.Float)   # Settlement-2 (previous day snapshot)
    delta2 = db.Column(db.Float)        # Delta-2 (previous day snapshot)
    live_price = db.Column(db.Float)    # Live mid price: (Bid+Ask)/2 or Last
    live_iv    = db.Column(db.Float)    # IV computed from live mid price
    live_delta = db.Column(db.Float)    # Delta computed from live mid price
    sett_date  = db.Column(db.Date)     # Actual settlement bar date from TradeStation
    fetched_at = db.Column(db.DateTime, default=datetime.utcnow)

    def __repr__(self):
        return f"<MarketPrice {self.contract} {self.settlement}>"


class WatchedContract(db.Model):
    __tablename__ = "sugar_watched_contracts"

    id = db.Column(db.Integer, primary_key=True, autoincrement=True)
    contract = db.Column(db.String(50), unique=True, nullable=False)
    expired = db.Column(db.Boolean, default=False, nullable=False, server_default='false')
    sort_order = db.Column(db.Integer, default=0, nullable=False, server_default='0')
    created_at = db.Column(db.DateTime, default=datetime.utcnow)

    def __repr__(self):
        return f"<WatchedContract {self.contract}>"


class PhysicalTrade(db.Model):
    __tablename__ = "sugar_physical_trades"

    id = db.Column(db.Integer, primary_key=True, autoincrement=True)
    row_index = db.Column(db.Integer, nullable=False)
    data = db.Column(db.JSON, nullable=False)  # {col_label: cell_value}
    synced_at = db.Column(db.DateTime, nullable=False)

    def __repr__(self):
        return f"<PhysicalTrade row={self.row_index}>"


class FFATrade(db.Model):
    __tablename__ = "sugar_ffa_trades"

    id = db.Column(db.Integer, primary_key=True, autoincrement=True)
    row_index = db.Column(db.Integer, nullable=False)
    trade_date = db.Column(db.String(20), nullable=False)
    shipment = db.Column(db.String(20), nullable=False)
    size = db.Column(db.String(20), nullable=False)
    long_ = db.Column("long", db.Float)
    short_ = db.Column("short", db.Float)
    trade_price = db.Column(db.Float)
    synced_at = db.Column(db.DateTime, nullable=False)

    def __repr__(self):
        return f"<FFATrade {self.trade_date} {self.shipment}>"


class FFASettlement(db.Model):
    __tablename__ = "sugar_ffa_settlement"

    id = db.Column(db.Integer, primary_key=True, autoincrement=True)
    row_index = db.Column(db.Integer, nullable=False)
    shipment = db.Column(db.String(20), nullable=False)
    smx = db.Column(db.Float)
    pmx = db.Column(db.Float)
    synced_at = db.Column(db.DateTime, nullable=False)

    def __repr__(self):
        return f"<FFASettlement {self.shipment}>"


class OptionExpiry(db.Model):
    __tablename__ = "sugar_option_expiries"

    series     = db.Column(db.String(20), primary_key=True)   # e.g. "SB K26"
    expiry     = db.Column(db.Date, nullable=False)
    fetched_at = db.Column(db.DateTime, nullable=False, default=datetime.utcnow)

    def __repr__(self):
        return f"<OptionExpiry {self.series} {self.expiry}>"


class PnlSnapshot(db.Model):
    __tablename__ = "sugar_pnl_snapshots"

    slot           = db.Column(db.String(10), primary_key=True)  # 'daily', 'weekly', 'monthly'
    snapshotted_at = db.Column(db.DateTime, nullable=False)
    data           = db.Column(db.JSON, nullable=False)

    def __repr__(self):
        return f"<PnlSnapshot {self.slot} {self.snapshotted_at}>"


# ── Simulator models ────────────────────────────────────────────────────────

class SimLoadedFuture(db.Model):
    __tablename__ = "sugar_sim_futures"

    id          = db.Column(db.Integer, primary_key=True, autoincrement=True)
    contract    = db.Column(db.String(50), nullable=False)
    commodity   = db.Column(db.String(50))
    net_lots    = db.Column(db.Float, default=0)
    avg_price   = db.Column(db.Float, default=0)
    settlement  = db.Column(db.Float, default=0)
    lower_limit = db.Column(db.Float, default=0)
    upper_limit = db.Column(db.Float, default=0)
    expiry_date = db.Column(db.Date)
    point_value = db.Column(db.Float, default=1)
    created_at  = db.Column(db.DateTime, default=datetime.utcnow)

    def __repr__(self):
        return f"<SimLoadedFuture {self.contract} {self.net_lots}>"


class SimLoadedOption(db.Model):
    __tablename__ = "sugar_sim_options"

    id               = db.Column(db.Integer, primary_key=True, autoincrement=True)
    contract         = db.Column(db.String(50), nullable=False)
    underlying       = db.Column(db.String(50))
    commodity        = db.Column(db.String(50))
    put_call         = db.Column(db.String(10))
    strike           = db.Column(db.Float, default=0)
    net_lots         = db.Column(db.Float, default=0)
    avg_price        = db.Column(db.Float, default=0)
    settlement       = db.Column(db.Float, default=0)
    underlying_price = db.Column(db.Float, default=0)
    iv               = db.Column(db.Float, default=0)
    iv_lower         = db.Column(db.Float, default=0)
    iv_upper         = db.Column(db.Float, default=0)
    expiry_date      = db.Column(db.Date)
    point_value      = db.Column(db.Float, default=1)
    r                = db.Column(db.Float, default=0)
    delta            = db.Column(db.Float, default=0)
    gamma            = db.Column(db.Float, default=0)
    vega             = db.Column(db.Float, default=0)
    theta            = db.Column(db.Float, default=0)
    created_at       = db.Column(db.DateTime, default=datetime.utcnow)

    def __repr__(self):
        return f"<SimLoadedOption {self.contract} {self.net_lots}>"


class PhysicalDeal(db.Model):
    __tablename__ = "sugar_physical_deals"

    id         = db.Column(db.Integer, primary_key=True, autoincrement=True)
    book       = db.Column(db.String(20), nullable=False)   # 'Raws' or 'Whites'
    row_index  = db.Column(db.Integer, nullable=False)       # Display order
    data       = db.Column(db.JSON, nullable=False)          # All input fields as JSON
    updated_at = db.Column(db.DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
    created_at = db.Column(db.DateTime, default=datetime.utcnow)

    def __repr__(self):
        return f"<PhysicalDeal {self.book} row={self.row_index}>"


class SimStack(db.Model):
    __tablename__ = "sugar_sim_stacks"

    id             = db.Column(db.Integer, primary_key=True, autoincrement=True)
    label          = db.Column(db.String(100), nullable=False)
    x_axis         = db.Column(db.String(20))
    commodity_code = db.Column(db.String(50))
    data           = db.Column(db.JSON)
    created_at     = db.Column(db.DateTime, default=datetime.utcnow)

    def __repr__(self):
        return f"<SimStack {self.label}>"

from datetime import datetime
from models.db import db


class CottonTradePosition(db.Model):
    __tablename__ = "cotton_trade_positions"

    sf_id = db.Column(db.String(18), primary_key=True)
    name = db.Column(db.String(255))
    data = db.Column(db.JSON)  # Raw Salesforce record
    last_synced_at = db.Column(db.DateTime, default=datetime.utcnow)
    # 6-part Strategy__c parse: Instrument-Spread-ContractXL-Book-Region-BF=fee
    # Legacy 5-part rows may have bf_parsed=None until edited or re-synced.
    instrument  = db.Column(db.String(100), nullable=True)
    spread      = db.Column(db.String(100), nullable=True)
    contract_xl = db.Column(db.String(100), nullable=True)
    book_parsed = db.Column(db.String(100), nullable=True)
    region      = db.Column(db.String(100), nullable=True)  # cotton-only 5th part
    bf_parsed   = db.Column(db.Float, nullable=True)         # Strategy__c part [5] BF=xxx value

    @property
    def commission(self):
        if self.bf_parsed is not None:
            return -self.bf_parsed
        return float((self.data or {}).get('Broker_Commission__c') or 0)

    def __repr__(self):
        return f"<CottonTradePosition {self.sf_id} {self.name}>"


class CottonSyncLog(db.Model):
    __tablename__ = "cotton_sync_logs"

    id = db.Column(db.Integer, primary_key=True, autoincrement=True)
    synced_at = db.Column(db.DateTime, default=datetime.utcnow)
    record_count = db.Column(db.Integer, default=0)
    status = db.Column(db.String(20), default="success")
    message = db.Column(db.Text)

    def __repr__(self):
        return f"<CottonSyncLog {self.synced_at} {self.record_count} records>"


class CottonMarketPrice(db.Model):
    __tablename__ = "cotton_market_prices"

    id = db.Column(db.Integer, primary_key=True, autoincrement=True)
    contract = db.Column(db.String(50), unique=True)
    settlement = db.Column(db.Float)
    delta = db.Column(db.Float)
    iv = db.Column(db.Float)
    settlement2 = db.Column(db.Float)
    delta2 = db.Column(db.Float)
    live_price = db.Column(db.Float)
    live_iv    = db.Column(db.Float)
    live_delta = db.Column(db.Float)
    sett_date  = db.Column(db.Date)
    fetched_at = db.Column(db.DateTime, default=datetime.utcnow)
    sett_fetched_at = db.Column(db.DateTime)
    live_fetched_at = db.Column(db.DateTime)

    def __repr__(self):
        return f"<CottonMarketPrice {self.contract} {self.settlement}>"


class CottonWatchedContract(db.Model):
    __tablename__ = "cotton_watched_contracts"

    id = db.Column(db.Integer, primary_key=True, autoincrement=True)
    contract = db.Column(db.String(50), unique=True, nullable=False)
    expired = db.Column(db.Boolean, default=False, nullable=False, server_default='false')
    sort_order = db.Column(db.Integer, default=0, nullable=False, server_default='0')
    created_at = db.Column(db.DateTime, default=datetime.utcnow)

    def __repr__(self):
        return f"<CottonWatchedContract {self.contract}>"


class CottonSimLoadedFuture(db.Model):
    __tablename__ = "cotton_sim_futures"

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
        return f"<CottonSimLoadedFuture {self.contract} {self.net_lots}>"


class CottonSimLoadedOption(db.Model):
    __tablename__ = "cotton_sim_options"

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
        return f"<CottonSimLoadedOption {self.contract} {self.net_lots}>"


class CottonSimStack(db.Model):
    __tablename__ = "cotton_sim_stacks"

    id             = db.Column(db.Integer, primary_key=True, autoincrement=True)
    label          = db.Column(db.String(100), nullable=False)
    x_axis         = db.Column(db.String(20))
    commodity_code = db.Column(db.String(50))
    data           = db.Column(db.JSON)
    created_at     = db.Column(db.DateTime, default=datetime.utcnow)

    def __repr__(self):
        return f"<CottonSimStack {self.label}>"


class CottonIndexRow(db.Model):
    __tablename__ = "cotton_index_rows"

    id         = db.Column(db.Integer, primary_key=True, autoincrement=True)
    table_name = db.Column(db.String(50), nullable=False, index=True)
    row_index  = db.Column(db.Integer, nullable=False)
    data       = db.Column(db.JSON, nullable=False)
    source     = db.Column(db.String(20), nullable=False, default="seed")
    created_at = db.Column(db.DateTime, default=datetime.utcnow)
    updated_at = db.Column(db.DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

    def __repr__(self):
        return f"<CottonIndexRow {self.table_name} #{self.row_index}>"


class CottonPhysicalDeal(db.Model):
    __tablename__ = "cotton_physical_deals"

    id         = db.Column(db.Integer, primary_key=True, autoincrement=True)
    book       = db.Column(db.String(20), nullable=False, index=True)  # 'Purchases' or 'Sales'
    row_index  = db.Column(db.Integer, nullable=False)
    data       = db.Column(db.JSON, nullable=False)
    source     = db.Column(db.String(20), nullable=False, default="excel-default")
    created_at = db.Column(db.DateTime, default=datetime.utcnow)
    updated_at = db.Column(db.DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

    def __repr__(self):
        return f"<CottonPhysicalDeal {self.book} #{self.row_index}>"

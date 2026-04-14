from datetime import datetime
from models.db import db


class CottonTradePosition(db.Model):
    __tablename__ = "cotton_trade_positions"

    sf_id = db.Column(db.String(18), primary_key=True)
    name = db.Column(db.String(255))
    data = db.Column(db.JSON)  # Raw Salesforce record
    last_synced_at = db.Column(db.DateTime, default=datetime.utcnow)
    # 5-part Strategy__c parse: Instrument-Spread-ContractXL-Book-Region
    instrument  = db.Column(db.String(100), nullable=True)
    spread      = db.Column(db.String(100), nullable=True)
    contract_xl = db.Column(db.String(100), nullable=True)
    book_parsed = db.Column(db.String(100), nullable=True)
    region      = db.Column(db.String(100), nullable=True)  # cotton-only 5th part

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

from datetime import datetime
from models.db import db


class CoffeeMarketPrice(db.Model):
    __tablename__ = "coffee_market_prices"

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
        return f"<CoffeeMarketPrice {self.contract} {self.settlement}>"


class CoffeeWatchedContract(db.Model):
    __tablename__ = "coffee_watched_contracts"

    id = db.Column(db.Integer, primary_key=True, autoincrement=True)
    contract = db.Column(db.String(50), unique=True, nullable=False)
    expired = db.Column(db.Boolean, default=False, nullable=False, server_default='false')
    sort_order = db.Column(db.Integer, default=0, nullable=False, server_default='0')
    created_at = db.Column(db.DateTime, default=datetime.utcnow)

    def __repr__(self):
        return f"<CoffeeWatchedContract {self.contract}>"

from flask import Blueprint, render_template
from collections import defaultdict
from sqlalchemy import cast, Date
from models.db import db, TradePosition

strategy_warnings_bp = Blueprint("strategy_warnings", __name__)


def get_warning_groups():
    """Return list of (trade_date, contract) groups with missing Strategy__c and non-zero net position."""
    records = (
        TradePosition.query
        .filter(TradePosition.instrument == None)
        .order_by(cast(TradePosition.data["Trade_Date__c"].as_string(), Date).desc())
        .all()
    )

    groups = defaultdict(lambda: {'long': 0.0, 'short': 0.0})
    for pos in records:
        trade_date = pos.data.get('Trade_Date__c') or ''
        contract   = (pos.data.get('Contract__c') or '').replace(' ', '')
        price      = pos.data.get('Price__c')
        key = (trade_date, contract, price)
        groups[key]['long']  += float(pos.data.get('Long__c')  or 0)
        groups[key]['short'] += float(pos.data.get('Short__c') or 0)

    return sorted(
        [
            {
                'trade_date': k[0],
                'contract':   k[1],
                'price':      k[2],
                'long':       v['long'],
                'short':      v['short'],
                'net':        v['long'] + v['short'],
            }
            for k, v in groups.items()
            if round(v['long'] + v['short'], 8) != 0
        ],
        key=lambda x: x['trade_date'],
        reverse=True,
    )


@strategy_warnings_bp.route("/strategy-warnings")
def index():
    net_groups = get_warning_groups()
    return render_template("strategy_warnings.html", groups=net_groups)

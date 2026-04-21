import time
from threading import Lock
from simple_salesforce import Salesforce
from flask import current_app


# Module-level session cache. Salesforce session TTL defaults to ~2 hours, so
# refreshing every 20 min keeps us comfortably inside that window — we should
# not hit SalesforceExpiredSession in practice. invalidate_sf_session() is
# exposed so callers can force a refresh after catching one if it ever happens.
_SF_TTL_SECONDS = 20 * 60
_sf_cache = {"sf": None, "expires_at": 0.0}
_sf_lock = Lock()


def _new_sf_connection():
    return Salesforce(
        username=current_app.config["SF_USERNAME"],
        password=current_app.config["SF_PASSWORD"],
        security_token=current_app.config["SF_SECURITY_TOKEN"],
        domain=current_app.config["SF_DOMAIN"],
    )


def get_sf_connection():
    """Return a cached authenticated Salesforce connection (~20 min TTL)."""
    now = time.time()
    with _sf_lock:
        if _sf_cache["sf"] is not None and now < _sf_cache["expires_at"]:
            return _sf_cache["sf"]
        sf = _new_sf_connection()
        _sf_cache["sf"] = sf
        _sf_cache["expires_at"] = now + _SF_TTL_SECONDS
        return sf


def invalidate_sf_session():
    """Force the next get_sf_connection() call to re-authenticate."""
    with _sf_lock:
        _sf_cache["sf"] = None
        _sf_cache["expires_at"] = 0.0


def list_custom_objects(sf):
    """Return names of all custom objects available in the Salesforce org."""
    result = sf.describe()
    custom_objects = [
        obj["name"]
        for obj in result["sobjects"]
        if obj["custom"] and obj["queryable"]
    ]
    return sorted(custom_objects)


def _soql_quote_list(values):
    """Safely quote a list of string values for a SOQL IN (...) clause."""
    escaped = [str(v).replace("\\", "\\\\").replace("'", "\\'") for v in values]
    return ", ".join(f"'{v}'" for v in escaped)


def fetch_trade_records(sf, object_name, commodity_names):
    """
    Query filtered trade records from the given Salesforce custom object.
    Filters:
      - Account_No__c in ('08290CA', 'LSU15001')
      - Commodity_Name__c in (commodity_names)
      - Trade_Date__c > 2025-03-31
    Returns a list of dicts (one per record).
    """
    # Describe the object to get all field names, then swap lookup fields
    # for their relationship Name equivalents
    obj_desc = getattr(sf, object_name).describe()
    fields = [f["name"] for f in obj_desc["fields"]]

    # For lookup fields, keep the ID field AND add the __r.Name traversal
    relationship_fields = {
        "New_AGP__c": "New_AGP__r.Name",
        "New_AGS__c": "New_AGS__r.Name",
    }
    new_fields = []
    for f in fields:
        new_fields.append(f)
        if f in relationship_fields:
            new_fields.append(relationship_fields[f])
    fields = new_fields

    soql = (
        f"SELECT {', '.join(fields)} FROM {object_name} "
        f"WHERE Account_No__c IN ('08290CA', 'LSU15001') "
        f"AND Commodity_Name__c IN ({_soql_quote_list(commodity_names)}) "
        f"AND Trade_Date__c > 2025-03-31 "
        f"AND (Trade_Date__c > 2026-02-01 OR Trader__c != null)"
    )

    result = sf.query_all(soql)
    records = []
    for rec in result["records"]:
        rec.pop("attributes", None)
        # Flatten relationship fields: {"New_AGP__r": {"Name": "x"}} → {"New_AGP__r.Name": "x"}
        for rel_key in ("New_AGP__r", "New_AGS__r"):
            if rel_key in rec and isinstance(rec[rel_key], dict):
                rec[f"{rel_key}.Name"] = rec[rel_key].get("Name", "")
                del rec[rel_key]
        records.append(rec)

    return records


def fetch_master_contract_id(sf, name):
    """
    Look up a Master_Contract__c record's Id by Name.
    Returns the 18-char Id string, or None if not found.
    Single-quote escaped to be safe against Excel-sourced names.
    """
    if not name:
        return None
    safe_name = str(name).replace("\\", "\\\\").replace("'", "\\'")
    soql = f"SELECT Id FROM Master_Contract__c WHERE Name = '{safe_name}' LIMIT 1"
    result = sf.query(soql)
    if result.get("totalSize", 0) == 0:
        return None
    return result["records"][0]["Id"]


def fetch_report(sf, report_id):
    """
    Fetch a Salesforce tabular report by ID.
    Returns (column_labels, rows) where:
      - column_labels: list of strings (display labels, in order)
      - rows: list of dicts {label: cell_value, ...}
    """
    result = sf.restful(f"analytics/reports/{report_id}?includeDetails=true")
    col_api_names = result["reportMetadata"]["detailColumns"]
    col_info = result["reportExtendedMetadata"]["detailColumnInfo"]
    column_labels = [col_info[c]["label"] for c in col_api_names]

    rows = []
    for row in result["factMap"]["T!T"]["rows"]:
        cells = row["dataCells"]
        row_dict = {column_labels[i]: cells[i]["label"] for i in range(len(column_labels))}
        rows.append(row_dict)

    return column_labels, rows

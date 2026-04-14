"""
Internal transfer check: faithful port of `internal_transfer.ipynb`.

Fetches all Futur__c records where Broker_Name__c = 'Internal transfer',
groups by (Trade_Date__c, Contract__c, Price__c, Strike__c, Put_Call_2__c),
sums Long/Short, and reports any groups whose net quantity != 0.
Read-only — no Salesforce writes.
"""

from __future__ import annotations
from datetime import date
import pandas as pd


def fetch_internal_transfers(sf, start_date, end_date) -> pd.DataFrame:
    """
    Replicates notebook Cells 3 + 5.

    Queries Salesforce live (same reason as spec-check — local DB sync filter
    may exclude older trades). Filters to Internal transfer broker,
    sugar commodities, standard accounts, and the user's date window.

    Returns grouped DataFrame with columns:
        Trade_Date__c, Contract__c, Price__c, Strike__c, Put_Call_2__c,
        Long__c, Short__c, Broker_Name__c, quantity
    Only rows where quantity != 0 are returned (i.e. the imbalances).
    """
    start_iso = pd.Timestamp(start_date).strftime("%Y-%m-%d")
    end_iso = pd.Timestamp(end_date).strftime("%Y-%m-%d")

    soql = (
        "SELECT Id, Trade_Date__c, Strike__c, Put_Call_2__c, Status__c, "
        "Commodity_Name__c, Contract__c, Long__c, Short__c, Book__c, "
        "Account_No__c, Price__c, Broker_Name__c "
        "FROM Futur__c "
        f"WHERE Trade_Date__c > {start_iso} AND Trade_Date__c < {end_iso} "
        "AND Account_No__c IN ('08290CA', 'LSU15001') "
        "AND Commodity_Name__c IN ('ICE Raw Sugar', 'LDN Sugar #5') "
        "AND Broker_Name__c = 'Internal transfer'"
    )
    result = sf.query_all(soql)
    records = [r for r in result.get("records", [])]
    for r in records:
        r.pop("attributes", None)

    if not records:
        return pd.DataFrame(columns=[
            "Trade_Date__c", "Contract__c", "Price__c", "Strike__c",
            "Put_Call_2__c", "Long__c", "Short__c", "Broker_Name__c", "quantity",
        ])

    df = pd.DataFrame(records)
    df["Trade_Date__c"] = pd.to_datetime(df["Trade_Date__c"], errors="coerce")
    df[["Long__c", "Short__c"]] = df[["Long__c", "Short__c"]].fillna(0)

    # Notebook Cell 3 — date normalizations. Some internal-transfer pairs
    # have legs booked on adjacent dates (e.g. one leg on Dec 31, the other
    # on Jan 1). Without aligning them, the groupby treats them as separate
    # groups and both appear as imbalances. These are known date-entry
    # corrections in Salesforce.
    df["Trade_Date__c"] = df["Trade_Date__c"].replace(
        pd.Timestamp("2026-01-01"), pd.Timestamp("2025-12-31")
    )
    df["Trade_Date__c"] = df["Trade_Date__c"].replace(
        pd.Timestamp("2025-03-31"), pd.Timestamp("2025-04-01")
    )

    # Notebook Cell 5 — group separately for futures (NaN strike) vs options.
    nan_mask = df["Strike__c"].isna() & df["Put_Call_2__c"].isna()
    nan_rows = df[nan_mask].copy()
    non_nan_rows = df[~nan_mask].copy()

    group_keys_options = ["Trade_Date__c", "Contract__c", "Price__c", "Strike__c", "Put_Call_2__c"]
    group_keys_futures = ["Trade_Date__c", "Contract__c", "Price__c"]
    agg_dict = {"Long__c": "sum", "Short__c": "sum", "Broker_Name__c": "first"}

    if not non_nan_rows.empty:
        non_nan_grp = non_nan_rows.groupby(group_keys_options).agg(agg_dict).reset_index()
    else:
        non_nan_grp = pd.DataFrame(columns=group_keys_options + list(agg_dict.keys()))

    if not nan_rows.empty:
        nan_grp = nan_rows.groupby(group_keys_futures).agg(agg_dict).reset_index()
        nan_grp["Strike__c"] = float("nan")
        nan_grp["Put_Call_2__c"] = float("nan")
    else:
        nan_grp = pd.DataFrame(columns=group_keys_options + list(agg_dict.keys()))

    grp = pd.concat([non_nan_grp, nan_grp], ignore_index=True)
    grp["quantity"] = grp["Long__c"] + grp["Short__c"]

    # Only keep rows that DON'T net to zero — these are the problems.
    imbalances = grp[grp["quantity"] != 0].copy()
    imbalances = imbalances.sort_values(["Trade_Date__c", "Contract__c"]).reset_index(drop=True)
    return imbalances


def build_it_check_preview(sf, start_date, end_date) -> dict:
    """Run the check and return a dict suitable for staging/rendering."""
    imbalances = fetch_internal_transfers(sf, start_date, end_date)

    rows = []
    for _, r in imbalances.iterrows():
        td = r["Trade_Date__c"]
        rows.append({
            "Trade_Date__c": td.strftime("%Y-%m-%d") if pd.notna(td) else "",
            "Contract__c": r["Contract__c"],
            "Price__c": float(r["Price__c"]) if pd.notna(r["Price__c"]) else 0,
            "Strike__c": float(r["Strike__c"]) if pd.notna(r["Strike__c"]) else None,
            "Put_Call_2__c": r["Put_Call_2__c"] if pd.notna(r["Put_Call_2__c"]) else None,
            "Long__c": float(r["Long__c"]),
            "Short__c": float(r["Short__c"]),
            "quantity": float(r["quantity"]),
        })

    return {
        "start_date": pd.Timestamp(start_date).date(),
        "end_date": pd.Timestamp(end_date).date(),
        "total_it_records": int(len(imbalances)) if not imbalances.empty else 0,
        "rows": rows,
        "all_balanced": len(rows) == 0,
    }

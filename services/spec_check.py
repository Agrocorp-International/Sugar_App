"""
Spec position check: faithful port of `1_sugar_overall_spec.ipynb`.

Compares the "Summary" sheet of sugarm2m*.xlsm against a live Salesforce
query (filtered to Spec / Open) and reports per-contract differences.
Read-only — no Salesforce writes.
"""

from __future__ import annotations

from datetime import datetime, date

import pandas as pd


# Notebook Cell 10 — month letter ↔ month-name lookup.
MONTHS_V = {
    "JAN": "F", "FEB": "G", "MAR": "H", "APR": "J", "MAY": "K", "JUN": "M",
    "JUL": "N", "AUG": "Q", "SEP": "U", "OCT": "V", "NOV": "X", "DEC": "Z",
}
MONTHS_V_REVERSE = {v: k for k, v in MONTHS_V.items()}


# =============================================================================
# Step 1 — read the Summary sheet (notebook Cell 6)
# =============================================================================

def read_spec_xlsx(file_storage) -> pd.DataFrame:
    """Exact port of notebook Cell 6."""
    sugarxl = pd.read_excel(file_storage, sheet_name="Summary", skiprows=39, engine="openpyxl")

    sugarxl = sugarxl.iloc[:, :3]

    grand_total_idx = sugarxl[
        sugarxl.iloc[:, 0].astype(str).str.strip().str.lower() == "grand total"
    ].index.min()

    if pd.notna(grand_total_idx):
        sugarxl = sugarxl.iloc[:grand_total_idx]

    sugarxl.columns = ["Contract", "Long/Short", "Position"]
    sugarxl["Contract"] = sugarxl["Contract"].str.replace(" ", "")

    return sugarxl


# =============================================================================
# Step 2 — fetch & aggregate SF spec positions (notebook Cells 8-11)
# =============================================================================

def fetch_spec_internals(sf, start_date, end_date) -> pd.DataFrame:
    """Exact port of notebook Cells 8-11, with filters pushed to SOQL for speed."""
    start_iso = pd.Timestamp(start_date).strftime("%Y-%m-%d")
    end_iso = pd.Timestamp(end_date).strftime("%Y-%m-%d")

    # Cell 8 — SOQL is case-insensitive for strings, so this matches the
    # notebook's .str.lower() == "open" / "spec" filtering.
    soql = (
        "SELECT Id, Trade_Date__c, Strike__c, Put_Call_2__c, Status__c, "
        "Commodity_Name__c, Contract__c, Long__c, Short__c, Book__c, "
        "Contract_type__c, Account_No__c, Price__c, Broker_Name__c "
        "FROM Futur__c "
        f"WHERE Trade_Date__c > {start_iso} AND Trade_Date__c < {end_iso} "
        "AND Account_No__c IN ('08290CA', 'LSU15001') "
        "AND Commodity_Name__c IN ('ICE Raw Sugar', 'LDN Sugar #5') "
        "AND Status__c = 'OPEN' "
        "AND Book__c = 'Spec'"
    )
    result = sf.query_all(soql)
    Internals = pd.json_normalize(result["records"], errors="ignore")

    if Internals.empty:
        return pd.DataFrame(columns=["Contract__c", "quantity"])

    # Cell 9 — group separately for futures (NaN strike) vs options
    nan_mask = Internals["Strike__c"].isna() & Internals["Put_Call_2__c"].isna()
    nan_rows = Internals[nan_mask].copy()
    non_nan_rows = Internals[~nan_mask].copy()

    non_nan_grp = non_nan_rows.groupby(
        ["Contract__c", "Strike__c", "Put_Call_2__c"]
    ).agg({"Long__c": "sum", "Short__c": "sum"}).reset_index()

    nan_grp = nan_rows.groupby(["Contract__c"]).agg(
        {"Long__c": "sum", "Short__c": "sum"}
    ).reset_index()
    nan_grp["Strike__c"] = float("nan")
    nan_grp["Put_Call_2__c"] = float("nan")

    Internals_grp = pd.concat([non_nan_grp, nan_grp], ignore_index=True)
    Internals_grp["quantity"] = Internals_grp["Long__c"] + Internals_grp["Short__c"]
    Internals_grp = Internals_grp[Internals_grp["quantity"] != 0]

    # Cell 10 — drop expired contracts
    cur_date = datetime.today()
    for index, row in Internals_grp.iterrows():
        contract = row["Contract__c"]
        monthyear = contract[-3:]
        month = MONTHS_V_REVERSE[monthyear[0]]
        year = "20" + monthyear[1:]
        contract_date = datetime.strptime(f"{year}-{month}", "%Y-%b")
        if contract_date.year < cur_date.year or (
            contract_date.year == cur_date.year and contract_date.month < cur_date.month
        ):
            Internals_grp.drop(index, inplace=True)
    Internals_grp.reset_index(drop=True, inplace=True)

    # Cell 11 — compound option contract codes
    for index, row in Internals_grp.iterrows():
        contract = row["Contract__c"]
        strike = row["Strike__c"]
        put_call = row["Put_Call_2__c"]
        if pd.notna(strike) and pd.notna(put_call):
            strike_str = str(int(strike * 100))
            put_call_char = "P" if put_call.lower() == "put" else "C" if put_call.lower() == "call" else ""
            Internals_grp.at[index, "Contract__c"] = f"{contract}{put_call_char}{strike_str}"
    Internals_grp.drop(columns=["Strike__c", "Put_Call_2__c"], inplace=True)

    return Internals_grp


# =============================================================================
# Step 3 — compare (notebook Cell 13)
# =============================================================================

def compare_spec(sugarxl: pd.DataFrame, Internals_grp: pd.DataFrame) -> dict:
    """Exact port of notebook Cell 13."""
    merged_df = pd.merge(
        sugarxl, Internals_grp,
        left_on="Contract", right_on="Contract__c", how="left",
    )
    merged_df = merged_df[["Contract", "Long/Short", "quantity"]]
    merged_df.columns = ["Contract", "Excel", "Salesforce"]
    merged_df = merged_df.fillna(0)
    merged_df["Difference"] = merged_df["Excel"] - merged_df["Salesforce"]

    rows = []
    for _, r in merged_df.iterrows():
        rows.append({
            "Contract": r["Contract"],
            "Excel": float(r["Excel"]),
            "Salesforce": float(r["Salesforce"]),
            "Difference": float(r["Difference"]),
        })

    excel_contracts = set(sugarxl["Contract"].dropna().astype(str).tolist()) if not sugarxl.empty else set()
    sf_contracts = set(Internals_grp["Contract__c"].dropna().astype(str).tolist()) if not Internals_grp.empty else set()

    return {
        "rows": rows,
        "discrepancies": [r for r in rows if r["Difference"] != 0],
        "in_excel_only": len(excel_contracts - sf_contracts),
        "in_sf_only": len(sf_contracts - excel_contracts),
        "matched": len(excel_contracts & sf_contracts),
        "all_match": all(r["Difference"] == 0 for r in rows),
    }


# =============================================================================
# Convenience: end-to-end preview build
# =============================================================================

def build_spec_preview(sf, file_storage, start_date, end_date) -> dict:
    sugarxl = read_spec_xlsx(file_storage)
    Internals_grp = fetch_spec_internals(sf, start_date, end_date)
    result = compare_spec(sugarxl, Internals_grp)
    return {
        "filename": getattr(file_storage, "filename", None),
        "start_date": pd.Timestamp(start_date).date(),
        "end_date": pd.Timestamp(end_date).date(),
        "excel_contract_count": int(len(sugarxl)),
        "sf_contract_count": int(len(Internals_grp)),
        "rows": result["rows"],
        "discrepancies": result["discrepancies"],
        "in_excel_only": result["in_excel_only"],
        "in_sf_only": result["in_sf_only"],
        "matched": result["matched"],
        "all_match": result["all_match"],
    }

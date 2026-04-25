"""
Auto-tag service: faithful port of `9_sugar_auto_tagging.ipynb`.

Pipeline:
    1. read_trades_xlsx(file, start, end)        -> sugarxl_grp
    2. fetch_internals(sf, start, end)           -> internals (raw, with compound contract)
    3. aggregate_internals(internals)            -> internals_grp
    4. match_trades(sugarxl_grp, internals_grp)  -> MatchResult
    5. build_update_batches(unmatched_excel,
                            internals)            -> UpdateBatches (Path 1, 2, 3)
    6. execute_full_push(sf, batches)            -> dict[str, PushReport]

The matching merge uses 15 keys (notebook Cell 13). The three update batches
are produced by Cells 19, 22-23, and 26-28 respectively.
"""

from __future__ import annotations

from dataclasses import dataclass, field, asdict
from datetime import date, datetime
from pathlib import Path
import os
import pickle
import tempfile
import time
import uuid

import numpy as np
import pandas as pd

from services.salesforce import fetch_master_contract_id


# ── Lookup tables (verbatim from notebook Cell 17) ───────────────────────────

COMMODITY_NAME = {"SW": "LDN Sugar #5", "SB": "ICE Raw Sugar"}
ACCOUNT = {"Marex": "08290CA", "Internal transfer": "08290CA", "FC_Stone": "LSU15001"}
BOOK = {"Raws": "Hedge", "Whites": "Hedge", "Alpha": "Spec"}
INSTRUMENT = {"Futures": "Futures", "Spread": "Futures", "Options": "Option"}


# ── Salesforce field set fetched for matching (notebook Cell 7) ──────────────

SF_FIELDS = [
    "Id", "Trade_Date__c", "Strike__c", "Put_Call_2__c", "Status__c",
    "Commodity_Name__c", "Contract__c", "Long__c", "Short__c", "Book__c",
    "Contract_type__c", "Account_No__c", "Price__c", "Broker_Name__c",
    "New_AGP__c", "New_AGS__c", "New_AGP__r.Name", "New_AGS__r.Name",
    "Broker_Commission__c", "Realised__c", "Trader__c", "Trade_Code__c",
    "Trade_Key__c", "Trade_Group__c", "Strategy__c",
]


# ── Data classes ─────────────────────────────────────────────────────────────

@dataclass
class MatchResult:
    matched: pd.DataFrame
    unmatched_excel: pd.DataFrame
    unmatched_sf: pd.DataFrame


@dataclass
class UpdateBatches:
    sf_update_1: pd.DataFrame          # Path 1: Long-or-Short single-side rows
    sf_update_2: pd.DataFrame          # Path 2: rows that have BOTH Long and Short
    final_df: pd.DataFrame             # Path 3: manual / split with broker commission
    missing_option_creates: pd.DataFrame  # Path 3b: option close/expiry entries (price=0, no SF Id)


@dataclass
class PushReport:
    batch_name: str
    created: list = field(default_factory=list)   # list of new Salesforce Ids
    updated: list = field(default_factory=list)   # list of existing Ids that were updated
    skipped: list = field(default_factory=list)   # list of (row_label, reason)
    errors:  list = field(default_factory=list)   # list of (row_label, message)


# =============================================================================
# Step 1 — read Excel
# =============================================================================

def read_trades_xlsx(file_storage, start_date, end_date) -> pd.DataFrame:
    """
    Replicates notebook Cells 3-5.

    Returns the *grouped* sugarxl dataframe with columns:
        Trade Date, Book, Contract, Trade Price, Contract Ref, Contract Ref SF,
        Account, Instrument, Status, Trader, Trade Code, Trade ID, Group,
        Spread Contract, Long, Short, quantity
    """
    start_ts = pd.Timestamp(start_date)
    end_ts = pd.Timestamp(end_date)

    sugarxl = pd.read_excel(file_storage, sheet_name="Trades", engine="openpyxl")

    sugarxl["Trade Date"] = pd.to_datetime(sugarxl["Trade Date"], errors="coerce")

    # Notebook uses strict > / < — keep that semantics so end_date is exclusive.
    sugarxl = sugarxl[
        (sugarxl["Trade Date"] > start_ts) & (sugarxl["Trade Date"] < end_ts)
    ]

    sugarxl = sugarxl[
        (sugarxl["Account"] != "Dummy") & (sugarxl["Book"] != "Dummy")
    ]
    sugarxl = sugarxl[~(sugarxl["Trade Price"] == 0)]
    sugarxl = sugarxl.sort_values(by="Trade Date")

    sugarxl.loc[sugarxl["Book"] == "Alpha", "Contract Ref"] = ""
    sugarxl["Contract Ref"] = sugarxl["Contract Ref"].fillna("")
    sugarxl[["Long", "Short"]] = sugarxl[["Long", "Short"]].fillna(0)
    sugarxl["Short"] = sugarxl["Short"] * -1
    sugarxl[["Trade Code", "Trade ID", "Group", "Spread Contract"]] = (
        sugarxl[["Trade Code", "Trade ID", "Group", "Spread Contract"]]
        .fillna("")
        .astype(str)
    )

    sugarxl["Account"] = sugarxl["Account"].replace({
        "Fcstone": "FC_Stone",
        "Internal": "Internal transfer",
    })
    sugarxl["Status"] = sugarxl["Status"].replace({
        "Open": "Unrealised",
        "Closed": "Realised",
    })

    sugarxl["Contract"] = sugarxl["Contract"].str.replace(" ", "", regex=False)
    sugarxl["Contract Ref"] = sugarxl["Contract Ref"].str.replace(" ", "", regex=False)
    sugarxl["Contract Ref SF"] = (
        sugarxl["Contract Ref"].str.split("_").str[0].str.replace(" ", "", regex=False)
    )

    if "Brokerage Fees" not in sugarxl.columns:
        sugarxl["Brokerage Fees"] = 0.0

    sugarxl = sugarxl[[
        "Trade Date", "Account", "Book", "Long", "Short", "Trade Price",
        "Contract", "Contract Ref", "Contract Ref SF", "Instrument",
        "Status", "Trader", "Trade Code", "Trade ID", "Group", "Spread Contract",
        "Brokerage Fees",
    ]]

    grouped = sugarxl.groupby(
        [
            "Trade Date", "Book", "Contract", "Trade Price", "Contract Ref",
            "Contract Ref SF", "Account", "Instrument", "Status", "Trader",
            "Trade Code", "Trade ID", "Group", "Spread Contract",
        ],
    ).agg({"Long": "sum", "Short": "sum", "Brokerage Fees": "sum"}).reset_index()

    grouped["quantity"] = grouped["Long"] + grouped["Short"]
    grouped = grouped[grouped["quantity"] != 0]
    grouped["Brokerage Fees Strategy"] = (
        grouped["Brokerage Fees"].round(2).map(lambda x: f"BF={x:.2f}")
    )
    return grouped


# =============================================================================
# Step 2 — fetch Salesforce internals
# =============================================================================

def fetch_internals(sf, start_date, end_date) -> pd.DataFrame:
    """
    Replicates notebook Cells 7, 9, 15.

    Returns a dataframe with the same columns the notebook's `Internals`
    has *after* Cell 15 — i.e. with the compound option contract code already
    written into Contract__c, Strategy__c parsed into 4 _xl columns, and the
    AGP/AGS column populated. NOTE: this is the *non-aggregated* dataframe
    used by the rank-based merges in Cells 19 / 23 / 27, not Internals_grp.
    """
    start_iso = pd.Timestamp(start_date).strftime("%Y-%m-%d")
    end_iso = pd.Timestamp(end_date).strftime("%Y-%m-%d")

    field_list = ", ".join(SF_FIELDS)
    soql = (
        f"SELECT {field_list} FROM Futur__c "
        f"WHERE Trade_Date__c > {start_iso} AND Trade_Date__c < {end_iso}"
    )
    result = sf.query_all(soql)
    internals = pd.json_normalize(result["records"], errors="ignore")

    if internals.empty:
        # Build an empty frame with the columns downstream code expects.
        for col in [
            "Id", "Trade_Date__c", "Strike__c", "Put_Call_2__c", "Status__c",
            "Commodity_Name__c", "Contract__c", "Long__c", "Short__c",
            "Book__c", "Contract_type__c", "Account_No__c", "Price__c",
            "Broker_Name__c", "New_AGP__c", "New_AGS__c", "New_AGP__r.Name",
            "New_AGS__r.Name", "Broker_Commission__c", "Realised__c",
            "Trader__c", "Trade_Code__c", "Trade_Key__c", "Trade_Group__c",
            "Strategy__c",
        ]:
            if col not in internals.columns:
                internals[col] = pd.Series(dtype="object")

    internals["Trade_Date__c"] = pd.to_datetime(internals["Trade_Date__c"], errors="coerce")
    internals["Account_No__c"] = internals["Account_No__c"].astype(str).str.replace(" ", "", regex=False)

    # Filter account and commodity in Python after space removal (notebook Cell 7)
    internals = internals[internals["Account_No__c"].isin(["08290CA", "LSU15001"])]
    internals = internals[internals["Commodity_Name__c"].isin(["ICE Raw Sugar", "LDN Sugar #5"])]

    # Combine AGP/AGS into single ref column (Cell 7)
    internals["AGP/AGS"] = internals["New_AGP__r.Name"].fillna(internals["New_AGS__r.Name"])
    internals["AGP/AGS"] = internals["AGP/AGS"].fillna("")
    internals["Realised__c"] = internals["Realised__c"].fillna("")
    internals[["Long__c", "Short__c"]] = internals[["Long__c", "Short__c"]].fillna(0)
    internals["Book__c"] = internals["Book__c"].fillna("")
    fill_cols = ["Realised__c", "Trader__c", "Trade_Code__c", "Trade_Key__c", "Trade_Group__c", "Strategy__c"]
    internals[fill_cols] = internals[fill_cols].fillna("")

    # Cell 9 — split Strategy__c into 5 columns (Instrument, Spread, ContractRef, Book, BF=fee)
    split_cols = (
        internals["Strategy__c"]
        .fillna("")
        .str.split("-", n=4, expand=True)
        .apply(lambda col: col.str.strip())
    )
    split_cols = split_cols.reindex(columns=[0, 1, 2, 3, 4])
    internals[["Instrument_xl", "Spread_xl", "Contract_Ref_xl", "Book_xl", "Brokerage_Fees_xl"]] = split_cols
    internals[["Instrument_xl", "Spread_xl", "Contract_Ref_xl", "Book_xl", "Brokerage_Fees_xl"]] = (
        internals[["Instrument_xl", "Spread_xl", "Contract_Ref_xl", "Book_xl", "Brokerage_Fees_xl"]]
        .fillna("").astype(str)
    )
    internals["Brokerage_Fees_num"] = (
        internals["Brokerage_Fees_xl"]
        .str.replace("BF=", "", regex=False)
        .replace("", "0")
        .pipe(pd.to_numeric, errors="coerce")
        .fillna(0)
    )

    # Cell 15 — compound contract code for options
    for idx, row in internals.iterrows():
        contract = row["Contract__c"]
        strike = row["Strike__c"]
        put_call = row["Put_Call_2__c"]
        if pd.notna(strike) and pd.notna(put_call) and contract is not None:
            strike_str = str(int(strike * 100))
            pc = put_call.lower() if isinstance(put_call, str) else ""
            pc_char = "P" if pc == "put" else ("C" if pc == "call" else "")
            internals.at[idx, "Contract__c"] = f"{contract}{pc_char}{strike_str}"

    return internals


def aggregate_internals(internals: pd.DataFrame) -> pd.DataFrame:
    """
    Replicates notebook Cells 11-12. Produces Internals_grp used in the
    initial 15-key match merge.
    """
    if internals.empty:
        return internals.copy()

    nan_mask = internals["Strike__c"].isna() & internals["Put_Call_2__c"].isna()
    nan_rows = internals[nan_mask].copy()
    non_nan_rows = internals[~nan_mask].copy()

    non_nan_grp = non_nan_rows.groupby([
        "Trade_Date__c", "Book__c", "Contract__c", "Price__c",
        "Strike__c", "Put_Call_2__c", "AGP/AGS", "Broker_Name__c",
        "Realised__c", "Trader__c", "Trade_Code__c", "Trade_Key__c",
        "Trade_Group__c", "Strategy__c", "Instrument_xl", "Spread_xl",
        "Contract_Ref_xl", "Book_xl",
    ]).agg({"Long__c": "sum", "Short__c": "sum", "Brokerage_Fees_num": "sum"}).reset_index()

    nan_grp = nan_rows.groupby([
        "Trade_Date__c", "Book__c", "Contract__c", "Price__c",
        "AGP/AGS", "Broker_Name__c", "Realised__c", "Trader__c",
        "Trade_Code__c", "Trade_Key__c", "Trade_Group__c", "Strategy__c",
        "Instrument_xl", "Spread_xl", "Contract_Ref_xl", "Book_xl",
    ]).agg({"Long__c": "sum", "Short__c": "sum", "Brokerage_Fees_num": "sum"}).reset_index()

    nan_grp["Strike__c"] = float("nan")
    nan_grp["Put_Call_2__c"] = float("nan")

    grp = pd.concat([non_nan_grp, nan_grp], ignore_index=True)
    grp["quantity"] = grp["Long__c"] + grp["Short__c"]
    grp = grp[grp["quantity"] != 0]
    grp["Brokerage_Fees_xl"] = (
        grp["Brokerage_Fees_num"].round(2).map(lambda x: f"BF={x:.2f}")
    )
    return grp


# =============================================================================
# Step 3 — match
# =============================================================================

LEFT_KEYS = [
    "Trade Price", "quantity", "Contract", "Trade Date", "Contract Ref",
    "Contract Ref SF", "Account", "Status", "Trader", "Trade Code",
    "Trade ID", "Group", "Spread Contract", "Instrument", "Book",
    "Brokerage Fees Strategy",
]
RIGHT_KEYS = [
    "Price__c", "quantity", "Contract__c", "Trade_Date__c", "Contract_Ref_xl",
    "AGP/AGS", "Broker_Name__c", "Realised__c", "Trader__c", "Trade_Code__c",
    "Trade_Key__c", "Trade_Group__c", "Spread_xl", "Instrument_xl", "Book_xl",
    "Brokerage_Fees_xl",
]


def match_trades(sugarxl_grp: pd.DataFrame, internals_grp: pd.DataFrame) -> MatchResult:
    """Notebook Cell 13."""
    merged = pd.merge(
        sugarxl_grp,
        internals_grp,
        left_on=LEFT_KEYS,
        right_on=RIGHT_KEYS,
        how="inner",
    )

    unmatched_xl = sugarxl_grp[~sugarxl_grp.set_index(LEFT_KEYS).index.isin(
        merged.set_index(LEFT_KEYS).index
    )]
    unmatched_sf = internals_grp[~internals_grp.set_index(RIGHT_KEYS).index.isin(
        merged.set_index(RIGHT_KEYS).index
    )]
    unmatched_sf = unmatched_sf[unmatched_sf["Trade_Date__c"] != "2026-01-01"]

    return MatchResult(
        matched=merged,
        unmatched_excel=unmatched_xl,
        unmatched_sf=unmatched_sf,
    )


# =============================================================================
# Step 4 — build update batches (Paths 1, 2, 3)
# =============================================================================

XL_RANK_KEYS = ["Trade Price", "Contract", "Trade Date", "Account", "Long", "Short"]
SF_RANK_KEYS = ["Price__c", "Contract__c", "Trade_Date__c", "Broker_Name__c", "Long__c", "Short__c"]

PROJECT_COLS = [
    "Instrument", "Trade Price", "Contract", "Trade Date", "Account", "Long",
    "Short", "Id", "Price__c", "Long__c", "Short__c", "Contract__c",
    "Trade_Date__c", "Broker_Name__c", "Book__c", "Book", "New_AGP__c",
    "New_AGS__c", "Contract Ref", "Realised__c", "Status", "Status__c",
    "Trader", "Trader__c", "Trade Code", "Trade_Code__c", "Trade ID",
    "Trade_Key__c", "Group", "Trade_Group__c", "Spread Contract",
    "Strategy__c", "Contract Ref SF", "Brokerage Fees Strategy",
]

PROJECT_COLS_3 = PROJECT_COLS + ["Broker_Commission__c"]


def _ensure_columns(df: pd.DataFrame, cols: list) -> pd.DataFrame:
    out = df.copy()
    for c in cols:
        if c not in out.columns:
            out[c] = np.nan
    return out[cols]


def build_update_batches(
    unmatched_excel: pd.DataFrame, internals: pd.DataFrame
) -> UpdateBatches:
    """
    Replicates Cells 19, 22-23, 26-28.

    `internals` is the *non-aggregated* internals dataframe (after fetch_internals).
    """
    # ---------- Path 1 (Cell 19) ----------
    xl_ranked = unmatched_excel.copy()
    xl_ranked["_rank"] = xl_ranked.groupby(XL_RANK_KEYS).cumcount()

    sf_ranked = internals.copy()
    sf_ranked["_rank"] = sf_ranked.groupby(SF_RANK_KEYS).cumcount()

    records_to_update = pd.merge(
        xl_ranked,
        sf_ranked,
        left_on=XL_RANK_KEYS + ["_rank"],
        right_on=SF_RANK_KEYS + ["_rank"],
        how="left",
    ).drop(columns=["_rank"])

    records_to_update = _ensure_columns(records_to_update, PROJECT_COLS)

    rt1 = records_to_update[
        ((records_to_update["Long"] != 0) & (records_to_update["Short"] == 0)) |
        ((records_to_update["Long"] == 0) & (records_to_update["Short"] != 0))
    ]
    sf_update_1 = rt1[
        ~((rt1["Id"].isna()) & (rt1["Account"] != "Internal transfer"))
    ]
    manual_update_1 = rt1[(rt1["Id"].isna()) & (rt1["Account"] != "Internal transfer")]

    # ---------- Path 2 (Cells 22-23) ----------
    rt2_src = records_to_update[
        (records_to_update["Long"] != 0) & (records_to_update["Short"] != 0)
    ]
    rt2_src = rt2_src[[
        "Instrument", "Trade Price", "Contract", "Trade Date", "Account",
        "Long", "Short", "Book", "Contract Ref", "Status", "Trader",
        "Trade Code", "Trade ID", "Group", "Spread Contract", "Contract Ref SF",
        "Brokerage Fees Strategy",
    ]]
    rt2_a = rt2_src.copy()
    rt2_b = rt2_src.copy()
    rt2_a["Long"] = rt2_a["Long"] * 0
    rt2_b["Short"] = rt2_b["Short"] * 0
    rt2 = pd.concat([rt2_a, rt2_b], ignore_index=True)

    rt2_ranked = rt2.copy()
    rt2_ranked["_rank"] = rt2_ranked.groupby(XL_RANK_KEYS).cumcount()
    sf_ranked_2 = internals.copy()
    sf_ranked_2["_rank"] = sf_ranked_2.groupby(SF_RANK_KEYS).cumcount()

    rt2_final = pd.merge(
        rt2_ranked,
        sf_ranked_2,
        left_on=XL_RANK_KEYS + ["_rank"],
        right_on=SF_RANK_KEYS + ["_rank"],
        how="left",
    ).drop(columns=["_rank"])

    rt2_final = _ensure_columns(rt2_final, PROJECT_COLS)

    sf_update_2 = rt2_final[
        ~((rt2_final["Id"].isna()) & (rt2_final["Account"] != "Internal transfer"))
    ]
    manual_update_2 = rt2_final[
        (rt2_final["Id"].isna()) & (rt2_final["Account"] != "Internal transfer")
    ]

    # ---------- Path 3 (Cells 26-28) ----------
    manual_df = pd.concat([manual_update_1, manual_update_2], ignore_index=True)
    if not manual_df.empty:
        manual_df["Total Long"] = manual_df.groupby(
            ["Trade Price", "Contract", "Trade Date", "Account"]
        )["Long"].transform("sum")
        manual_df["Total Short"] = manual_df.groupby(
            ["Trade Price", "Contract", "Trade Date", "Account"]
        )["Short"].transform("sum")
    else:
        manual_df["Total Long"] = pd.Series(dtype="float64")
        manual_df["Total Short"] = pd.Series(dtype="float64")

    manual_df = manual_df[[
        "Instrument", "Trade Price", "Contract", "Trade Date", "Account",
        "Long", "Short", "Total Long", "Total Short", "Book", "Contract Ref",
        "Status", "Trader", "Trade Code", "Trade ID", "Group",
        "Spread Contract", "Contract Ref SF", "Brokerage Fees Strategy",
    ]]

    rt3 = pd.merge(
        manual_df,
        internals,
        left_on=["Trade Price", "Contract", "Trade Date", "Account", "Total Long", "Total Short"],
        right_on=["Price__c", "Contract__c", "Trade_Date__c", "Broker_Name__c", "Long__c", "Short__c"],
        how="left",
    )
    rt3 = _ensure_columns(rt3, PROJECT_COLS_3)

    final_records = []
    if not rt3.empty:
        for sf_id, group in rt3.groupby("Id"):
            group = group.copy().reset_index(drop=True)
            orig_long = group["Long__c"].iloc[0]
            orig_short = group["Short__c"].iloc[0]
            if pd.notna(orig_long) and orig_long != 0:
                total_sf_qty = abs(orig_long)
            elif pd.notna(orig_short) and orig_short != 0:
                total_sf_qty = abs(orig_short)
            else:
                total_sf_qty = None

            for i, row in group.iterrows():
                new_row = row.copy()
                if pd.notna(row["Long"]) and row["Long"] != 0:
                    split_qty = abs(row["Long"])
                    new_row["Long__c"] = row["Long"]
                    new_row["Short__c"] = None
                elif pd.notna(row["Short"]) and row["Short"] != 0:
                    split_qty = abs(row["Short"])
                    new_row["Short__c"] = row["Short"]
                    new_row["Long__c"] = None
                else:
                    continue

                bc = row.get("Broker_Commission__c")
                if total_sf_qty and pd.notna(bc):
                    new_row["Broker_Commission__c"] = bc * split_qty / total_sf_qty
                else:
                    new_row["Broker_Commission__c"] = None

                if i != 0:
                    new_row["Id"] = None
                final_records.append(new_row)

    final_df = pd.DataFrame(final_records) if final_records else pd.DataFrame(columns=PROJECT_COLS_3)

    missing_option_creates = rt3[
        rt3["Id"].isna()
        & (rt3["Instrument"] == "Options")
        & (pd.to_numeric(rt3["Trade Price"], errors="coerce") == 0)
    ].copy() if not rt3.empty else pd.DataFrame(columns=PROJECT_COLS_3)

    return UpdateBatches(
        sf_update_1=sf_update_1.reset_index(drop=True),
        sf_update_2=sf_update_2.reset_index(drop=True),
        final_df=final_df.reset_index(drop=True),
        missing_option_creates=missing_option_creates.reset_index(drop=True),
    )


# =============================================================================
# Step 5 — push to Salesforce
# =============================================================================

def _clean_sf_values(d):
    """Notebook clean_salesforce_values — drop NaN, format Timestamp."""
    out = {}
    for k, v in d.items():
        if v is None:
            out[k] = None
        elif isinstance(v, float) and np.isnan(v):
            out[k] = None
        elif isinstance(v, pd.Timestamp):
            out[k] = v.strftime("%Y-%m-%d")
        elif isinstance(v, (datetime, date)):
            out[k] = v.strftime("%Y-%m-%d") if hasattr(v, "strftime") else v
        else:
            try:
                if pd.isna(v):
                    out[k] = None
                    continue
            except (TypeError, ValueError):
                pass
            out[k] = v
    return out


def _row_to_sf_fields(sf, row, *, include_broker_commission, is_create, push_report):
    """
    Build the field dict for one row, mirroring notebook Cell 20.
    Resolves AGP/AGS lookup against Master_Contract__c via the helper.
    Returns the fields dict, or None if the row should be skipped.
    """
    try:
        update_fields = {
            "Book__c": BOOK[row["Book"]],
            "Trade_Date__c": row["Trade Date"],
            "Price__c": row["Trade Price"],
            "Broker_Name__c": row["Account"],
            "Contract__c": row["Contract"][:5],
            "Commodity_Name__c": COMMODITY_NAME[row["Contract"][:2]],
            "Account_No__c": ACCOUNT[row["Account"]],
            "Contract_type__c": INSTRUMENT[row["Instrument"]],
            "QuoteCurrency__c": "USD",
            "Status__c": row.get("Status__c"),
            "Realised__c": row["Status"],
            "Trader__c": row["Trader"],
            "Trade_Code__c": row["Trade Code"],
            "Trade_Key__c": row["Trade ID"],
            "Trade_Group__c": row["Group"],
            "Strategy__c": f'{row["Instrument"]}-{row["Spread Contract"]}-{row["Contract Ref"]}-{row["Book"]}-{row.get("Brokerage Fees Strategy", "BF=0.00")}',
        }
    except KeyError as e:
        push_report.errors.append((str(row.get("Trade ID", "?")), f"Lookup miss: {e}"))
        return None

    if include_broker_commission and "Broker_Commission__c" in row.index:
        update_fields["Broker_Commission__c"] = row["Broker_Commission__c"]

    long_val = row["Long"]
    short_val = row["Short"]
    if pd.notna(long_val) and long_val != 0:
        update_fields["Long__c"] = long_val
    elif pd.notna(short_val) and short_val != 0:
        update_fields["Short__c"] = short_val

    contract_name = row.get("Contract Ref SF")
    if isinstance(contract_name, str):
        if contract_name[:3] == "AGS":
            mc_id = fetch_master_contract_id(sf, contract_name)
            if mc_id:
                update_fields["New_AGS__c"] = mc_id
                update_fields["New_AGP__c"] = None
            else:
                push_report.errors.append(
                    (str(row.get("Trade ID", "?")),
                     f"Master Contract '{contract_name}' not found for AGS")
                )
        elif contract_name[:3] == "AGP":
            mc_id = fetch_master_contract_id(sf, contract_name)
            if mc_id:
                update_fields["New_AGP__c"] = mc_id
                update_fields["New_AGS__c"] = None
            else:
                push_report.errors.append(
                    (str(row.get("Trade ID", "?")),
                     f"Master Contract '{contract_name}' not found for AGP")
                )
        else:
            update_fields["New_AGP__c"] = None
            update_fields["New_AGS__c"] = None

    if row["Instrument"] == "Options":
        contract_full = row["Contract"]
        if isinstance(contract_full, str) and len(contract_full) >= 7:
            pc_char = contract_full[5]
            try:
                strike_raw = int(contract_full[6:])
                update_fields["Put_Call_2__c"] = "Put" if pc_char == "P" else "Call"
                update_fields["Strike__c"] = strike_raw / 100
            except ValueError:
                push_report.errors.append(
                    (str(row.get("Trade ID", "?")),
                     f"Could not parse option strike from '{contract_full}'")
                )

    if row["Book"] in ("Raws", "Whites"):
        agp_empty = update_fields.get("New_AGP__c") in (None, "") or pd.isna(update_fields.get("New_AGP__c"))
        ags_empty = update_fields.get("New_AGS__c") in (None, "") or pd.isna(update_fields.get("New_AGS__c"))
        if agp_empty and ags_empty:
            push_report.skipped.append(
                (str(row.get("Trade ID", "?")), "Hedge row missing AGP/AGS")
            )
            update_fields["Book__c"] = ""

    cleaned = _clean_sf_values(update_fields)
    if is_create:
        cleaned["Status__c"] = "OPEN"
    return cleaned


def push_batch(
    sf,
    batch_df: pd.DataFrame,
    *,
    batch_name: str,
    include_broker_commission: bool,
    create_internal_transfer_only: bool,
) -> PushReport:
    report = PushReport(batch_name=batch_name)
    if batch_df is None or batch_df.empty:
        return report

    for _, row in batch_df.iterrows():
        create_record = pd.isna(row.get("Id")) if "Id" in row.index else True
        if create_internal_transfer_only:
            create_record = create_record and row.get("Account") == "Internal transfer"

        cleaned = _row_to_sf_fields(
            sf, row,
            include_broker_commission=include_broker_commission,
            is_create=create_record,
            push_report=report,
        )
        if cleaned is None:
            continue

        try:
            if create_record:
                instr = row.get("Instrument", "")
                price = pd.to_numeric(row.get("Trade Price"), errors="coerce")
                if instr == "Options" and pd.notna(price) and price != 0:
                    report.skipped.append((str(row.get("Trade ID", "?")), "Option create skipped — non-zero price"))
                    continue
                if instr == "Options" and (pd.isna(price) or price == 0):
                    cleaned["Status__c"] = "CLOSE"
                    cleaned["Closed_Date__c"] = datetime.today().strftime("%Y-%m-%d")
                result = sf.Futur__c.create(cleaned)
                report.created.append(result.get("id"))
            else:
                if pd.isna(row.get("Id")):
                    report.skipped.append((str(row.get("Trade ID", "?")), "Missing SF Id, not Internal transfer"))
                    continue
                sf.Futur__c.update(row["Id"], cleaned)
                report.updated.append(row["Id"])
        except Exception as e:  # noqa: BLE001
            report.errors.append((str(row.get("Trade ID", "?")), str(e)))
    return report


def execute_full_push(sf, batches: UpdateBatches) -> dict:
    return {
        "sf_update_1": push_batch(
            sf, batches.sf_update_1,
            batch_name="sf_update_1",
            include_broker_commission=False,
            create_internal_transfer_only=True,
        ),
        "sf_update_2": push_batch(
            sf, batches.sf_update_2,
            batch_name="sf_update_2",
            include_broker_commission=False,
            create_internal_transfer_only=True,
        ),
        "missing_option_creates": push_batch(
            sf, batches.missing_option_creates,
            batch_name="missing_option_creates",
            include_broker_commission=False,
            create_internal_transfer_only=False,
        ),
        "final_df": push_batch(
            sf, batches.final_df,
            batch_name="final_df",
            include_broker_commission=True,
            create_internal_transfer_only=False,
        ),
    }


# =============================================================================
# Step 6 — staging (preview → confirm) via tempfile
# =============================================================================

def _persistent_base_dir() -> Path:
    # On Azure App Service Linux, /home is a persistent share. Elsewhere, use tempdir.
    if os.environ.get("WEBSITE_SITE_NAME"):
        return Path("/home/data/sugar_admin")
    return Path(tempfile.gettempdir())


STAGE_DIR = _persistent_base_dir() / "sugar_auto_tag"
STAGE_TTL_SECONDS = 60 * 60  # 1 hour


def _ensure_stage_dir():
    STAGE_DIR.mkdir(parents=True, exist_ok=True)


def _purge_stale():
    if not STAGE_DIR.exists():
        return
    cutoff = time.time() - STAGE_TTL_SECONDS
    for p in STAGE_DIR.glob("*.pkl"):
        try:
            if p.stat().st_mtime < cutoff:
                p.unlink()
        except OSError:
            pass


def stage_to_tempfile(payload: dict) -> str:
    """Pickle a payload (batches + metadata + unmatched dataframes) and return a token."""
    _ensure_stage_dir()
    _purge_stale()
    token = uuid.uuid4().hex
    path = STAGE_DIR / f"{token}.pkl"
    with open(path, "wb") as f:
        pickle.dump(payload, f)
    return token


def load_staged(token: str):
    if not token:
        return None
    path = STAGE_DIR / f"{token}.pkl"
    if not path.exists():
        return None
    try:
        with open(path, "rb") as f:
            return pickle.load(f)
    except Exception:
        return None


def discard_staged(token: str):
    if not token:
        return
    path = STAGE_DIR / f"{token}.pkl"
    try:
        path.unlink()
    except OSError:
        pass


# =============================================================================
# Convenience: end-to-end preview build
# =============================================================================

def build_preview(sf, file_storage, start_date, end_date) -> dict:
    """
    Run pipeline steps 1-4 and package everything needed for preview + confirm.
    Returns a dict suitable for stage_to_tempfile().
    """
    sugarxl_grp = read_trades_xlsx(file_storage, start_date, end_date)
    internals_raw = fetch_internals(sf, start_date, end_date)
    internals_grp = aggregate_internals(internals_raw)
    match = match_trades(sugarxl_grp, internals_grp)
    batches = build_update_batches(match.unmatched_excel, internals_raw)

    by_book_xl = _by_book_summary(
        match.unmatched_excel, book_col="Book", long_col="Long", short_col="Short"
    )
    by_book_sf = _by_book_summary(
        match.unmatched_sf, book_col="Book__c", long_col="Long__c", short_col="Short__c"
    )

    # Diagnose where any unmatched-Excel vs unmatched-SF asymmetry comes from.
    # The matching is the inner-merge from notebook Cell 13. After grouping,
    # unmatched_excel and unmatched_sf will only be equal if BOTH:
    #   (a) the grouped totals are equal: |sugarxl_grp| == |internals_grp|
    #   (b) every match is one-to-one (no row on either side absorbs multiple)
    sugarxl_grp_count = int(len(sugarxl_grp))
    internals_grp_count = int(len(internals_grp))
    matched_rows = int(len(match.matched))
    matched_unique_excel = sugarxl_grp_count - int(len(match.unmatched_excel))
    matched_unique_sf = internals_grp_count - int(len(match.unmatched_sf))

    return {
        "filename": getattr(file_storage, "filename", None),
        "start_date": pd.Timestamp(start_date).date(),
        "end_date": pd.Timestamp(end_date).date(),
        "excel_row_count": sugarxl_grp_count,
        "matched_count": matched_rows,
        "unmatched_excel": match.unmatched_excel.reset_index(drop=True),
        "unmatched_sf": match.unmatched_sf.reset_index(drop=True),
        "unmatched_excel_by_book": by_book_xl,
        "unmatched_sf_by_book": by_book_sf,
        "batches": batches,
        "summary": {
            "matched": matched_rows,
            "unmatched_excel": int(len(match.unmatched_excel)),
            "unmatched_sf": int(len(match.unmatched_sf)),
            "sugarxl_grp_count": sugarxl_grp_count,
            "internals_grp_count": internals_grp_count,
            "matched_unique_excel": matched_unique_excel,
            "matched_unique_sf": matched_unique_sf,
            "path1_rows": int(len(batches.sf_update_1)),
            "path2_rows": int(len(batches.sf_update_2)),
            "path3_rows": int(len(batches.final_df)),
            "path3_creates": int(batches.final_df["Id"].isna().sum()) if not batches.final_df.empty else 0,
            "missing_option_creates": int(len(batches.missing_option_creates)),
        },
    }


def _by_book_summary(df: pd.DataFrame, *, book_col: str, long_col: str, short_col: str) -> list:
    """Group an unmatched dataframe by its Book column and return per-book totals."""
    if df is None or df.empty or book_col not in df.columns:
        return []
    agg = (
        df.groupby(book_col, dropna=False)
        .agg(rows=(book_col, "size"),
             net_long=(long_col, "sum"),
             net_short=(short_col, "sum"))
        .reset_index()
    )
    agg["net_qty"] = agg["net_long"] + agg["net_short"]
    out = []
    for _, r in agg.iterrows():
        out.append({
            "book": r[book_col] if pd.notna(r[book_col]) and r[book_col] != "" else "(blank)",
            "rows": int(r["rows"]),
            "net_long": float(r["net_long"]),
            "net_short": float(r["net_short"]),
            "net_qty": float(r["net_qty"]),
        })
    return out

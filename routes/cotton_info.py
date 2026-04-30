"""Cotton (CT) info page — reference tables for futures, options, and NYSE holidays.

Formulas (ICE Cotton #2 product specifications):

  Futures Last Trading Day:
    "Seventeen business days from end of spot month."
    Implementation: workday(last_biz_of_month, LTD_OFFSET, holidays)
    LTD_OFFSET = -16 (last_biz counted as biz day 1, so -16 = 17th biz day back).
    Verified: CTH26 LTD = Mon 09 Mar 2026, CTN25 LTD = Wed 09 Jul 2025,
              CTV26 LTD = Thu 08 Oct 2026.

  Options expiry — LISTED months (H, K, N, V, Z):
    Listed-month option expires on the last Friday preceding the underlying
    future's first notice day by at least 5 business days.
    Implementation:
      1. Compute futures first notice day = 5 business days before the first
         business day of the delivery month.
      2. Step back 5 business days from first notice day.
      3. Take the last Friday on or before that date.
    Examples verified against ICE expiry pages:
      Jul26 option LTD = 12 Jun 2026, Oct26 option LTD = 11 Sep 2026.

  Options expiry — SERIAL months (F, U, X only):
    Serial option expires on the THIRD FRIDAY of the option's own month.
    Per ICE Cotton No. 2 Options spec. Only 3 serials are listed:
      F (Jan) → rolls into H (Mar) same year
      U (Sep) → rolls into Z (Dec) same year
      X (Nov) → rolls into Z (Dec) same year
    G, J, M, Q are NOT listed CT option contracts.
"""
import logging
import io
import posixpath
import re
import zipfile
import xml.etree.ElementTree as ET
from datetime import datetime, date, timedelta
from collections import defaultdict
from functools import lru_cache
from pathlib import Path
from flask import Blueprint, jsonify, render_template, request

from models.cotton import CottonIndexRow
from models.db import db

from services.exchange_calendar import (
    FUTURES_MONTH_CODES,
    YEARS_BACK,
    YEARS_FORWARD,
    RAW_HOLIDAYS,
    HOLIDAY_DATES,
    workday,
    last_biz_of_month,
    third_friday,
)

cotton_info_bp = Blueprint("cotton_info", __name__)

EXCEL_PATH = Path(__file__).parent.parent / "cottonm2m.xlsm"
MISC_INFO_TABLES = (
    "Info_Origins",
    "Info_Colours",
    "Info_WAF_Colours",
    "Info_Financing",
)
MISC_INFO_HEADERS = {
    "Info_Origins": ["Origin", "Region"],
    "Info_Colours": ["Colour", "Grade"],
    "Info_WAF_Colours": ["WAF Colour", "Grade"],
    "Info_Financing": ["Origin", "Interest", "Default Financing", "Misc"],
}
_XML_NS = {
    "m": "http://schemas.openxmlformats.org/spreadsheetml/2006/main",
    "r": "http://schemas.openxmlformats.org/officeDocument/2006/relationships",
    "rel": "http://schemas.openxmlformats.org/package/2006/relationships",
}


# ICE Cotton #2 (CT) listed contract months: March, May, July, October, December.
CT_FUTURES_MONTHS = ["H", "K", "N", "V", "Z"]

# Listed-month options expire on the last Friday preceding the underlying
# future's first notice day by at least 5 business days.
# Serial options (F, U, X) expire on the 3rd Friday of the option month.
# ICE spec: F→H same year, U→Z same year, X→Z same year.
CT_OPTION_TO_UNDERLYING = {m: (m, 0) for m in CT_FUTURES_MONTHS}
CT_OPTION_TO_UNDERLYING.update({
    "F": ("H", 0),
    "U": ("Z", 0),
    "X": ("Z", 0),
})

CT_SERIAL_OPTION_MONTHS = frozenset({"F", "U", "X"})

# Guardrail: the full set of CT option month codes per ICE spec. G, J, M, Q are
# explicitly forbidden — they are NOT listed as CT option contracts.
CT_EXPECTED_OPTION_CODES = frozenset({"H", "K", "N", "V", "Z", "F", "U", "X"})
CT_FORBIDDEN_OPTION_CODES = frozenset({"G", "J", "M", "Q"})

LTD_OFFSET = -16   # last_biz counted as day 1, so -16 means 17th biz day from end


def _first_biz_of_month(year, month, holidays):
    """Return the first business day of (year, month)."""
    d = date(year, month, 1)
    while d.weekday() >= 5 or d in holidays:
        d += timedelta(days=1)
    return d


def _last_friday_on_or_before(d):
    """Return the most recent Friday on or before *d*."""
    return d - timedelta(days=(d.weekday() - 4) % 7)


def _ct_regular_option_expiry(underlying, holidays):
    """ICE CT regular option LTD.

    Rule from ICE Cotton No. 2 Options spec:
    "Last Friday preceding the first notice day for the underlying futures by
    at least 5 business days."
    """
    parts = underlying.split()
    code = parts[1][0]
    year = 2000 + int(parts[1][1:])
    month = FUTURES_MONTH_CODES[code]
    first_notice_day = workday(_first_biz_of_month(year, month, holidays), -5, holidays)
    threshold = workday(first_notice_day, -5, holidays)
    return _last_friday_on_or_before(threshold)


def _generate_ct_futures(years_back=YEARS_BACK, years_forward=YEARS_FORWARD):
    current_year = date.today().year
    contracts = []
    for yr in range(current_year - years_back, current_year + years_forward + 1):
        yy = yr % 100
        for m in CT_FUTURES_MONTHS:
            contracts.append(f"CT {m}{yy:02d}")
    return contracts


def _generate_ct_options(years_back=YEARS_BACK, years_forward=YEARS_FORWARD):
    current_year = date.today().year
    out = []
    for yr in range(current_year - years_back, current_year + years_forward + 1):
        yy = yr % 100
        for opt_code in sorted(CT_OPTION_TO_UNDERLYING.keys(),
                               key=lambda c: FUTURES_MONTH_CODES[c]):
            und_code, year_offset = CT_OPTION_TO_UNDERLYING[opt_code]
            und_yy = (yr + year_offset) % 100
            out.append((f"CT {opt_code}{yy:02d}", f"CT {und_code}{und_yy:02d}"))
    return out


def _column_to_number(col):
    out = 0
    for ch in col:
        out = out * 26 + ord(ch.upper()) - 64
    return out


def _number_to_column(num):
    out = ""
    while num:
        num, rem = divmod(num - 1, 26)
        out = chr(65 + rem) + out
    return out


def _split_cell_ref(cell_ref):
    match = re.match(r"^([A-Z]+)(\d+)$", cell_ref)
    if not match:
        raise ValueError(f"Invalid cell reference: {cell_ref!r}")
    return int(match.group(2)), _column_to_number(match.group(1))


def _shared_strings(zip_file):
    if "xl/sharedStrings.xml" not in zip_file.namelist():
        return []
    root = ET.fromstring(zip_file.read("xl/sharedStrings.xml"))
    return [
        "".join(t.text or "" for t in si.findall(".//m:t", _XML_NS))
        for si in root.findall("m:si", _XML_NS)
    ]


def _sheet_path(zip_file, sheet_name):
    workbook = ET.fromstring(zip_file.read("xl/workbook.xml"))
    rels = ET.fromstring(zip_file.read("xl/_rels/workbook.xml.rels"))
    rel_map = {r.attrib["Id"]: r.attrib["Target"] for r in rels}
    for sheet in workbook.find("m:sheets", _XML_NS):
        if sheet.attrib.get("name") == sheet_name:
            rid = sheet.attrib["{http://schemas.openxmlformats.org/officeDocument/2006/relationships}id"]
            return "xl/" + rel_map[rid].lstrip("/")
    raise ValueError(f"Workbook is missing sheet {sheet_name!r}")


def _sheet_table_paths(zip_file, sheet_path):
    rel_path = (
        sheet_path.rsplit("/", 1)[0] + "/_rels/" +
        sheet_path.rsplit("/", 1)[1] + ".rels"
    )
    if rel_path not in zip_file.namelist():
        return []
    rels = ET.fromstring(zip_file.read(rel_path))
    base = sheet_path.rsplit("/", 1)[0]
    return [
        posixpath.normpath(posixpath.join(base, rel.attrib["Target"]))
        for rel in rels
        if rel.attrib.get("Type", "").endswith("/table")
    ]


def _cell_value(cell, shared_strings):
    value = cell.find("m:v", _XML_NS)
    cell_type = cell.attrib.get("t")
    if cell_type == "inlineStr":
        return "".join(t.text or "" for t in cell.findall(".//m:t", _XML_NS))
    if value is None:
        return ""
    raw = value.text or ""
    if cell_type == "s":
        return shared_strings[int(raw)]
    return raw


def _format_info_value(table_name, header, value):
    if value in (None, ""):
        return ""
    text = str(value)
    try:
        numeric = float(text)
    except ValueError:
        return text
    if table_name == "Info_Financing" and header == "Interest":
        return f"{numeric:.2%}"
    if numeric.is_integer():
        return str(int(numeric))
    return f"{numeric:g}"


@lru_cache(maxsize=4)
def _load_misc_info_tables_cached(path_str, mtime_ns):
    with zipfile.ZipFile(path_str) as zip_file:
        return _parse_misc_info_tables_zip(zip_file)


def _parse_misc_info_tables_zip(zip_file):
    tables = []
    shared = _shared_strings(zip_file)
    sheet_path = _sheet_path(zip_file, "Info")
    sheet_root = ET.fromstring(zip_file.read(sheet_path))
    cells = {
        cell.attrib["r"]: _cell_value(cell, shared)
        for cell in sheet_root.findall(".//m:c", _XML_NS)
    }

    table_meta = {}
    for table_path in _sheet_table_paths(zip_file, sheet_path):
        table_root = ET.fromstring(zip_file.read(table_path))
        name = table_root.attrib.get("name") or table_root.attrib.get("displayName")
        if name in MISC_INFO_TABLES:
            table_meta[name] = {
                "range": table_root.attrib["ref"],
                "headers": [
                    col.attrib.get("name", "")
                    for col in table_root.find("m:tableColumns", _XML_NS)
                ],
            }

    missing = [table_name for table_name in MISC_INFO_TABLES if table_name not in table_meta]
    if missing:
        raise ValueError(f"Info sheet is missing required tables: {', '.join(missing)}")

    for table_name in MISC_INFO_TABLES:
        meta = table_meta[table_name]
        start_ref, end_ref = meta["range"].split(":")
        start_row, start_col = _split_cell_ref(start_ref)
        end_row, end_col = _split_cell_ref(end_ref)
        headers = meta["headers"]
        rows = []
        for row_idx in range(start_row + 1, end_row + 1):
            row = []
            for col_idx, header in zip(range(start_col, end_col + 1), headers):
                cell_ref = f"{_number_to_column(col_idx)}{row_idx}"
                row.append(_format_info_value(table_name, header, cells.get(cell_ref, "")))
            if any(value != "" for value in row):
                rows.append(row)
        tables.append({
            "name": table_name,
            "range": meta["range"],
            "headers": headers,
            "rows": rows,
        })
    return tables


def _load_uploaded_misc_info_tables(file_storage):
    return _parse_misc_info_tables_zip(zipfile.ZipFile(io.BytesIO(file_storage.read())))


def _replace_misc_info_rows(tables, source):
    CottonIndexRow.query.filter(
        CottonIndexRow.table_name.in_(MISC_INFO_TABLES)
    ).delete(synchronize_session=False)
    for table in tables:
        headers = table["headers"]
        for row_index, row in enumerate(table["rows"]):
            db.session.add(CottonIndexRow(
                table_name=table["name"],
                row_index=row_index,
                data=dict(zip(headers, row)),
                source=source,
            ))
    db.session.commit()


def _load_uploaded_misc_info_tables_from_db():
    tables = []
    for table_name in MISC_INFO_TABLES:
        rows = (CottonIndexRow.query
                .filter_by(table_name=table_name)
                .order_by(CottonIndexRow.row_index)
                .all())
        if not rows:
            return []
        headers = MISC_INFO_HEADERS[table_name]
        tables.append({
            "name": table_name,
            "range": None,
            "headers": headers,
            "rows": [[row.data.get(header, "") for header in headers] for row in rows],
        })
    return tables


def load_misc_info_tables():
    uploaded = _load_uploaded_misc_info_tables_from_db()
    if uploaded:
        return uploaded
    if not EXCEL_PATH.exists():
        _log.warning("Cotton info workbook not found: %s", EXCEL_PATH)
        return []
    return _load_misc_info_tables_cached(str(EXCEL_PATH), EXCEL_PATH.stat().st_mtime_ns)


_RAW_CT_FUTURES = _generate_ct_futures()
_RAW_CT_OPTIONS = _generate_ct_options()


def _parse_ct_futures(contracts):
    """CT futures Last Trading Day = workday(last_biz_of_month, LTD_OFFSET)."""
    result = []
    for c in contracts:
        parts = c.split()
        code = parts[1][0]
        year = 2000 + int(parts[1][1:])
        month = FUTURES_MONTH_CODES.get(code)
        ref_date = date(year, month, 1) if month else None
        if month:
            last_biz = last_biz_of_month(year, month, HOLIDAY_DATES)
            expiry = workday(last_biz, LTD_OFFSET, HOLIDAY_DATES)
        else:
            expiry = None
        result.append({"contract": c, "ref_date": ref_date, "expiry": expiry})
    return result


def _parse_ct_options(options, futures_expiry_map):
    """Listed-month options: last Friday preceding FND by at least 5 business days.
       Serial options (F, U, X): expiry = 3rd Friday of option's own month."""
    result = []
    for contract, underlying in options:
        opt_parts = contract.split()
        opt_code = opt_parts[1][0]
        opt_year = 2000 + int(opt_parts[1][1:])
        opt_month = FUTURES_MONTH_CODES.get(opt_code)
        if opt_month is None:
            raise ValueError(f"Unknown CT option month code: {opt_code!r} in {contract!r}")
        ref_date = date(opt_year, opt_month, 1)
        if opt_code in CT_SERIAL_OPTION_MONTHS:
            expiry = third_friday(opt_year, opt_month)
        else:
            expiry = _ct_regular_option_expiry(underlying, HOLIDAY_DATES)
        result.append({"contract": contract, "underlying": underlying,
                       "ref_date": ref_date, "expiry": expiry})
    return result


# Module-level cached parses (mirrors sugar pattern).
PARSED_CT_FUTURES = _parse_ct_futures(_RAW_CT_FUTURES)

# Exported for cotton_prices.py auto-archive wiring (parallels sugar's
# FUTURES_EXPIRY_MAP in routes/prices.py:26).
CT_FUTURES_EXPIRY_MAP = {
    f["contract"].replace(" ", ""): f["expiry"] for f in PARSED_CT_FUTURES
}

# Options computed after the futures map is available.
PARSED_CT_OPTIONS = _parse_ct_options(_RAW_CT_OPTIONS, CT_FUTURES_EXPIRY_MAP)


def compute_ct_futures_expiry(contract):
    """LTD for a single CT futures contract code (e.g. 'CTH24' or 'CT H24'),
    regardless of the rolling-window cache. Returns None for malformed input."""
    c = (contract or "").replace(" ", "")
    if len(c) != 5 or not c.startswith("CT"):
        return None
    code = c[2]
    month = FUTURES_MONTH_CODES.get(code)
    if month is None:
        return None
    try:
        yy = int(c[3:5])
    except ValueError:
        return None
    year = 2000 + yy
    last_biz = last_biz_of_month(year, month, HOLIDAY_DATES)
    return workday(last_biz, LTD_OFFSET, HOLIDAY_DATES)


def compute_ct_option_expiry(option_contract):
    """LTD for a single CT option contract code (e.g. 'CTK24' or 'CT K24'),
    regardless of the rolling-window cache. Returns None for malformed input or
    forbidden option codes (G/J/M/Q)."""
    c = (option_contract or "").replace(" ", "")
    if len(c) != 5 or not c.startswith("CT"):
        return None
    opt_code = c[2]
    if opt_code not in CT_OPTION_TO_UNDERLYING:
        return None
    try:
        yy = int(c[3:5])
    except ValueError:
        return None
    year = 2000 + yy
    if opt_code in CT_SERIAL_OPTION_MONTHS:
        month = FUTURES_MONTH_CODES[opt_code]
        return third_friday(year, month)
    und_code, year_offset = CT_OPTION_TO_UNDERLYING[opt_code]
    und_yy = (yy + year_offset) % 100
    underlying = f"CT {und_code}{und_yy:02d}"
    return _ct_regular_option_expiry(underlying, HOLIDAY_DATES)


def _assert_regression():
    """Loud-fail at module import if any pinned cotton expiry has drifted.
    See routes/_cotton_info_regression.py for the anchors."""
    from routes._cotton_info_regression import (
        GOLDEN_CT_FUTURES, GOLDEN_CT_OPTIONS,
    )

    fut_by_code = {f["contract"]: f for f in PARSED_CT_FUTURES}
    for code, expected_ref, expected_expiry in GOLDEN_CT_FUTURES:
        actual = fut_by_code.get(code)
        assert actual is not None, f"Regression: cotton futures {code} missing"
        assert actual["ref_date"] == expected_ref, (
            f"Regression: {code} ref_date drifted: "
            f"got {actual['ref_date']}, expected {expected_ref}"
        )
        assert actual["expiry"] == expected_expiry, (
            f"Regression: {code} expiry drifted: "
            f"got {actual['expiry']}, expected {expected_expiry}"
        )

    opt_by_code = {o["contract"]: o for o in PARSED_CT_OPTIONS}
    for code, exp_und, expected_ref, expected_expiry in GOLDEN_CT_OPTIONS:
        actual = opt_by_code.get(code)
        assert actual is not None, f"Regression: cotton option {code} missing"
        assert actual["underlying"] == exp_und, (
            f"Regression: {code} underlying drifted: "
            f"got {actual['underlying']}, expected {exp_und}"
        )
        assert actual["ref_date"] == expected_ref, (
            f"Regression: {code} option ref_date drifted: "
            f"got {actual['ref_date']}, expected {expected_ref}"
        )
        assert actual["expiry"] == expected_expiry, (
            f"Regression: {code} option expiry drifted: "
            f"got {actual['expiry']}, expected {expected_expiry}"
        )

    # Negative guard: CT option codes must exactly match ICE spec.
    # G, J, M, Q are NOT listed CT option contracts — protects against
    # re-introducing the old (incorrect) 7-serial list.
    actual_codes = {o["contract"].split()[1][0] for o in PARSED_CT_OPTIONS}
    assert actual_codes == CT_EXPECTED_OPTION_CODES, (
        f"Regression: CT option codes drifted from ICE spec. "
        f"Got {sorted(actual_codes)}, expected {sorted(CT_EXPECTED_OPTION_CODES)}. "
        f"Unexpected serials (G/J/M/Q are NOT listed CT options per ICE spec): "
        f"{sorted(actual_codes & CT_FORBIDDEN_OPTION_CODES)}"
    )

_assert_regression()


_log = logging.getLogger(__name__)


@cotton_info_bp.route("/info")
def index():
    today = date.today()

    holidays_list = sorted([
        {
            "name": name,
            "date": datetime.strptime(d, "%Y-%m-%d").date(),
            "day": datetime.strptime(d, "%Y-%m-%d").strftime("%A"),
        }
        for name, d in RAW_HOLIDAYS
    ], key=lambda x: x["date"])

    upcoming_date = next(
        (h["date"] for h in holidays_list if h["date"] >= today), None
    )

    grouped = defaultdict(list)
    for h in holidays_list:
        grouped[h["date"].year].append(h)

    return render_template("cotton/info.html",
                           grouped=dict(sorted(grouped.items())),
                           upcoming_date=upcoming_date,
                           futures=PARSED_CT_FUTURES,
                           options=PARSED_CT_OPTIONS,
                           misc_info_tables=load_misc_info_tables())


@cotton_info_bp.route("/info/api/upload", methods=["POST"])
def api_upload():
    """Upload an .xlsm / .xlsx and replace miscellaneous Info tables."""
    f = request.files.get("file")
    if not f:
        return jsonify({"error": "No file uploaded"}), 400
    try:
        tables = _load_uploaded_misc_info_tables(f)
        _replace_misc_info_rows(tables, source="upload")
    except ValueError as e:
        return jsonify({"error": str(e)}), 422
    except Exception as e:
        db.session.rollback()
        _log.exception("Cotton info upload failed")
        return jsonify({"error": str(e)}), 500

    counts = {table["name"]: len(table["rows"]) for table in tables}
    return jsonify({
        "ok": True,
        "origins": counts.get("Info_Origins", 0),
        "colours": counts.get("Info_Colours", 0),
        "waf_colours": counts.get("Info_WAF_Colours", 0),
        "financing": counts.get("Info_Financing", 0),
    })

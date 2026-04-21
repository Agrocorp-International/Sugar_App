import logging
from sqlalchemy import text, bindparam
from models.db import db

log = logging.getLogger(__name__)

VAR_IDENTIFIERS = ("Pratik_Sugar",)

# public.daily_var."case" is stored as Postgres real.
# Match by nearest-value because real has ~7-digit precision drift.
_CASE_LABELS = ((95.0, "95%"), (99.0, "99%"), (100.0, "Worst Case"))

# Sum per (case, var_date) BEFORE ranking so each displayed row
# corresponds to a single date — the two identifiers may publish on
# different days and we don't want to mix dates in one sum.
_VAR_SQL = text("""
    WITH combined AS (
        SELECT "case", var_date, SUM(value_change) AS value_change
        FROM public.daily_var
        WHERE identifier IN :identifiers
        GROUP BY "case", var_date
    ),
    ranked AS (
        SELECT "case", value_change, var_date,
               ROW_NUMBER() OVER (PARTITION BY "case" ORDER BY var_date DESC) AS rn
        FROM combined
    )
    SELECT "case" AS case_val, value_change, var_date, rn
    FROM ranked
    WHERE rn <= 2
    ORDER BY "case", rn
""").bindparams(bindparam("identifiers", expanding=True))


def _label_for(case_val):
    for target, label in _CASE_LABELS:
        if abs(float(case_val) - target) < 0.005:
            return label
    return None


def compute_var_summary():
    empty = {
        "as_of": None,
        "prev_as_of": None,
        "cases": {lbl: {"yesterday": None, "prev": None} for _, lbl in _CASE_LABELS},
    }
    try:
        rows = db.session.execute(
            _VAR_SQL, {"identifiers": list(VAR_IDENTIFIERS)}
        ).fetchall()
    except Exception:
        log.exception("compute_var_summary failed")
        db.session.rollback()
        return empty

    by_label = {lbl: {"yesterday": None, "prev": None} for _, lbl in _CASE_LABELS}
    as_of = None
    prev_as_of = None
    for r in rows:
        lbl = _label_for(r.case_val)
        if lbl is None:
            continue
        slot = "yesterday" if r.rn == 1 else "prev"
        by_label[lbl][slot] = float(r.value_change)
        if slot == "yesterday" and (as_of is None or r.var_date > as_of):
            as_of = r.var_date
        if slot == "prev" and (prev_as_of is None or r.var_date > prev_as_of):
            prev_as_of = r.var_date

    return {"as_of": as_of, "prev_as_of": prev_as_of, "cases": by_label}

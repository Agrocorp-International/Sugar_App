import os
import tempfile
from datetime import date, datetime
from pathlib import Path

from flask import Blueprint, render_template, request, redirect, url_for, flash, session
from models.db import db, AutoTagRun

admin_bp = Blueprint("admin", __name__)

_UPLOAD_DIR = Path(tempfile.gettempdir()) / "sugar_admin_upload"

# ── Default date filters (update these when the year rolls over) ─────────────
AUTO_TAG_DEFAULT_START = "2025-12-31"
SPEC_CHECK_DEFAULT_START = "2025-03-31"
IT_CHECK_DEFAULT_START = "2025-03-30"


def _get_xlsx_path():
    """Save an uploaded Excel file to a temp dir and store its path in the session.
    If no new file is uploaded, return the previously cached path (if any).
    Returns (path_str, filename) or (None, None)."""
    f = request.files.get("xlsx")
    if f and f.filename:
        _UPLOAD_DIR.mkdir(parents=True, exist_ok=True)
        # Remove old cached file
        old_path = session.pop("uploaded_xlsx_path", None)
        if old_path and os.path.exists(old_path):
            try:
                os.unlink(old_path)
            except OSError:
                pass
        save_path = _UPLOAD_DIR / f.filename
        f.save(str(save_path))
        session["uploaded_xlsx_path"] = str(save_path)
        session["uploaded_xlsx_name"] = f.filename
        return str(save_path), f.filename

    # No new upload — try cached file
    cached = session.get("uploaded_xlsx_path")
    if cached and os.path.exists(cached):
        return cached, session.get("uploaded_xlsx_name")
    return None, None


def _df_to_records(df, limit=200):
    """Convert a (possibly None/empty) pandas DataFrame to a list of plain dicts
    suitable for Jinja rendering. Limited to `limit` rows."""
    if df is None or len(df) == 0:
        return []
    import pandas as _pd
    out = []
    for _, row in df.head(limit).iterrows():
        d = {}
        for k, v in row.items():
            try:
                if _pd.isna(v):
                    d[k] = ""
                    continue
                if isinstance(v, _pd.Timestamp):
                    d[k] = v.strftime("%Y-%m-%d")
                    continue
            except (TypeError, ValueError):
                pass
            d[k] = v
        out.append(d)
    return out


@admin_bp.route("/admin")
def index():
    # Auto-tag preview (only loaded when a token is staged in the session).
    auto_tag_preview = None
    token = session.get("auto_tag_token")
    if token:
        from services.auto_tag import load_staged
        staged = load_staged(token)
        if staged is None:
            session.pop("auto_tag_token", None)
        else:
            auto_tag_preview = {
                "token": token,
                "filename": staged.get("filename"),
                "start_date": staged.get("start_date"),
                "end_date": staged.get("end_date"),
                "summary": staged.get("summary", {}),
                "unmatched_excel": _df_to_records(staged.get("unmatched_excel")),
                "unmatched_sf": _df_to_records(staged.get("unmatched_sf")),
                "unmatched_excel_by_book": staged.get("unmatched_excel_by_book", []),
                "unmatched_sf_by_book": staged.get("unmatched_sf_by_book", []),
            }

    # Spec-check preview (only loaded when a token is staged in the session).
    spec_check_preview = None
    spec_token = session.get("spec_check_token")
    if spec_token:
        from services.auto_tag import load_staged as _load_staged
        spec_staged = _load_staged(spec_token)
        if spec_staged is None:
            session.pop("spec_check_token", None)
        else:
            spec_check_preview = dict(spec_staged)
            spec_check_preview["token"] = spec_token

    # Internal-transfer check preview.
    it_check_preview = None
    it_token = session.get("it_check_token")
    if it_token:
        from services.auto_tag import load_staged as _load_staged2
        it_staged = _load_staged2(it_token)
        if it_staged is None:
            session.pop("it_check_token", None)
        else:
            it_check_preview = dict(it_staged)
            it_check_preview["token"] = it_token

    return render_template(
        "admin.html",
        auto_tag_preview=auto_tag_preview,
        auto_tag_default_start=AUTO_TAG_DEFAULT_START,
        auto_tag_default_end=date.today().isoformat(),
        it_check_default_start=IT_CHECK_DEFAULT_START,
        spec_check_default_start=SPEC_CHECK_DEFAULT_START,
        spec_check_preview=spec_check_preview,
        it_check_preview=it_check_preview,
        cached_xlsx_name=session.get("uploaded_xlsx_name"),
    )


# ── Auto Tag (port of 9_sugar_auto_tagging.ipynb) ────────────────────────────

@admin_bp.route("/admin/auto-tag/preview", methods=["POST"])
def auto_tag_preview():
    """Step 1: read uploaded Excel + match against Salesforce, stage the result."""
    from services.auto_tag import build_preview, stage_to_tempfile, discard_staged
    from services.salesforce import get_sf_connection

    xlsx_path, xlsx_name = _get_xlsx_path()
    if not xlsx_path:
        flash("Please choose a sugarm2m .xlsm file to preview.", "warning")
        return redirect(url_for("admin.index"))

    start_str = (request.form.get("start_date") or AUTO_TAG_DEFAULT_START).strip()
    end_str = (request.form.get("end_date") or date.today().isoformat()).strip()
    try:
        start_d = datetime.strptime(start_str, "%Y-%m-%d").date()
        end_d = datetime.strptime(end_str, "%Y-%m-%d").date()
    except ValueError:
        flash("Invalid date format — please use YYYY-MM-DD.", "danger")
        return redirect(url_for("admin.index"))
    if end_d < start_d:
        flash("End date must be on or after start date.", "danger")
        return redirect(url_for("admin.index"))

    # Discard any prior staged preview before staging a new one
    old = session.pop("auto_tag_token", None)
    if old:
        discard_staged(old)

    try:
        sf = get_sf_connection()
        payload = build_preview(sf, xlsx_path, start_d, end_d)
        payload["filename"] = xlsx_name
    except Exception as e:  # noqa: BLE001
        flash(f"Auto-tag preview failed: {e}", "danger")
        return redirect(url_for("admin.index"))

    token = stage_to_tempfile(payload)
    session["auto_tag_token"] = token
    s = payload["summary"]
    flash(
        f"Auto-tag preview ready — {s['matched']} matched, {s['unmatched_excel']} unmatched (Excel), "
        f"{s['unmatched_sf']} unmatched (SF). Path1={s['path1_rows']}, Path2={s['path2_rows']}, "
        f"Path3={s['path3_rows']} (creates: {s['path3_creates']}). Review below before pushing.",
        "info",
    )
    return redirect(url_for("admin.index"))


@admin_bp.route("/admin/auto-tag/confirm", methods=["POST"])
def auto_tag_confirm():
    """Step 2: actually push the staged batches to Salesforce."""
    from services.auto_tag import load_staged, discard_staged, execute_full_push
    from services.salesforce import get_sf_connection

    token = session.get("auto_tag_token")
    staged = load_staged(token) if token else None
    if not staged:
        flash("No staged auto-tag preview found — please re-upload the file.", "warning")
        return redirect(url_for("admin.index"))

    try:
        sf = get_sf_connection()
        reports = execute_full_push(sf, staged["batches"])
    except Exception as e:  # noqa: BLE001
        flash(f"Auto-tag push failed: {e}", "danger")
        return redirect(url_for("admin.index"))

    created = sum(len(r.created) for r in reports.values())
    updated = sum(len(r.updated) for r in reports.values())
    errors_list = []
    for r in reports.values():
        for trade_id, msg in r.errors:
            errors_list.append({"batch": r.batch_name, "trade_id": trade_id, "error": msg})
    skipped_list = []
    for r in reports.values():
        for trade_id, reason in r.skipped:
            skipped_list.append({"batch": r.batch_name, "trade_id": trade_id, "reason": reason})
    err_count = len(errors_list)

    if err_count == 0:
        status = "success"
    elif created + updated > 0:
        status = "partial"
    else:
        status = "error"

    try:
        run = AutoTagRun(
            filename=staged.get("filename"),
            start_date=staged.get("start_date"),
            end_date=staged.get("end_date"),
            excel_row_count=staged.get("excel_row_count") or 0,
            matched_count=staged.get("matched_count") or 0,
            unmatched_excel_count=staged.get("summary", {}).get("unmatched_excel", 0),
            unmatched_sf_count=staged.get("summary", {}).get("unmatched_sf", 0),
            sf_created=created,
            sf_updated=updated,
            sf_errors=err_count,
            status=status,
            error_sample={"errors": errors_list[:30], "skipped": skipped_list[:30]},
        )
        db.session.add(run)
        db.session.commit()
    except Exception as e:  # noqa: BLE001
        db.session.rollback()
        flash(f"Auto-tag completed but failed to log run: {e}", "warning")

    discard_staged(token)
    session.pop("auto_tag_token", None)

    if status == "success":
        flash(f"Auto-tag pushed: {created} created, {updated} updated. No errors.", "success")
    elif status == "partial":
        flash(
            f"Auto-tag pushed with errors: {created} created, {updated} updated, "
            f"{err_count} errors. First error: {errors_list[0]['error']}",
            "warning",
        )
    else:
        flash(
            f"Auto-tag failed — {err_count} errors, no records written. "
            f"First error: {errors_list[0]['error'] if errors_list else 'unknown'}",
            "danger",
        )
    return redirect(url_for("admin.index"))


@admin_bp.route("/admin/auto-tag/cancel", methods=["POST"])
def auto_tag_cancel():
    from services.auto_tag import discard_staged
    token = session.pop("auto_tag_token", None)
    if token:
        discard_staged(token)
    flash("Auto-tag preview discarded.", "info")
    return redirect(url_for("admin.index"))


# ── Spec position check (port of 1_sugar_overall_spec.ipynb) ─────────────────

@admin_bp.route("/admin/spec-check/preview", methods=["POST"])
def spec_check_preview():
    """Compare uploaded sugarm2m Summary sheet against live Salesforce Spec/Open trades."""
    from services.spec_check import build_spec_preview
    from services.auto_tag import stage_to_tempfile, discard_staged
    from services.salesforce import get_sf_connection

    xlsx_path, xlsx_name = _get_xlsx_path()
    if not xlsx_path:
        flash("Please choose a sugarm2m .xlsm file for the spec check.", "warning")
        return redirect(url_for("admin.index"))

    start_str = (request.form.get("start_date") or SPEC_CHECK_DEFAULT_START).strip()
    end_str = (request.form.get("end_date") or date.today().isoformat()).strip()
    try:
        start_d = datetime.strptime(start_str, "%Y-%m-%d").date()
        end_d = datetime.strptime(end_str, "%Y-%m-%d").date()
    except ValueError:
        flash("Invalid date format — please use YYYY-MM-DD.", "danger")
        return redirect(url_for("admin.index"))
    if end_d < start_d:
        flash("End date must be on or after start date.", "danger")
        return redirect(url_for("admin.index"))

    old = session.pop("spec_check_token", None)
    if old:
        discard_staged(old)

    try:
        sf = get_sf_connection()
        payload = build_spec_preview(sf, xlsx_path, start_d, end_d)
        payload["filename"] = xlsx_name
    except Exception as e:  # noqa: BLE001
        flash(f"Spec check failed: {e}", "danger")
        return redirect(url_for("admin.index"))

    token = stage_to_tempfile(payload)
    session["spec_check_token"] = token

    if payload["all_match"]:
        flash(
            f"Spec check — no discrepancies — {payload['matched']} contracts compared.",
            "success",
        )
    else:
        flash(
            f"Spec check found {len(payload['discrepancies'])} discrepancy(ies) — "
            f"{payload['in_excel_only']} Excel-only, {payload['in_sf_only']} SF-only.",
            "warning",
        )
    return redirect(url_for("admin.index"))


@admin_bp.route("/admin/spec-check/cancel", methods=["POST"])
def spec_check_cancel():
    from services.auto_tag import discard_staged
    token = session.pop("spec_check_token", None)
    if token:
        discard_staged(token)
    flash("Spec check result discarded.", "info")
    return redirect(url_for("admin.index"))


# ── Internal Transfer Check (port of internal_transfer.ipynb) ─────────────────

@admin_bp.route("/admin/it-check/preview", methods=["POST"])
def it_check_preview():
    """Check that all internal-transfer trades net to zero."""
    from services.internal_transfer_check import build_it_check_preview
    from services.auto_tag import stage_to_tempfile, discard_staged
    from services.salesforce import get_sf_connection

    start_str = (request.form.get("start_date") or IT_CHECK_DEFAULT_START).strip()
    end_str = (request.form.get("end_date") or date.today().isoformat()).strip()
    try:
        start_d = datetime.strptime(start_str, "%Y-%m-%d").date()
        end_d = datetime.strptime(end_str, "%Y-%m-%d").date()
    except ValueError:
        flash("Invalid date format — please use YYYY-MM-DD.", "danger")
        return redirect(url_for("admin.index"))
    if end_d < start_d:
        flash("End date must be on or after start date.", "danger")
        return redirect(url_for("admin.index"))

    old = session.pop("it_check_token", None)
    if old:
        discard_staged(old)

    try:
        sf = get_sf_connection()
        payload = build_it_check_preview(sf, start_d, end_d)
    except Exception as e:  # noqa: BLE001
        flash(f"Internal transfer check failed: {e}", "danger")
        return redirect(url_for("admin.index"))

    token = stage_to_tempfile(payload)
    session["it_check_token"] = token

    if payload["all_balanced"]:
        flash("Internal transfer check — all trades net to zero.", "success")
    else:
        flash(
            f"Internal transfer check found {len(payload['rows'])} imbalance(s).",
            "warning",
        )
    return redirect(url_for("admin.index"))


@admin_bp.route("/admin/it-check/cancel", methods=["POST"])
def it_check_cancel():
    from services.auto_tag import discard_staged
    token = session.pop("it_check_token", None)
    if token:
        discard_staged(token)
    flash("Internal transfer check result discarded.", "info")
    return redirect(url_for("admin.index"))

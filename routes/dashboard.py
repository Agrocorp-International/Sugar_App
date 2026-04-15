import os
from datetime import datetime, date
from flask import Blueprint, render_template, redirect, url_for, flash, current_app, request, make_response, jsonify, abort
from sqlalchemy import cast, Date
from models.db import db, TradePosition, SyncLog, MarketPrice, PnlSnapshot, PnlSnapshotSchedule
from routes.info import (_parse_futures, _parse_options, _parse_sw_futures,
                         _RAW_FUTURES, _RAW_OPTIONS, _RAW_SW_FUTURES, _RAW_HOLIDAYS)
from services.pnl_summary import compute_pnl_summary, compute_exposure, get_reference_snapshots
from services.physical_pnl import compute_all_pnl_totals
from services.price_source import get_price_source
from services.snapshots import create_snapshot
from services.schedule import is_due

_DTE_WARN_DAYS = 10  # highlight DTE column when this close to expiry

dashboard_bp = Blueprint("dashboard", __name__)


@dashboard_bp.route("/")
def index():
    price_source = get_price_source()

    total_positions = TradePosition.query.count()
    last_sync = SyncLog.query.order_by(SyncLog.synced_at.desc()).first()
    _latest = TradePosition.query.order_by(
        cast(TradePosition.data["Trade_Date__c"].as_string(), Date).desc()
    ).first()
    latest_trade_date = _latest.data.get("Trade_Date__c") if _latest else None

    last_price_update = MarketPrice.query.order_by(MarketPrice.fetched_at.desc()).first()

    # Compute physical totals once; share with pnl_summary and exposure
    try:
        physical_totals = compute_all_pnl_totals(price_source)
    except Exception:
        physical_totals = None

    try:
        pnl_summary = compute_pnl_summary(price_source, physical_totals=physical_totals)
    except Exception:
        pnl_summary = None

    try:
        exposure = compute_exposure(price_source, physical_totals=physical_totals)
    except Exception:
        exposure = None

    def _delta(snap):
        if not snap or not snap.data:
            return None
        return {
            k: (pnl_summary[k] - snap.data[k])
               if pnl_summary.get(k) is not None and snap.data.get(k) is not None
               else None
            for k in pnl_summary
        }

    daily_s = weekly_s = monthly_s = None
    try:
        daily_s, weekly_s, monthly_s = get_reference_snapshots()
    except Exception:
        pass

    snap_slots = {'daily': daily_s, 'weekly': weekly_s, 'monthly': monthly_s}

    schedules = {s.slot: s for s in PnlSnapshotSchedule.query.all()}

    daily_delta = weekly_delta = monthly_delta = None
    if pnl_summary:
        try: daily_delta   = _delta(daily_s)
        except Exception: pass
        try: weekly_delta  = _delta(weekly_s)
        except Exception: pass
        try: monthly_delta = _delta(monthly_s)
        except Exception: pass

    pnl_changes = {"daily": daily_delta, "weekly": weekly_delta, "monthly": monthly_delta}

    # Upcoming expiries + next ICE holiday for the dashboard side panel.
    # DTE is measured from the latest trade date (falls back to today if missing).
    try:
        as_of = date.fromisoformat(latest_trade_date) if latest_trade_date else date.today()
    except (TypeError, ValueError):
        as_of = date.today()

    def _row(label, name, expiry):
        if expiry is None:
            return None
        dte = (expiry - as_of).days
        return {
            "label": label,
            "name": name,
            "expiry": expiry,
            "dte": dte,
            "warn": dte <= _DTE_WARN_DAYS,
        }

    next_future = next(
        (f for f in sorted(_parse_futures(_RAW_FUTURES), key=lambda f: f["expiry"] or date.max)
         if f["expiry"] and f["expiry"] >= as_of),
        None,
    )
    next_sw_future = next(
        (f for f in sorted(_parse_sw_futures(_RAW_SW_FUTURES), key=lambda f: f["expiry"] or date.max)
         if f["expiry"] and f["expiry"] >= as_of),
        None,
    )
    next_option = next(
        (o for o in sorted(_parse_options(_RAW_OPTIONS), key=lambda o: o["expiry"] or date.max)
         if o["expiry"] and o["expiry"] >= as_of),
        None,
    )
    next_holiday = None
    for name, d in _RAW_HOLIDAYS:
        parsed = datetime.strptime(d, "%Y-%m-%d").date()
        if parsed >= as_of:
            next_holiday = {"name": name, "date": parsed}
            break

    upcoming_rows = [
        _row("Next SB Futures Expiry",
             next_future["contract"] if next_future else None,
             next_future["expiry"] if next_future else None),
        _row("Next SW Futures Expiry",
             next_sw_future["contract"] if next_sw_future else None,
             next_sw_future["expiry"] if next_sw_future else None),
        _row("Next Options Expiry",
             next_option["contract"] if next_option else None,
             next_option["expiry"] if next_option else None),
        _row("Next ICE Holiday",
             next_holiday["name"] if next_holiday else None,
             next_holiday["date"] if next_holiday else None),
    ]
    upcoming_rows = [r for r in upcoming_rows if r is not None]

    return render_template(
        "dashboard.html",
        total_positions=total_positions,
        last_sync=last_sync,
        last_price_update=last_price_update,
        latest_trade_date=latest_trade_date,
        pnl_summary=pnl_summary,
        exposure=exposure,
        pnl_changes=pnl_changes,
        snap_slots=snap_slots,
        schedules=schedules,
        upcoming_rows=upcoming_rows,
        price_source=price_source,
    )


@dashboard_bp.route("/set-price-source", methods=["GET", "POST"])
def set_price_source():
    """Persist the chosen price source in a cookie and redirect back.

    Called by the navbar toggle in templates/base.html. The cookie is
    read by services/price_source.get_price_source() on every request,
    so toggling here flips the entire app to live or sett-1 in one click.

    Accepts both GET (link-based, what the navbar uses) and POST
    (form-based, kept for backwards compatibility) so the toggle works
    in any browser without relying on form submission semantics.
    """
    src = (request.values.get("source") or "sett1").strip()
    if src not in ("sett1", "live"):
        src = "sett1"
    next_url = request.values.get("next") or url_for("dashboard.index")
    # Defence against open-redirect: only allow same-origin relative paths
    if not next_url.startswith("/"):
        next_url = url_for("dashboard.index")
    resp = make_response(redirect(next_url))
    # 30 days, lax samesite is fine — this is a UI preference, not auth
    resp.set_cookie("price_source", src, max_age=60 * 60 * 24 * 30, samesite="Lax")
    return resp


@dashboard_bp.route("/snapshot/<slot>", methods=["POST"])
def save_snapshot(slot):
    if slot not in ('daily', 'weekly', 'monthly'):
        return redirect(url_for("dashboard.index"))
    try:
        create_snapshot(slot, source="manual")
        flash(f"{slot.capitalize()} snapshot saved.", "success")
    except Exception as e:
        db.session.rollback()
        current_app.logger.exception("Snapshot save failed")
        flash(f"Snapshot failed: {e}", "danger")
    return redirect(url_for("dashboard.index"))


@dashboard_bp.route("/snapshot/schedule/<slot>", methods=["POST"])
def save_snapshot_schedule(slot):
    """Upsert the auto-snapshot schedule for a slot. Normalizes irrelevant
    fields to null (e.g. weekday cleared when slot is not 'weekly') so stale
    values can't leak into later tick decisions."""
    if slot not in ('daily', 'weekly', 'monthly'):
        return redirect(url_for("dashboard.index"))
    try:
        enabled = request.form.get("enabled") == "on"
        time_str = (request.form.get("time") or "").strip()  # "HH:MM"
        try:
            hh, mm = [int(x) for x in time_str.split(":", 1)]
        except Exception:
            hh, mm = 18, 0
        if not (0 <= hh <= 23 and 0 <= mm <= 59):
            raise ValueError("time must be HH:MM in 00:00..23:59")

        weekday = None
        day_of_month = None
        if slot == "weekly":
            weekday = int(request.form.get("weekday") or 4)  # default Fri
            if not (0 <= weekday <= 6):
                raise ValueError("weekday must be 0..6")
        elif slot == "monthly":
            dom_raw = request.form.get("day_of_month") or "-1"
            day_of_month = int(dom_raw)
            if not (day_of_month == -1 or 1 <= day_of_month <= 28):
                raise ValueError("day_of_month must be 1..28 or -1")

        sched = db.session.get(PnlSnapshotSchedule, slot)
        if sched is None:
            sched = PnlSnapshotSchedule(slot=slot)
            db.session.add(sched)
        sched.enabled = enabled
        sched.hour = hh
        sched.minute = mm
        sched.weekday = weekday
        sched.day_of_month = day_of_month
        # Editing the schedule resets the idempotency guard so the new
        # occurrence will be picked up on the next tick.
        sched.last_scheduled_for = None
        db.session.commit()
        flash(f"{slot.capitalize()} schedule saved.", "success")
    except Exception as e:
        db.session.rollback()
        current_app.logger.exception("Schedule save failed")
        flash(f"Schedule save failed: {e}", "danger")
    return redirect(url_for("dashboard.index"))


@dashboard_bp.route("/snapshot/tick", methods=["POST"])
def snapshot_tick():
    """Cron entrypoint. Requires X-Cron-Key header matching SNAPSHOT_CRON_KEY
    env var. Iterates enabled schedules; for each one whose scheduled
    occurrence has passed and hasn't been fired yet, creates an 'auto'
    snapshot. Per-slot failures do not stop other slots."""
    expected = os.getenv("SNAPSHOT_CRON_KEY")
    if not expected or request.headers.get("X-Cron-Key") != expected:
        abort(403)

    fired, skipped, errors = [], [], []
    now_utc = datetime.utcnow()
    schedules = PnlSnapshotSchedule.query.all()
    for sched in schedules:
        try:
            due, occurrence = is_due(sched, now_utc)
            if not due:
                skipped.append({"slot": sched.slot, "enabled": sched.enabled,
                                "occurrence": occurrence.isoformat() if occurrence else None})
                continue
            create_snapshot(sched.slot, source="auto", scheduled_for=occurrence)
            sched.last_scheduled_for = occurrence
            sched.last_fired_at = now_utc
            db.session.commit()
            fired.append({"slot": sched.slot, "occurrence": occurrence.isoformat()})
        except Exception as e:
            db.session.rollback()
            current_app.logger.exception("Auto snapshot failed for %s", sched.slot)
            errors.append({"slot": sched.slot, "error": str(e)})
    return jsonify({"fired": fired, "skipped": skipped, "errors": errors})

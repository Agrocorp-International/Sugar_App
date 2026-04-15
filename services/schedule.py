"""Schedule math for auto PnL snapshots.

All user-facing times are Singapore time (SGT, UTC+8, no DST). DB stores UTC.
Kept dependency-free (stdlib only) so the tick endpoint can run fast.
"""
from datetime import datetime, timedelta, date as date_cls
import calendar

SGT_OFFSET = timedelta(hours=8)


def utc_to_sgt(dt_utc: datetime) -> datetime:
    return dt_utc + SGT_OFFSET


def sgt_to_utc(dt_sgt: datetime) -> datetime:
    return dt_sgt - SGT_OFFSET


def _last_weekday_of_month(year: int, month: int) -> int:
    """Return the day number of the last Mon–Fri in the given month."""
    last_day = calendar.monthrange(year, month)[1]
    d = date_cls(year, month, last_day)
    while d.weekday() >= 5:  # 5=Sat,6=Sun
        d = d - timedelta(days=1)
    return d.day


def current_scheduled_occurrence(schedule, now_utc: datetime) -> datetime | None:
    """Return the current period's scheduled occurrence as UTC, or None.

    "Current period" is defined per slot:
      - daily   : today (SGT)
      - weekly  : this week's configured weekday (SGT); returns None if weekday unset
      - monthly : this month's configured day (SGT); -1 = last weekday of month;
                  returns None if day_of_month unset or exceeds month length
    """
    if schedule.slot not in ("daily", "weekly", "monthly"):
        return None

    now_sgt = utc_to_sgt(now_utc)
    hh, mm = int(schedule.hour), int(schedule.minute)

    if schedule.slot == "daily":
        occ_sgt = now_sgt.replace(hour=hh, minute=mm, second=0, microsecond=0)
        return sgt_to_utc(occ_sgt)

    if schedule.slot == "weekly":
        if schedule.weekday is None:
            return None
            # align to this ISO-week's target weekday
        delta = int(schedule.weekday) - now_sgt.weekday()
        occ_sgt = (now_sgt + timedelta(days=delta)).replace(hour=hh, minute=mm, second=0, microsecond=0)
        return sgt_to_utc(occ_sgt)

    # monthly
    if schedule.day_of_month is None:
        return None
    y, m = now_sgt.year, now_sgt.month
    dom = int(schedule.day_of_month)
    if dom == -1:
        target_day = _last_weekday_of_month(y, m)
    else:
        last_day = calendar.monthrange(y, m)[1]
        if dom < 1 or dom > last_day:
            return None
        target_day = dom
    occ_sgt = now_sgt.replace(day=target_day, hour=hh, minute=mm, second=0, microsecond=0)
    return sgt_to_utc(occ_sgt)


def is_due(schedule, now_utc: datetime):
    """Return (due, occurrence_utc). Due iff enabled, now past the occurrence,
    and that exact occurrence has not already been processed."""
    if not schedule.enabled:
        return False, None
    occ = current_scheduled_occurrence(schedule, now_utc)
    if occ is None:
        return False, None
    if now_utc < occ:
        return False, occ
    if schedule.last_scheduled_for == occ:
        return False, occ
    return True, occ

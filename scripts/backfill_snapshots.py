"""Combined backfill — clears all snapshot tables and refills them.

Phase 1 — Daily (last DAILY_DAYS days, every day):
    daily_hour_snapshot   exact faults per hour for that day
    daily_plc_snapshot    exact faults per PLC for that day

Phase 2 — Weekly (last WEEKS_BACK Mondays):
    top_riser_snapshot        recent 7d vs baseline 30d ending on that Monday
    mtbf_snapshot             avg hours between faults, 30d window ending on that Monday
    repeat_offender_snapshot  max faults in single hour, 30d window ending on that Monday

Phase 3 — Long-term trend (up to 52 weeks of available data):
    long_term_trend_snapshot  weekly fault counts per fault/PLC combination
"""

from collections import defaultdict
from datetime import date, datetime, timedelta, timezone

try:
    from zoneinfo import ZoneInfo
    _BRUSSELS = ZoneInfo("Europe/Brussels")
except ImportError:
    _BRUSSELS = timezone(timedelta(hours=1))

from business.core.fault_count_service import FaultCountService
from data.repositories.snapshot_repository import SnapshotRepository

DAILY_DAYS    = 90
WEEKS_BACK    = 52
RECENT_DAYS   = 7
BASELINE_DAYS = 30
DAYS_WINDOW   = 30
TOP_N         = 10


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _today() -> date:
    return datetime.now(tz=_BRUSSELS).date()


def _to_utc(d: date) -> datetime:
    return datetime(d.year, d.month, d.day, tzinfo=_BRUSSELS).astimezone(timezone.utc)


def _fetch(start: date, end: date) -> list:
    return FaultCountService._fetch_window(_to_utc(start), _to_utc(end))


def _past_mondays(weeks: int) -> list[date]:
    today = _today()
    last_monday = today - timedelta(days=today.weekday())
    return [last_monday - timedelta(weeks=i) for i in range(1, weeks + 1)]


def _clear_all_tables() -> None:
    from sqlalchemy import text
    from data.repositories.DB_Connection import get_session
    tables = [
        "daily_hour_snapshot",
        "daily_plc_snapshot",
        "top_riser_snapshot",
        "mtbf_snapshot",
        "repeat_offender_snapshot",
        "long_term_trend_snapshot",
    ]
    with get_session() as session:
        for table in tables:
            session.execute(text(f"DELETE FROM dbo.{table}"))
    print(f"  Cleared: {', '.join(tables)}")


# ---------------------------------------------------------------------------
# Phase 1 — daily hour / plc
# ---------------------------------------------------------------------------

def _daily_hour_plc(ref: date) -> tuple[list, list]:
    rows = _fetch(ref, ref + timedelta(days=1))
    hour_counts: dict[int, int] = defaultdict(int)
    plc_counts:  dict[int, int] = defaultdict(int)
    for utc_dt, plc_id, _, _, _ in rows:
        hour = utc_dt.replace(tzinfo=timezone.utc).astimezone(_BRUSSELS).hour
        hour_counts[hour] += 1
        plc_counts[plc_id] += 1
    by_hour = [(h, hour_counts.get(h, 0)) for h in range(24)]
    by_plc  = list(plc_counts.items())
    return by_hour, by_plc


# ---------------------------------------------------------------------------
# Phase 2 — weekly risers / mtbf / repeat offenders
# ---------------------------------------------------------------------------

def _top_risers(monday: date) -> list:
    recent_rows   = _fetch(monday, monday + timedelta(days=RECENT_DAYS))
    baseline_rows = _fetch(monday - timedelta(days=BASELINE_DAYS), monday)

    recent_counts:   dict[tuple, int] = defaultdict(int)
    baseline_counts: dict[tuple, int] = defaultdict(int)
    for _, plc_id, td_id, _, _ in recent_rows:
        recent_counts[(plc_id, td_id)] += 1
    for _, plc_id, td_id, _, _ in baseline_rows:
        baseline_counts[(plc_id, td_id)] += 1

    risers = []
    for (plc_id, td_id), r_count in recent_counts.items():
        b_count = baseline_counts.get((plc_id, td_id), 0)
        if b_count == 0:
            continue
        baseline_rate = b_count / BASELINE_DAYS
        delta_pct = (r_count / RECENT_DAYS - baseline_rate) / baseline_rate * 100
        if delta_pct > 0:
            risers.append((plc_id, td_id, r_count, b_count, delta_pct))

    risers.sort(key=lambda x: x[4], reverse=True)
    return risers[:TOP_N]


def _mtbf(monday: date) -> list:
    rows = _fetch(monday - timedelta(days=DAYS_WINDOW), monday + timedelta(days=7))
    plc_timestamps: dict[int, list[datetime]] = defaultdict(list)
    for utc_dt, plc_id, _, _, _ in rows:
        plc_timestamps[plc_id].append(utc_dt.replace(tzinfo=timezone.utc))

    results = []
    for plc_id, timestamps in plc_timestamps.items():
        if len(timestamps) < 2:
            continue
        timestamps.sort()
        gaps = [(b - a).total_seconds() / 3600 for a, b in zip(timestamps, timestamps[1:])]
        results.append((plc_id, sum(gaps) / len(gaps), len(timestamps)))
    return results


def _repeat_offenders(monday: date) -> list:
    rows = _fetch(monday - timedelta(days=DAYS_WINDOW), monday + timedelta(days=7))
    bucket_counts: dict[tuple, int] = defaultdict(int)
    for utc_dt, plc_id, td_id, _, _ in rows:
        local_dt = utc_dt.replace(tzinfo=timezone.utc).astimezone(_BRUSSELS)
        bucket_counts[(plc_id, td_id, local_dt.date(), local_dt.hour)] += 1

    max_per_fault: dict[tuple, int] = defaultdict(int)
    for (plc_id, td_id, _d, _h), count in bucket_counts.items():
        key = (plc_id, td_id)
        if count > max_per_fault[key]:
            max_per_fault[key] = count

    results = [
        (plc_id, td_id, count)
        for (plc_id, td_id), count in max_per_fault.items()
        if count > 1
    ]
    return sorted(results, key=lambda x: x[2], reverse=True)[:TOP_N]


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def run() -> None:
    repo  = SnapshotRepository()
    today = _today()

    print("Clearing all snapshot tables ...")
    _clear_all_tables()

    # --- Phase 1: daily hour / plc ---
    print(f"\nPhase 1 — Daily snapshots (last {DAILY_DAYS} days) ...")
    start = today - timedelta(days=DAILY_DAYS)
    for i in range(DAILY_DAYS):
        ref = start + timedelta(days=i)
        if ref >= today:
            break
        print(f"  {ref} ...", end=" ", flush=True)
        try:
            by_hour, by_plc = _daily_hour_plc(ref)
            repo.save_daily_counts(ref, by_hour, by_plc)
            print(f"{sum(c for _, c in by_hour)} faults")
        except Exception as e:
            print(f"ERROR: {e}")

    # --- Phase 2: weekly risers / mtbf / repeat offenders ---
    mondays = _past_mondays(WEEKS_BACK)
    print(f"\nPhase 2 — Weekly snapshots ({mondays[-1]} → {mondays[0]}) ...")
    for monday in reversed(mondays):
        print(f"  {monday} ...", end=" ", flush=True)
        try:
            risers  = _top_risers(monday)
            mtbf    = _mtbf(monday)
            repeats = _repeat_offenders(monday)
            repo.save_top_risers(monday, RECENT_DAYS, BASELINE_DAYS, risers)
            repo.save_mtbf(monday, DAYS_WINDOW, mtbf)
            repo.save_repeat_offenders(monday, DAYS_WINDOW, repeats)
            print(f"{len(risers)} risers | {len(mtbf)} PLCs MTBF | {len(repeats)} repeats")
        except Exception as e:
            print(f"ERROR: {e}")

    # --- Phase 3: long-term trend ---
    print("\nPhase 3 — Long-term trend (up to 52 weeks) ...")
    try:
        service = FaultCountService()
        trend   = service.get_weekly_trend_snapshot_data(weeks=WEEKS_BACK)
        repo.save_weekly_trend(trend)
        unique_weeks = len({r[0] for r in trend})
        print(f"  {unique_weeks} weeks saved ({len(trend)} fault-week rows)")
    except Exception as e:
        print(f"  ERROR: {e}")

    print("\nBackfill complete.")


if __name__ == "__main__":
    run()
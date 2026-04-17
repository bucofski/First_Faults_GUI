"""Daily snapshot job — run once per day (e.g. 01:00) via cron or task scheduler.

Computes yesterday's fault counts and top risers, saves to DB, removes data
older than 90 days.
"""

from datetime import datetime, timedelta, timezone

from business.services.fault_count_service import FaultCountService
from data.repositories.snapshot_repository import SnapshotRepository

try:
    from zoneinfo import ZoneInfo
    _BRUSSELS = ZoneInfo("Europe/Brussels")
except ImportError:
    _BRUSSELS = timezone(timedelta(hours=1))

RECENT_DAYS   = 7
BASELINE_DAYS = 30
TOP_N         = 10


def run() -> None:
    reference_date = datetime.now(tz=_BRUSSELS).date() - timedelta(days=1)
    print(f"Snapshot for {reference_date} ...")

    service = FaultCountService()
    repo    = SnapshotRepository()

    by_hour, by_plc, risers = service.get_snapshot_data(
        reference_date=reference_date,
        recent_days=RECENT_DAYS,
        baseline_days=BASELINE_DAYS,
        top_n=TOP_N,
    )

    repo.save_daily_counts(reference_date, by_hour, by_plc)
    repo.save_top_risers(reference_date, RECENT_DAYS, BASELINE_DAYS, risers)
    repo.cleanup_old_snapshots()

    print(f"  hours saved : {len(by_hour)}")
    print(f"  PLCs saved  : {len(by_plc)}")
    print(f"  risers saved: {len(risers)}")
    print("Done.")


if __name__ == "__main__":
    run()
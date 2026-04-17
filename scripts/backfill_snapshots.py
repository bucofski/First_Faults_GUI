"""One-time backfill — fills the last 90 days of snapshot data.

Run once after creating the snapshot tables, then delete this script.
Skips dates that already have a snapshot.
"""

from datetime import date, datetime, timedelta, timezone

from business.services.fault_count_service import FaultCountService
from data.repositories.snapshot_repository import SnapshotRepository, RETENTION_DAYS

try:
    from zoneinfo import ZoneInfo
    _BRUSSELS = ZoneInfo("Europe/Brussels")
except ImportError:
    _BRUSSELS = timezone(timedelta(hours=1))

RECENT_DAYS   = 7
BASELINE_DAYS = 30
TOP_N         = 10


def run() -> None:
    service = FaultCountService()
    repo    = SnapshotRepository()

    today      = datetime.now(tz=_BRUSSELS).date()
    start_date = today - timedelta(days=RETENTION_DAYS)

    print(f"Backfilling {start_date} → {today - timedelta(days=1)} ({RETENTION_DAYS} days) ...")

    existing_hour = {
        d for d, in _existing_dates(repo)
    }

    for i in range(RETENTION_DAYS):
        ref = start_date + timedelta(days=i)
        if ref >= today:
            break

        if ref in existing_hour:
            print(f"  {ref} — already exists, skipping")
            continue

        print(f"  {ref} ...", end=" ", flush=True)
        try:
            by_hour, by_plc, risers = service.get_snapshot_data(
                reference_date=ref,
                recent_days=RECENT_DAYS,
                baseline_days=BASELINE_DAYS,
                top_n=TOP_N,
            )
            repo.save_daily_counts(ref, by_hour, by_plc)
            repo.save_top_risers(ref, RECENT_DAYS, BASELINE_DAYS, risers)
            total = sum(c for _, c in by_hour)
            print(f"{total} faults")
        except Exception as e:
            print(f"ERROR: {e}")

    print("Backfill complete.")


def _existing_dates(repo: SnapshotRepository):
    from sqlalchemy import select
    from data.orm.reporting_orm import DailyHourSnapshot
    from data.repositories.DB_Connection import get_session
    with get_session() as session:
        return session.execute(
            select(DailyHourSnapshot.snapshot_date).distinct()
        ).all()


if __name__ == "__main__":
    run()
"""Save and read pre-computed fault count snapshots."""

from datetime import date, timedelta

from sqlalchemy import delete, select

from data.orm.reporting_orm import (
    DailyHourSnapshot, DailyPlcSnapshot, MtbfSnapshot,
    RepeatOffenderSnapshot, TopRiserSnapshot,
)
from data.repositories.DB_Connection import get_session

RETENTION_DAYS = 90


class SnapshotRepository:

    # ------------------------------------------------------------------
    # Save
    # ------------------------------------------------------------------

    def save_daily_counts(
        self,
        snapshot_date: date,
        by_hour: list[tuple[int, int]],          # (hour, count)
        by_plc:  list[tuple[int, int]],           # (plc_id, count)
    ) -> None:
        with get_session() as session:
            session.execute(
                delete(DailyHourSnapshot)
                .where(DailyHourSnapshot.snapshot_date == snapshot_date)
            )
            session.execute(
                delete(DailyPlcSnapshot)
                .where(DailyPlcSnapshot.snapshot_date == snapshot_date)
            )
            for hour, count in by_hour:
                session.add(DailyHourSnapshot(
                    snapshot_date=snapshot_date, hour=hour, fault_count=count,
                ))
            for plc_id, count in by_plc:
                session.add(DailyPlcSnapshot(
                    snapshot_date=snapshot_date, plc_id=plc_id, fault_count=count,
                ))

    def save_top_risers(
        self,
        snapshot_date: date,
        recent_days:   int,
        baseline_days: int,
        risers: list[tuple[int, int, int, int, float]],  # (plc_id, text_def_id, recent, baseline, delta_pct)
    ) -> None:
        with get_session() as session:
            session.execute(
                delete(TopRiserSnapshot)
                .where(TopRiserSnapshot.snapshot_date  == snapshot_date)
                .where(TopRiserSnapshot.recent_days    == recent_days)
                .where(TopRiserSnapshot.baseline_days  == baseline_days)
            )
            for plc_id, text_def_id, recent, baseline, delta_pct in risers:
                session.add(TopRiserSnapshot(
                    snapshot_date=snapshot_date,
                    recent_days=recent_days,
                    baseline_days=baseline_days,
                    plc_id=plc_id,
                    text_def_id=text_def_id,
                    recent_count=recent,
                    baseline_count=baseline,
                    delta_pct=delta_pct,
                ))

    def save_mtbf(
        self,
        snapshot_date: date,
        days_window:   int,
        rows: list[tuple[int, float, int]],   # (plc_id, avg_hours, fault_count)
    ) -> None:
        with get_session() as session:
            session.execute(
                delete(MtbfSnapshot)
                .where(MtbfSnapshot.snapshot_date == snapshot_date)
                .where(MtbfSnapshot.days_window   == days_window)
            )
            for plc_id, avg_hours, fault_count in rows:
                session.add(MtbfSnapshot(
                    snapshot_date=snapshot_date,
                    days_window=days_window,
                    plc_id=plc_id,
                    avg_hours=avg_hours,
                    fault_count=fault_count,
                ))

    # ------------------------------------------------------------------
    # Read
    # ------------------------------------------------------------------

    def get_latest_hour_snapshot(self) -> tuple[date | None, list[tuple[int, int]]]:
        """Return (snapshot_date, [(hour, count), ...]) for the most recent date."""
        with get_session() as session:
            latest = session.execute(
                select(DailyHourSnapshot.snapshot_date)
                .order_by(DailyHourSnapshot.snapshot_date.desc())
                .limit(1)
            ).scalar_one_or_none()

            if latest is None:
                return None, []

            rows = session.execute(
                select(DailyHourSnapshot.hour, DailyHourSnapshot.fault_count)
                .where(DailyHourSnapshot.snapshot_date == latest)
                .order_by(DailyHourSnapshot.hour)
            ).all()
            return latest, [(r.hour, r.fault_count) for r in rows]

    def get_latest_plc_snapshot(self) -> tuple[date | None, list[tuple[str, int]]]:
        """Return (snapshot_date, [(plc_name, count), ...]) sorted descending."""
        with get_session() as session:
            latest = session.execute(
                select(DailyPlcSnapshot.snapshot_date)
                .order_by(DailyPlcSnapshot.snapshot_date.desc())
                .limit(1)
            ).scalar_one_or_none()

            if latest is None:
                return None, []

            rows = session.execute(
                select(DailyPlcSnapshot)
                .where(DailyPlcSnapshot.snapshot_date == latest)
                .order_by(DailyPlcSnapshot.fault_count.desc())
            ).scalars().all()
            return latest, [(r.plc.plc_name.strip(), r.fault_count) for r in rows]

    def get_latest_top_risers(
        self,
        recent_days:   int = 7,
        baseline_days: int = 30,
        top_n:         int = 10,
    ) -> tuple[date | None, list]:
        """Return (snapshot_date, [TopRiserSnapshot, ...]) for the most recent date."""
        with get_session() as session:
            latest = session.execute(
                select(TopRiserSnapshot.snapshot_date)
                .where(TopRiserSnapshot.recent_days   == recent_days)
                .where(TopRiserSnapshot.baseline_days == baseline_days)
                .order_by(TopRiserSnapshot.snapshot_date.desc())
                .limit(1)
            ).scalar_one_or_none()

            if latest is None:
                return None, []

            rows = session.execute(
                select(TopRiserSnapshot)
                .where(TopRiserSnapshot.snapshot_date  == latest)
                .where(TopRiserSnapshot.recent_days    == recent_days)
                .where(TopRiserSnapshot.baseline_days  == baseline_days)
                .order_by(TopRiserSnapshot.delta_pct.desc())
                .limit(top_n)
            ).scalars().all()

            return latest, [
                {
                    "mnemonic":       r.text_def.mnemonic.strip(),
                    "plc_name":       r.plc.plc_name.strip(),
                    "recent_count":   r.recent_count,
                    "baseline_count": r.baseline_count,
                    "delta_pct":      r.delta_pct,
                }
                for r in rows
            ]

    def save_repeat_offenders(
        self,
        snapshot_date: date,
        days_window:   int,
        rows: list[tuple[int, int, int]],  # (plc_id, text_def_id, max_per_hour)
    ) -> None:
        with get_session() as session:
            session.execute(
                delete(RepeatOffenderSnapshot)
                .where(RepeatOffenderSnapshot.snapshot_date == snapshot_date)
                .where(RepeatOffenderSnapshot.days_window   == days_window)
            )
            for plc_id, text_def_id, max_per_hour in rows:
                session.add(RepeatOffenderSnapshot(
                    snapshot_date=snapshot_date,
                    days_window=days_window,
                    plc_id=plc_id,
                    text_def_id=text_def_id,
                    max_per_hour=max_per_hour,
                ))

    def get_latest_repeat_offenders(
        self,
        days_window: int = 30,
        top_n:       int = 10,
    ) -> tuple[date | None, list[tuple[str, str, int]]]:
        """Return (snapshot_date, [(mnemonic, plc_name, max_per_hour), ...])."""
        with get_session() as session:
            latest = session.execute(
                select(RepeatOffenderSnapshot.snapshot_date)
                .where(RepeatOffenderSnapshot.days_window == days_window)
                .order_by(RepeatOffenderSnapshot.snapshot_date.desc())
                .limit(1)
            ).scalar_one_or_none()

            if latest is None:
                return None, []

            rows = session.execute(
                select(
                    RepeatOffenderSnapshot.plc_id,
                    RepeatOffenderSnapshot.text_def_id,
                    RepeatOffenderSnapshot.max_per_hour,
                )
                .where(RepeatOffenderSnapshot.snapshot_date == latest)
                .where(RepeatOffenderSnapshot.days_window   == days_window)
                .order_by(RepeatOffenderSnapshot.max_per_hour.desc())
                .limit(top_n)
            ).all()

            from data.orm.reporting_orm import Plc, TextDefinition
            plc_ids     = [r.plc_id      for r in rows]
            text_def_ids = [r.text_def_id for r in rows]

            plc_names = {
                pid: name.strip() for pid, name in session.execute(
                    select(Plc.plc_id, Plc.plc_name).where(Plc.plc_id.in_(plc_ids))
                ).all()
            }
            mnemonics = {
                tid: m.strip() for tid, m in session.execute(
                    select(TextDefinition.text_def_id, TextDefinition.mnemonic)
                    .where(TextDefinition.text_def_id.in_(text_def_ids))
                ).all()
            }

            return latest, [
                (
                    mnemonics.get(r.text_def_id, str(r.text_def_id)),
                    plc_names.get(r.plc_id, str(r.plc_id)),
                    r.max_per_hour,
                )
                for r in rows
            ]

    def get_latest_mtbf(
        self, days_window: int = 30
    ) -> tuple[date | None, list[tuple[str, float, int]]]:
        """Return (snapshot_date, [(plc_name, avg_hours, fault_count), ...]) ascending by avg_hours."""
        with get_session() as session:
            latest = session.execute(
                select(MtbfSnapshot.snapshot_date)
                .where(MtbfSnapshot.days_window == days_window)
                .order_by(MtbfSnapshot.snapshot_date.desc())
                .limit(1)
            ).scalar_one_or_none()

            if latest is None:
                return None, []

            rows = session.execute(
                select(MtbfSnapshot.avg_hours, MtbfSnapshot.fault_count, MtbfSnapshot.plc_id)
                .where(MtbfSnapshot.snapshot_date == latest)
                .where(MtbfSnapshot.days_window   == days_window)
                .order_by(MtbfSnapshot.avg_hours)
            ).all()

            plc_ids = [r.plc_id for r in rows]
            from data.orm.reporting_orm import Plc
            plc_names = {
                plc_id: name for plc_id, name in session.execute(
                    select(Plc.plc_id, Plc.plc_name).where(Plc.plc_id.in_(plc_ids))
                ).all()
            }

            return latest, [
                (plc_names.get(r.plc_id, str(r.plc_id)).strip(), r.avg_hours, r.fault_count)
                for r in rows
            ]

    # ------------------------------------------------------------------
    # Cleanup
    # ------------------------------------------------------------------

    def cleanup_old_snapshots(self, keep_days: int = RETENTION_DAYS) -> None:
        cutoff = date.today() - timedelta(days=keep_days)
        with get_session() as session:
            for model in (DailyHourSnapshot, DailyPlcSnapshot, TopRiserSnapshot, MtbfSnapshot, RepeatOffenderSnapshot):
                session.execute(
                    delete(model).where(model.snapshot_date < cutoff)
                )
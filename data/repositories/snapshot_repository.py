"""Save and read pre-computed fault count snapshots."""

from datetime import date, timedelta

from sqlalchemy import delete, func, select

from data.orm.reporting_orm import (
    DailyHourSnapshot, DailyPlcSnapshot, LongTermTrendSnapshot,
    MtbfSnapshot, RepeatOffenderSnapshot, TopRiserSnapshot,
    VwDailyHourSnapshot, VwDailyPlcSnapshot, VwTopRiserSnapshot,
    VwMtbfSnapshot, VwRepeatOffenderSnapshot, VwLongTermTrendSnapshot,
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

    def get_latest_hour_snapshot(self, reference_date: date | None = None) -> tuple[date | None, list[tuple[int, int]]]:
        """Return (snapshot_date, [(hour, count), ...]) for the given date, or empty if no snapshot exists."""
        if reference_date is None:
            return None, []
        with get_session() as session:
            sub = select(func.max(VwDailyHourSnapshot.snapshot_date)).where(
                VwDailyHourSnapshot.snapshot_date == reference_date
            ).scalar_subquery()
            rows = session.execute(
                select(VwDailyHourSnapshot.snapshot_date, VwDailyHourSnapshot.hour, VwDailyHourSnapshot.fault_count)
                .where(VwDailyHourSnapshot.snapshot_date == sub)
                .order_by(VwDailyHourSnapshot.hour)
            ).all()
            if not rows:
                return None, []
            return rows[0].snapshot_date, [(r.hour, r.fault_count) for r in rows]

    def get_latest_plc_snapshot(self, reference_date: date | None = None) -> tuple[date | None, list[tuple[str, int]]]:
        """Return (snapshot_date, [(plc_name, count), ...]) for the given date, or empty if no snapshot exists."""
        if reference_date is None:
            return None, []
        with get_session() as session:
            sub = select(func.max(VwDailyPlcSnapshot.snapshot_date)).where(
                VwDailyPlcSnapshot.snapshot_date == reference_date
            ).scalar_subquery()
            rows = session.execute(
                select(VwDailyPlcSnapshot.snapshot_date, VwDailyPlcSnapshot.plc_name, VwDailyPlcSnapshot.fault_count)
                .where(VwDailyPlcSnapshot.snapshot_date == sub)
                .order_by(VwDailyPlcSnapshot.fault_count.desc())
            ).all()
            if not rows:
                return None, []
            return rows[0].snapshot_date, [(r.plc_name.strip(), r.fault_count) for r in rows]

    def get_latest_top_risers(
        self,
        recent_days:   int = 7,
        baseline_days: int = 30,
        top_n:         int = 10,
        reference_date: date | None = None,
    ) -> tuple[date | None, list]:
        """Return (snapshot_date, [...]) for the given monday, or empty if no snapshot exists."""
        if reference_date is None:
            return None, []
        with get_session() as session:
            sub = (
                select(func.max(VwTopRiserSnapshot.snapshot_date))
                .where(VwTopRiserSnapshot.snapshot_date  == reference_date)
                .where(VwTopRiserSnapshot.recent_days    == recent_days)
                .where(VwTopRiserSnapshot.baseline_days  == baseline_days)
            ).scalar_subquery()
            rows = session.execute(
                select(VwTopRiserSnapshot)
                .where(VwTopRiserSnapshot.snapshot_date  == sub)
                .where(VwTopRiserSnapshot.recent_days    == recent_days)
                .where(VwTopRiserSnapshot.baseline_days  == baseline_days)
                .order_by(VwTopRiserSnapshot.delta_pct.desc())
                .limit(top_n)
            ).scalars().all()
            if not rows:
                return None, []
            return rows[0].snapshot_date, [
                {
                    "mnemonic":       r.mnemonic.strip(),
                    "plc_name":       r.plc_name.strip(),
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
        reference_date: date | None = None,
    ) -> tuple[date | None, list[tuple[str, str, int]]]:
        """Return (snapshot_date, [(mnemonic, plc_name, max_per_hour), ...]) for the given monday."""
        if reference_date is None:
            return None, []
        with get_session() as session:
            sub = (
                select(func.max(VwRepeatOffenderSnapshot.snapshot_date))
                .where(VwRepeatOffenderSnapshot.snapshot_date == reference_date)
                .where(VwRepeatOffenderSnapshot.days_window   == days_window)
            ).scalar_subquery()
            rows = session.execute(
                select(VwRepeatOffenderSnapshot)
                .where(VwRepeatOffenderSnapshot.snapshot_date == sub)
                .where(VwRepeatOffenderSnapshot.days_window   == days_window)
                .order_by(VwRepeatOffenderSnapshot.max_per_hour.desc())
                .limit(top_n)
            ).scalars().all()
            if not rows:
                return None, []
            return rows[0].snapshot_date, [
                (r.mnemonic.strip(), r.plc_name.strip(), r.max_per_hour)
                for r in rows
            ]

    def get_latest_mtbf(
        self, days_window: int = 30, reference_date: date | None = None,
    ) -> tuple[date | None, list[tuple[str, float, int]]]:
        """Return (snapshot_date, [(plc_name, avg_hours, fault_count), ...]) for the given monday."""
        if reference_date is None:
            return None, []
        with get_session() as session:
            sub = (
                select(func.max(VwMtbfSnapshot.snapshot_date))
                .where(VwMtbfSnapshot.snapshot_date == reference_date)
                .where(VwMtbfSnapshot.days_window   == days_window)
            ).scalar_subquery()
            rows = session.execute(
                select(VwMtbfSnapshot)
                .where(VwMtbfSnapshot.snapshot_date == sub)
                .where(VwMtbfSnapshot.days_window   == days_window)
                .order_by(VwMtbfSnapshot.avg_hours)
            ).scalars().all()
            if not rows:
                return None, []
            return rows[0].snapshot_date, [
                (r.plc_name.strip(), r.avg_hours, r.fault_count)
                for r in rows
            ]

    def save_weekly_trend(
        self,
        rows: list[tuple[date, int, int, int]],  # (week_start, plc_id, text_def_id, count)
    ) -> None:
        if not rows:
            return
        week_starts = {r[0] for r in rows}
        with get_session() as session:
            for ws in week_starts:
                session.execute(
                    delete(LongTermTrendSnapshot)
                    .where(LongTermTrendSnapshot.week_start == ws)
                )
            for week_start, plc_id, text_def_id, count in rows:
                session.add(LongTermTrendSnapshot(
                    week_start=week_start,
                    plc_id=plc_id,
                    text_def_id=text_def_id,
                    weekly_count=count,
                ))

    def get_top_climbers(
        self,
        top_n: int = 10,
    ) -> list[dict]:
        """
        Return top_n faults with the biggest absolute climb
        (avg last 4 weeks − avg first 4 weeks).

        Each entry: {mnemonic, plc_name, weeks: [(week_start, count), ...], climb}
        """
        with get_session() as session:
            rows = session.execute(
                select(
                    VwLongTermTrendSnapshot.week_start,
                    VwLongTermTrendSnapshot.plc_id,
                    VwLongTermTrendSnapshot.text_def_id,
                    VwLongTermTrendSnapshot.plc_name,
                    VwLongTermTrendSnapshot.mnemonic,
                    VwLongTermTrendSnapshot.weekly_count,
                ).order_by(VwLongTermTrendSnapshot.week_start)
            ).all()

            if not rows:
                return []

            from collections import defaultdict as _dd
            series: dict[tuple, list] = _dd(list)
            labels: dict[tuple, tuple] = {}
            for r in rows:
                key = (r.plc_id, r.text_def_id)
                series[key].append((r.week_start, r.weekly_count))
                labels[key] = (r.mnemonic.strip(), r.plc_name.strip())

        results = []
        for (plc_id, text_def_id), weekly in series.items():
            if len(weekly) < 2:
                continue
            counts     = [c for _, c in weekly]
            n          = max(1, len(counts) // 4)
            first_avg  = sum(counts[:n])  / n
            last_avg   = sum(counts[-n:]) / n
            climb      = last_avg - first_avg
            if climb <= 0:
                continue
            mnemonic, plc_name = labels.get((plc_id, text_def_id), (str(text_def_id), str(plc_id)))
            results.append({
                "mnemonic": mnemonic,
                "plc_name": plc_name,
                "weeks":    weekly,
                "climb":    round(climb, 2),
            })

        results.sort(key=lambda x: x["climb"], reverse=True)
        return results[:top_n]

    def cleanup_weekly_trend(self, keep_weeks: int = 52) -> None:
        cutoff = date.today() - timedelta(weeks=keep_weeks)
        with get_session() as session:
            session.execute(
                delete(LongTermTrendSnapshot)
                .where(LongTermTrendSnapshot.week_start < cutoff)
            )

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
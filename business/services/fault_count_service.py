"""Count root-cause faults for yesterday (Brussels time) per hour and per PLC."""

from collections import defaultdict
from dataclasses import dataclass
from datetime import date, datetime, timezone, timedelta
from typing import Optional

from sqlalchemy import select

from data.orm.reporting_orm import RootCauseFault
from data.repositories.DB_Connection import get_session

# Brussels is UTC+1 (CET) or UTC+2 (CEST); use zoneinfo when available.
try:
    from zoneinfo import ZoneInfo
    _BRUSSELS = ZoneInfo("Europe/Brussels")
except ImportError:
    # Python < 3.9 fallback — fixed UTC+1 offset (close enough for most cases)
    _BRUSSELS = timezone(timedelta(hours=1))


def _yesterday_utc_bounds() -> tuple[datetime, datetime]:
    """Return (start_utc, end_utc) for yesterday in Brussels local time."""
    now_brussels = datetime.now(tz=_BRUSSELS)
    yesterday = now_brussels.date() - timedelta(days=1)
    start_local = datetime(yesterday.year, yesterday.month, yesterday.day,
                           tzinfo=_BRUSSELS)
    end_local = start_local + timedelta(days=1)
    return start_local.astimezone(timezone.utc), end_local.astimezone(timezone.utc)


@dataclass
class HourCount:
    hour: int          # 0-23, Brussels local hour
    fault_count: int

    def to_dict(self) -> dict:
        return {"hour": self.hour, "fault_count": self.fault_count}


@dataclass
class PlcCount:
    plc_name: str
    fault_count: int

    def to_dict(self) -> dict:
        return {"plc_name": self.plc_name, "fault_count": self.fault_count}


@dataclass
class TopRiser:
    mnemonic:       str
    plc_name:       str
    recent_count:   int
    baseline_count: int
    delta_pct:      float   # (recent_rate - baseline_rate) / baseline_rate * 100

    def to_dict(self) -> dict:
        return {
            "mnemonic":       self.mnemonic,
            "plc_name":       self.plc_name,
            "recent_count":   self.recent_count,
            "baseline_count": self.baseline_count,
            "delta_pct":      round(self.delta_pct, 1),
        }


@dataclass
class DailyFaultCounts:
    reference_date: date          # Brussels local yesterday
    by_hour: list[HourCount]      # 24 entries, hour 0-23
    by_plc: list[PlcCount]        # one entry per PLC, descending

    def to_dict(self) -> dict:
        return {
            "reference_date": str(self.reference_date),
            "by_hour": [h.to_dict() for h in self.by_hour],
            "by_plc":  [p.to_dict() for p in self.by_plc],
        }


class FaultCountService:
    """
    Reads yesterday's root-cause faults and returns counts per hour and per PLC.

    Usage
    -----
    service = FaultCountService()
    counts  = service.get_yesterday_counts()
    print(counts.to_dict())
    """

    def get_yesterday_counts(self) -> DailyFaultCounts:
        start_utc, end_utc = _yesterday_utc_bounds()
        reference_date = datetime.now(tz=_BRUSSELS).date() - timedelta(days=1)

        rows = self._fetch_window(start_utc, end_utc)
        return DailyFaultCounts(
            reference_date=reference_date,
            by_hour=self._count_by_hour(rows),
            by_plc=self._count_by_plc(rows),
        )

    def get_top_risers(
        self,
        reference_date: Optional[date] = None,
        recent_days: int = 7,
        baseline_days: int = 30,
        top_n: int = 10,
    ) -> list[TopRiser]:
        """
        Return top_n faults with the highest percentage increase.

        reference_date  end of the recent window (default: yesterday Brussels)
        recent_days     length of the recent window
        baseline_days   length of the comparison baseline before the recent window
        top_n           how many risers to return
        """
        ref = reference_date or (datetime.now(tz=_BRUSSELS).date() - timedelta(days=1))

        recent_end   = self._date_to_utc(ref + timedelta(days=1))
        recent_start = self._date_to_utc(ref - timedelta(days=recent_days - 1))
        base_end     = recent_start
        base_start   = self._date_to_utc(ref - timedelta(days=recent_days - 1 + baseline_days))

        recent_rows   = self._fetch_window(recent_start, recent_end)
        baseline_rows = self._fetch_window(base_start, base_end)

        recent_counts:   dict[tuple, int] = defaultdict(int)
        baseline_counts: dict[tuple, int] = defaultdict(int)

        for _, _plc_id, _text_def_id, plc, mnemonic in recent_rows:
            recent_counts[(mnemonic.strip(), plc.strip())] += 1
        for _, _plc_id, _text_def_id, plc, mnemonic in baseline_rows:
            baseline_counts[(mnemonic.strip(), plc.strip())] += 1

        risers: list[TopRiser] = []
        for key, r_count in recent_counts.items():
            mnemonic, plc = key
            b_count = baseline_counts.get(key, 0)
            recent_rate   = r_count   / recent_days
            baseline_rate = b_count   / baseline_days if b_count > 0 else 0

            if baseline_rate == 0:
                continue  # skip faults with no baseline — brand-new faults are noise

            delta_pct = (recent_rate - baseline_rate) / baseline_rate * 100
            if delta_pct > 0:
                risers.append(TopRiser(
                    mnemonic=mnemonic,
                    plc_name=plc,
                    recent_count=r_count,
                    baseline_count=b_count,
                    delta_pct=delta_pct,
                ))

        return sorted(risers, key=lambda x: x.delta_pct, reverse=True)[:top_n]

    # ------------------------------------------------------------------

    @staticmethod
    def _date_to_utc(d: date) -> datetime:
        local = datetime(d.year, d.month, d.day, tzinfo=_BRUSSELS)
        return local.astimezone(timezone.utc)

    def get_snapshot_data(
        self,
        reference_date: date,
        recent_days:    int = 7,
        baseline_days:  int = 30,
        top_n:          int = 10,
    ) -> tuple[
        list[tuple[int, int]],              # by_hour:  (hour, count)
        list[tuple[int, int]],              # by_plc:   (plc_id, count)
        list[tuple[int, int, int, int, float]],  # risers: (plc_id, text_def_id, recent, baseline, delta_pct)
    ]:
        """Return all data needed to write one day's snapshot to the DB."""
        start_utc = self._date_to_utc(reference_date)
        end_utc   = self._date_to_utc(reference_date + timedelta(days=1))
        rows = self._fetch_window(start_utc, end_utc)

        hour_counts: dict[int, int] = defaultdict(int)
        plc_counts:  dict[int, int] = defaultdict(int)
        for utc_dt, plc_id, _, _, _ in rows:
            utc_aware = utc_dt.replace(tzinfo=timezone.utc)
            hour_counts[utc_aware.astimezone(_BRUSSELS).hour] += 1
            plc_counts[plc_id] += 1

        by_hour = [(h, hour_counts.get(h, 0)) for h in range(24)]
        by_plc  = list(plc_counts.items())

        recent_end   = self._date_to_utc(reference_date + timedelta(days=1))
        recent_start = self._date_to_utc(reference_date - timedelta(days=recent_days - 1))
        base_end     = recent_start
        base_start   = self._date_to_utc(reference_date - timedelta(days=recent_days - 1 + baseline_days))

        recent_rows   = self._fetch_window(recent_start, recent_end)
        baseline_rows = self._fetch_window(base_start, base_end)

        recent_id_counts:   dict[tuple, int] = defaultdict(int)
        baseline_id_counts: dict[tuple, int] = defaultdict(int)
        for _, plc_id, text_def_id, _, _ in recent_rows:
            recent_id_counts[(plc_id, text_def_id)] += 1
        for _, plc_id, text_def_id, _, _ in baseline_rows:
            baseline_id_counts[(plc_id, text_def_id)] += 1

        risers = []
        for (plc_id, text_def_id), r_count in recent_id_counts.items():
            b_count       = baseline_id_counts.get((plc_id, text_def_id), 0)
            baseline_rate = b_count / baseline_days if b_count > 0 else 0
            if baseline_rate == 0:
                continue
            delta_pct = (r_count / recent_days - baseline_rate) / baseline_rate * 100
            if delta_pct > 0:
                risers.append((plc_id, text_def_id, r_count, b_count, delta_pct))

        risers.sort(key=lambda x: x[4], reverse=True)
        return by_hour, by_plc, risers[:top_n]

    @staticmethod
    def _fetch_window(
        start_utc: datetime, end_utc: datetime
    ) -> list[tuple[datetime, int, int, str, str]]:
        """Return (utc_timestamp, plc_id, text_def_id, plc_name, mnemonic) rows."""
        stmt = (
            select(
                RootCauseFault.utc_timestamp,
                RootCauseFault.plc_id,
                RootCauseFault.text_def_id,
                RootCauseFault.plc_name,
                RootCauseFault.mnemonic,
            )
            .where(RootCauseFault.utc_timestamp >= start_utc.replace(tzinfo=None))
            .where(RootCauseFault.utc_timestamp <  end_utc.replace(tzinfo=None))
        )
        with get_session() as session:
            return session.execute(stmt).all()

    @staticmethod
    def _fetch(start_utc: datetime, end_utc: datetime) -> list[tuple[datetime, str]]:
        stmt = (
            select(RootCauseFault.utc_timestamp, RootCauseFault.plc_name)
            .where(RootCauseFault.utc_timestamp >= start_utc.replace(tzinfo=None))
            .where(RootCauseFault.utc_timestamp <  end_utc.replace(tzinfo=None))
        )
        with get_session() as session:
            return session.execute(stmt).all()

    @staticmethod
    def _count_by_hour(rows: list[tuple[datetime, int, int, str, str]]) -> list[HourCount]:
        counts = [0] * 24
        for utc_dt, *_ in rows:
            utc_aware = utc_dt.replace(tzinfo=timezone.utc)
            counts[utc_aware.astimezone(_BRUSSELS).hour] += 1
        return [HourCount(hour=h, fault_count=counts[h]) for h in range(24)]

    @staticmethod
    def _count_by_plc(rows: list[tuple[datetime, int, int, str, str]]) -> list[PlcCount]:
        totals: dict[str, int] = {}
        for _, _plc_id, _text_def_id, plc, _ in rows:
            totals[plc.strip()] = totals.get(plc.strip(), 0) + 1
        return sorted(
            [PlcCount(plc_name=k, fault_count=v) for k, v in totals.items()],
            key=lambda x: x.fault_count,
            reverse=True,
        )
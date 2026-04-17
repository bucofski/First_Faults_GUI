"""ORM mappings for reporting views and snapshot tables (read-only view + writable snapshots)."""

from datetime import date, datetime

from sqlalchemy import DATE, DateTime, Float, ForeignKey, Integer, SmallInteger, String
from sqlalchemy.orm import Mapped, mapped_column, relationship

from data.repositories.DB_Connection import Base


class Plc(Base):
    __tablename__ = "PLC"

    plc_id:   Mapped[int] = mapped_column("PLC_ID", Integer, primary_key=True)
    plc_name: Mapped[str] = mapped_column("PLC_NAME", String)


class TextDefinition(Base):
    __tablename__ = "TEXT_DEFINITION"

    text_def_id: Mapped[int] = mapped_column("TEXT_DEF_ID", Integer, primary_key=True)
    mnemonic:    Mapped[str] = mapped_column("MNEMONIC", String)
    message:     Mapped[str] = mapped_column("MESSAGE", String)


class RootCauseFault(Base):
    """Maps to vw_root_cause_faults — read-only view."""
    __tablename__ = "vw_root_cause_faults"

    fault_id:      Mapped[int]      = mapped_column(primary_key=True)
    utc_timestamp: Mapped[datetime] = mapped_column(DateTime)
    plc_id:        Mapped[int]      = mapped_column("PLC_ID", Integer)
    text_def_id:   Mapped[int]      = mapped_column("TEXT_DEF_ID", Integer)
    plc_name:      Mapped[str]      = mapped_column("PLC_NAME", String)
    mnemonic:      Mapped[str]      = mapped_column("MNEMONIC", String)


class DailyHourSnapshot(Base):
    __tablename__ = "daily_hour_snapshot"

    id:            Mapped[int]  = mapped_column(Integer, primary_key=True)
    snapshot_date: Mapped[date] = mapped_column(DATE)
    hour:          Mapped[int]  = mapped_column(Integer)
    fault_count:   Mapped[int]  = mapped_column(Integer)


class DailyPlcSnapshot(Base):
    __tablename__ = "daily_plc_snapshot"

    id:            Mapped[int]  = mapped_column(Integer, primary_key=True)
    snapshot_date: Mapped[date] = mapped_column(DATE)
    plc_id:        Mapped[int]  = mapped_column(Integer, ForeignKey("PLC.PLC_ID"))
    fault_count:   Mapped[int]  = mapped_column(Integer)

    plc: Mapped[Plc] = relationship("Plc")


class TopRiserSnapshot(Base):
    __tablename__ = "top_riser_snapshot"

    id:             Mapped[int]   = mapped_column(Integer, primary_key=True)
    snapshot_date:  Mapped[date]  = mapped_column(DATE)
    recent_days:    Mapped[int]   = mapped_column(Integer)
    baseline_days:  Mapped[int]   = mapped_column(SmallInteger)
    plc_id:         Mapped[int]   = mapped_column(Integer, ForeignKey("PLC.PLC_ID"))
    text_def_id:    Mapped[int]   = mapped_column(Integer, ForeignKey("TEXT_DEFINITION.TEXT_DEF_ID"))
    recent_count:   Mapped[int]   = mapped_column(Integer)
    baseline_count: Mapped[int]   = mapped_column(Integer)
    delta_pct:      Mapped[float] = mapped_column(Float)

    plc:      Mapped[Plc]            = relationship("Plc")
    text_def: Mapped[TextDefinition] = relationship("TextDefinition")


class MtbfSnapshot(Base):
    __tablename__ = "mtbf_snapshot"

    id:            Mapped[int]   = mapped_column(Integer, primary_key=True)
    snapshot_date: Mapped[date]  = mapped_column(DATE)
    days_window:   Mapped[int]   = mapped_column(SmallInteger)
    plc_id:        Mapped[int]   = mapped_column(Integer, ForeignKey("PLC.PLC_ID"))
    avg_hours:     Mapped[float] = mapped_column(Float)
    fault_count:   Mapped[int]   = mapped_column(Integer)

    plc: Mapped[Plc] = relationship("Plc")


class RepeatOffenderSnapshot(Base):
    __tablename__ = "repeat_offender_snapshot"

    id:            Mapped[int]  = mapped_column(Integer, primary_key=True)
    snapshot_date: Mapped[date] = mapped_column(DATE)
    days_window:   Mapped[int]  = mapped_column(SmallInteger)
    plc_id:        Mapped[int]  = mapped_column(Integer, ForeignKey("PLC.PLC_ID"))
    text_def_id:   Mapped[int]  = mapped_column(Integer, ForeignKey("TEXT_DEFINITION.TEXT_DEF_ID"))
    max_per_hour:  Mapped[int]  = mapped_column(Integer)
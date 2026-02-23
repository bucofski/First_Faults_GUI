from datetime import datetime, date
from decimal import Decimal
from typing import Optional, List
from sqlalchemy import String, Integer, ForeignKey, UniqueConstraint, Index, ForeignKeyConstraint
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, relationship


class Base(DeclarativeBase):
    pass


class PLC(Base):
    __tablename__ = "PLC"

    plc_id: Mapped[int] = mapped_column("PLC_ID", Integer, primary_key=True, autoincrement=True)
    plc_name: Mapped[str] = mapped_column("PLC_NAME", String(50), nullable=False, unique=True)
    description: Mapped[Optional[str]] = mapped_column("DESCRIPTION", String(255))

    # Relationships
    interlock_definitions: Mapped[List["InterlockDefinition"]] = relationship(back_populates="plc")
    condition_definitions: Mapped[List["ConditionDefinition"]] = relationship(back_populates="plc", viewonly=True)


class TextDefinition(Base):
    __tablename__ = "TEXT_DEFINITION"

    text_def_id: Mapped[int] = mapped_column("TEXT_DEF_ID", Integer, primary_key=True, autoincrement=True)
    mnemonic: Mapped[str] = mapped_column("MNEMONIC", String(255), nullable=False)
    message: Mapped[str] = mapped_column("MESSAGE", String(500), nullable=False)

    __table_args__ = (
        UniqueConstraint("MNEMONIC", "MESSAGE", name="UQ_Mnemonic_Message"),
    )

    # Relationships
    interlock_definitions: Mapped[List["InterlockDefinition"]] = relationship(back_populates="text_definition")
    condition_definitions: Mapped[List["ConditionDefinition"]] = relationship(back_populates="text_definition")


class InterlockDefinition(Base):
    __tablename__ = "INTERLOCK_DEFINITION"

    interlock_def_id: Mapped[int] = mapped_column("INTERLOCK_DEF_ID", Integer, primary_key=True, autoincrement=True)
    plc_id: Mapped[int] = mapped_column("PLC_ID", ForeignKey("PLC.PLC_ID"), nullable=False)
    number: Mapped[int] = mapped_column("NUMBER", Integer, nullable=False)
    text_def_id: Mapped[int] = mapped_column("TEXT_DEF_ID", ForeignKey("TEXT_DEFINITION.TEXT_DEF_ID"), nullable=False)

    __table_args__ = (
        UniqueConstraint("PLC_ID", "NUMBER", name="UQ_PLC_Number"),
        Index("IX_InterlockDef_Number", "NUMBER"),
        Index("IX_InterlockDef_PLC", "PLC_ID"),
        Index("IX_InterlockDef_Text", "TEXT_DEF_ID"),
    )

    # Relationships
    plc: Mapped["PLC"] = relationship(back_populates="interlock_definitions")
    text_definition: Mapped["TextDefinition"] = relationship(back_populates="interlock_definitions")
    condition_definitions: Mapped[List["ConditionDefinition"]] = relationship(back_populates="interlock_definition")
    interlock_logs: Mapped[List["FFInterlockLog"]] = relationship(back_populates="interlock_definition")

class ConditionDefinition(Base):
    __tablename__ = "CONDITION_DEFINITION"

    condition_def_id: Mapped[int] = mapped_column("CONDITION_DEF_ID", Integer, primary_key=True, autoincrement=True)
    plc_id: Mapped[int] = mapped_column("PLC_ID", ForeignKey("PLC.PLC_ID"), nullable=False)
    interlock_number: Mapped[int] = mapped_column("INTERLOCK_NUMBER", Integer, nullable=False)
    type: Mapped[int] = mapped_column("TYPE", Integer, nullable=False)
    bit_index: Mapped[int] = mapped_column("BIT_INDEX", Integer, nullable=False)
    text_def_id: Mapped[int] = mapped_column("TEXT_DEF_ID", ForeignKey("TEXT_DEFINITION.TEXT_DEF_ID"), nullable=False)

    __table_args__ = (
        ForeignKeyConstraint(
            ["PLC_ID", "INTERLOCK_NUMBER"],
            ["INTERLOCK_DEFINITION.PLC_ID", "INTERLOCK_DEFINITION.NUMBER"],
            name="FK_ConditionDef_Interlock"
        ),
        UniqueConstraint("PLC_ID", "INTERLOCK_NUMBER", "TYPE", "BIT_INDEX", name="UQ_PLC_InterlockNum_Type_BitIndex"),
        Index("IX_ConditionDef_PLC_InterlockNum", "PLC_ID", "INTERLOCK_NUMBER"),
        Index("IX_ConditionDef_Text", "TEXT_DEF_ID"),
    )

    # Relationships - mark plc as viewonly since PLC_ID comes from interlock_definition
    plc: Mapped["PLC"] = relationship(back_populates="condition_definitions", viewonly=True)
    text_definition: Mapped["TextDefinition"] = relationship(back_populates="condition_definitions")
    interlock_definition: Mapped["InterlockDefinition"] = relationship(
        back_populates="condition_definitions",
        foreign_keys=[plc_id, interlock_number],
        primaryjoin="and_(ConditionDefinition.plc_id==InterlockDefinition.plc_id, "
                    "ConditionDefinition.interlock_number==InterlockDefinition.number)"
    )
    condition_logs: Mapped[List["FFConditionLog"]] = relationship(back_populates="condition_definition")
    fault_trend_snapshots: Mapped[List["FaultTrendSnapshot"]] = relationship(back_populates="condition_definition")

class FFInterlockLog(Base):
    __tablename__ = "FF_INTERLOCK_LOG"

    id: Mapped[int] = mapped_column("ID", Integer, primary_key=True, autoincrement=True)
    interlock_def_id: Mapped[int] = mapped_column("INTERLOCK_DEF_ID", ForeignKey("INTERLOCK_DEFINITION.INTERLOCK_DEF_ID"), nullable=False)
    timestamp: Mapped[datetime] = mapped_column("TIMESTAMP", nullable=False)
    timestamp_log: Mapped[datetime] = mapped_column("TIMESTAMP_LOG", nullable=False)
    order_log: Mapped[int] = mapped_column("ORDER_LOG", Integer, nullable=False)
    upstream_interlock_log_id: Mapped[Optional[int]] = mapped_column("UPSTREAM_INTERLOCK_LOG_ID", ForeignKey("FF_INTERLOCK_LOG.ID"))

    __table_args__ = (
        Index("IX_InterlockLog_Timestamp", "TIMESTAMP", postgresql_ops={"TIMESTAMP": "DESC"}),
        Index("IX_InterlockLog_DefID", "INTERLOCK_DEF_ID"),
        Index("IX_InterlockLog_Upstream", "UPSTREAM_INTERLOCK_LOG_ID"),
    )

    # Relationships
    interlock_definition: Mapped["InterlockDefinition"] = relationship(back_populates="interlock_logs")
    upstream_interlock_log: Mapped[Optional["FFInterlockLog"]] = relationship(remote_side=[id])
    condition_logs: Mapped[List["FFConditionLog"]] = relationship(back_populates="interlock_log")


class FFConditionLog(Base):
    __tablename__ = "FF_CONDITION_LOG"

    id: Mapped[int] = mapped_column("ID", Integer, primary_key=True, autoincrement=True)
    interlock_log_id: Mapped[int] = mapped_column("INTERLOCK_LOG_ID", ForeignKey("FF_INTERLOCK_LOG.ID"), nullable=False)
    condition_def_id: Mapped[int] = mapped_column("CONDITION_DEF_ID", ForeignKey("CONDITION_DEFINITION.CONDITION_DEF_ID"), nullable=False)

    __table_args__ = (
        Index("IX_ConditionLog_InterlockID", "INTERLOCK_LOG_ID"),
    )

    # Relationships
    interlock_log: Mapped["FFInterlockLog"] = relationship(back_populates="condition_logs")
    condition_definition: Mapped["ConditionDefinition"] = relationship(back_populates="condition_logs")


class TrendAnalysisConfig(Base):
    __tablename__ = "TREND_ANALYSIS_CONFIG"

    config_id: Mapped[int] = mapped_column("CONFIG_ID", Integer, primary_key=True, autoincrement=True)
    days_recent: Mapped[int] = mapped_column("DAYS_RECENT", Integer, nullable=False)
    days_previous: Mapped[int] = mapped_column("DAYS_PREVIOUS", Integer, nullable=False)

    __table_args__ = (
        UniqueConstraint("DAYS_RECENT", "DAYS_PREVIOUS", name="UQ_Days_Configuration"),
    )

    # Relationships
    fault_trend_snapshots: Mapped[List["FaultTrendSnapshot"]] = relationship(back_populates="config")


class FaultTrendSnapshot(Base):
    __tablename__ = "FAULT_TREND_SNAPSHOTS"

    snapshot_id: Mapped[int] = mapped_column("SNAPSHOT_ID", Integer, primary_key=True, autoincrement=True)
    snapshot_date: Mapped[date] = mapped_column("SNAPSHOT_DATE", nullable=False)
    condition_def_id: Mapped[int] = mapped_column("CONDITION_DEF_ID", ForeignKey("CONDITION_DEFINITION.CONDITION_DEF_ID"), nullable=False)
    config_id: Mapped[int] = mapped_column("CONFIG_ID", ForeignKey("TREND_ANALYSIS_CONFIG.CONFIG_ID"), nullable=False)

    recent_daily_avg: Mapped[Optional[Decimal]] = mapped_column("RECENT_DAILY_AVG")
    previous_daily_avg: Mapped[Optional[Decimal]] = mapped_column("PREVIOUS_DAILY_AVG")
    change_percent: Mapped[Optional[Decimal]] = mapped_column("CHANGE_PERCENT")
    absolute_change: Mapped[Optional[Decimal]] = mapped_column("ABSOLUTE_CHANGE")
    recent_count: Mapped[Optional[int]] = mapped_column("RECENT_COUNT", Integer)
    previous_count: Mapped[Optional[int]] = mapped_column("PREVIOUS_COUNT", Integer)
    confidence_score: Mapped[Optional[Decimal]] = mapped_column("CONFIDENCE_SCORE")
    rank_position: Mapped[Optional[int]] = mapped_column("RANK_POSITION", Integer)
    created_at: Mapped[datetime] = mapped_column("CREATED_AT", default=datetime.now)

    __table_args__ = (
        UniqueConstraint("SNAPSHOT_DATE", "CONDITION_DEF_ID", "CONFIG_ID", name="UQ_Snapshot_Date_Condition_Config"),
        Index("IX_FaultTrendSnapshots_SnapshotDate", "SNAPSHOT_DATE", postgresql_ops={"SNAPSHOT_DATE": "DESC"}),
        Index("IX_FaultTrendSnapshots_ConditionDefID", "CONDITION_DEF_ID"),
        Index("IX_FaultTrendSnapshots_ConfigID", "CONFIG_ID"),
        Index("IX_FaultTrendSnapshots_RankPosition", "SNAPSHOT_DATE", "RANK_POSITION"),
    )

    # Relationships
    condition_definition: Mapped["ConditionDefinition"] = relationship(back_populates="fault_trend_snapshots")
    config: Mapped["TrendAnalysisConfig"] = relationship(back_populates="fault_trend_snapshots")


# Need to add this import at the top for ForeignKeyConstraint
from sqlalchemy import ForeignKeyConstraint
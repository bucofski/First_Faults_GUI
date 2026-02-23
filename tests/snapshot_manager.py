import pandas as pd
from datetime import datetime, date
from decimal import Decimal
from typing import Optional
from sqlalchemy import select, func, and_
from sqlalchemy.orm import Session

from data.repositories.DB_Connection import get_engine, get_session
from ml_models import (
    FaultTrendSnapshot, TrendAnalysisConfig,
    ConditionDefinition, TextDefinition
)


class TrendSnapshotManager:
    """Manage periodic snapshots of fault trends for historical tracking."""

    def __init__(self):
        self.engine = get_engine()

    def _get_or_create_config(self, session: Session, days_recent: int, days_previous: int) -> TrendAnalysisConfig:
        """Get or create a config for the given parameters."""
        stmt = select(TrendAnalysisConfig).where(
            and_(
                TrendAnalysisConfig.days_recent == days_recent,
                TrendAnalysisConfig.days_previous == days_previous
            )
        )
        config = session.execute(stmt).scalar_one_or_none()

        if config is None:
            config = TrendAnalysisConfig(days_recent=days_recent, days_previous=days_previous)
            session.add(config)
            session.flush()

        return config

    def _get_condition_def_id(self, session: Session, condition_mnemonic: str, plc_name: str) -> Optional[int]:
        """Look up CONDITION_DEF_ID using mnemonic + PLC."""
        from ml_models import PLC

        stmt = (
            select(ConditionDefinition.condition_def_id)
            .join(TextDefinition, ConditionDefinition.text_def_id == TextDefinition.text_def_id)
            .join(PLC, ConditionDefinition.plc_id == PLC.plc_id)
            .where(
                and_(
                    TextDefinition.mnemonic == condition_mnemonic,
                    PLC.plc_name == plc_name
                )
            )
        )
        result = session.execute(stmt).scalars().first()
        return result

    def save_snapshot(self, analyzer, days_recent: int = 7, days_previous: int = 30,
                      snapshot_date: Optional[date] = None):
        """Save current top risers analysis to database."""
        if snapshot_date is None:
            snapshot_date = datetime.now().date()

        result = analyzer.top_risers(
            days_recent=days_recent,
            days_previous=days_previous,
            top_n=50,
            min_recent_count=3
        )

        if result.empty:
            print(f"No risers to snapshot for {snapshot_date}")
            return

        with get_session() as session:
            config = self._get_or_create_config(session, days_recent, days_previous)

            saved_count = 0
            skipped_count = 0

            for _, row in result.iterrows():
                condition_def_id = self._get_condition_def_id(session, row['Condition'], row['PLC'])

                if condition_def_id is None:
                    skipped_count += 1
                    continue

                # Check if exists, update or insert
                existing = session.execute(
                    select(FaultTrendSnapshot).where(
                        and_(
                            FaultTrendSnapshot.snapshot_date == snapshot_date,
                            FaultTrendSnapshot.condition_def_id == condition_def_id,
                            FaultTrendSnapshot.config_id == config.config_id
                        )
                    )
                ).scalar_one_or_none()

                change_pct = None if row['Change_%'] == 'NEW' else Decimal(str(row['Change_%']))

                if existing:
                    existing.recent_daily_avg = Decimal(str(row['Recent_Daily_Avg']))
                    existing.previous_daily_avg = Decimal(str(row['Previous_Daily_Avg']))
                    existing.change_percent = change_pct
                    existing.absolute_change = Decimal(str(row['Absolute_Change']))
                    existing.recent_count = int(row['Recent_Count'])
                    existing.previous_count = int(row['Previous_Count'])
                    existing.confidence_score = Decimal(str(row['Confidence_Score']))
                    existing.rank_position = int(row['Rank'])
                else:
                    snapshot = FaultTrendSnapshot(
                        snapshot_date=snapshot_date,
                        condition_def_id=condition_def_id,
                        config_id=config.config_id,
                        recent_daily_avg=Decimal(str(row['Recent_Daily_Avg'])),
                        previous_daily_avg=Decimal(str(row['Previous_Daily_Avg'])),
                        change_percent=change_pct,
                        absolute_change=Decimal(str(row['Absolute_Change'])),
                        recent_count=int(row['Recent_Count']),
                        previous_count=int(row['Previous_Count']),
                        confidence_score=Decimal(str(row['Confidence_Score'])),
                        rank_position=int(row['Rank'])
                    )
                    session.add(snapshot)

                saved_count += 1

        print(f"✓ Saved {saved_count} fault trends for {snapshot_date}")
        if skipped_count > 0:
            print(f"⚠ Skipped {skipped_count} faults (not found in CONDITION_DEFINITION)")

    def get_fault_trend_history(self, condition_mnemonic: str, limit: int = 12) -> pd.DataFrame:
        """Get historical trend for a specific fault."""
        with get_session() as session:
            stmt = (
                select(
                    FaultTrendSnapshot.snapshot_date,
                    FaultTrendSnapshot.recent_daily_avg,
                    FaultTrendSnapshot.previous_daily_avg,
                    FaultTrendSnapshot.change_percent,
                    FaultTrendSnapshot.confidence_score,
                    FaultTrendSnapshot.rank_position
                )
                .join(ConditionDefinition, FaultTrendSnapshot.condition_def_id == ConditionDefinition.condition_def_id)
                .join(TextDefinition, ConditionDefinition.text_def_id == TextDefinition.text_def_id)
                .join(TrendAnalysisConfig, FaultTrendSnapshot.config_id == TrendAnalysisConfig.config_id)
                .where(
                    and_(
                        TextDefinition.mnemonic == condition_mnemonic,
                        TrendAnalysisConfig.days_recent == 7,
                        TrendAnalysisConfig.days_previous == 30
                    )
                )
                .order_by(FaultTrendSnapshot.snapshot_date.desc())
                .limit(limit)
            )

            results = session.execute(stmt).all()

            df = pd.DataFrame(results, columns=[
                'snapshot_date', 'recent_daily_avg', 'previous_daily_avg',
                'change_percent', 'confidence_score', 'rank_position'
            ])

        return df.sort_values('snapshot_date')

    def compare_snapshots(self, date1: date, date2: date, top_n: int = 10) -> pd.DataFrame:
        """Compare top risers between two snapshot dates."""
        with get_session() as session:
            stmt = (
                select(
                    FaultTrendSnapshot.snapshot_date,
                    TextDefinition.mnemonic.label('condition_mnemonic'),
                    FaultTrendSnapshot.recent_daily_avg,
                    FaultTrendSnapshot.change_percent,
                    FaultTrendSnapshot.confidence_score,
                    FaultTrendSnapshot.rank_position
                )
                .join(ConditionDefinition, FaultTrendSnapshot.condition_def_id == ConditionDefinition.condition_def_id)
                .join(TextDefinition, ConditionDefinition.text_def_id == TextDefinition.text_def_id)
                .join(TrendAnalysisConfig, FaultTrendSnapshot.config_id == TrendAnalysisConfig.config_id)
                .where(
                    and_(
                        FaultTrendSnapshot.snapshot_date.in_([date1, date2]),
                        TrendAnalysisConfig.days_recent == 7,
                        TrendAnalysisConfig.days_previous == 30,
                        FaultTrendSnapshot.rank_position <= top_n
                    )
                )
                .order_by(FaultTrendSnapshot.snapshot_date, FaultTrendSnapshot.rank_position)
            )

            results = session.execute(stmt).all()

            df = pd.DataFrame(results, columns=[
                'snapshot_date', 'condition_mnemonic', 'recent_daily_avg',
                'change_percent', 'confidence_score', 'rank_position'
            ])

        comparison = df.pivot_table(
            index='condition_mnemonic',
            columns='snapshot_date',
            values=['recent_daily_avg', 'rank_position'],
            aggfunc='first'
        )

        return comparison

    def get_top_risers(self, top_n: int = 5) -> pd.DataFrame:
        """Get current top N rising faults from latest snapshot."""
        with get_session() as session:
            max_date_subq = select(func.max(FaultTrendSnapshot.snapshot_date)).scalar_subquery()

            stmt = (
                select(
                    TextDefinition.mnemonic.label('condition_mnemonic'),
                    FaultTrendSnapshot.recent_daily_avg,
                    FaultTrendSnapshot.change_percent,
                    FaultTrendSnapshot.confidence_score
                )
                .join(ConditionDefinition, FaultTrendSnapshot.condition_def_id == ConditionDefinition.condition_def_id)
                .join(TextDefinition, ConditionDefinition.text_def_id == TextDefinition.text_def_id)
                .where(
                    and_(
                        FaultTrendSnapshot.snapshot_date == max_date_subq,
                        FaultTrendSnapshot.rank_position <= top_n
                    )
                )
                .order_by(FaultTrendSnapshot.rank_position)
            )

            results = session.execute(stmt).all()

            return pd.DataFrame(results, columns=[
                'condition_mnemonic', 'recent_daily_avg', 'change_percent', 'confidence_score'
            ])

    def get_latest_snapshot_dates(self, limit: int = 2) -> list[date]:
        """Get the most recent snapshot dates."""
        with get_session() as session:
            stmt = (
                select(FaultTrendSnapshot.snapshot_date)
                .distinct()
                .order_by(FaultTrendSnapshot.snapshot_date.desc())
                .limit(limit)
            )
            results = session.execute(stmt).scalars().all()
            return list(results)

    def get_summary_stats(self) -> dict:
        """Get management summary statistics."""
        with get_session() as session:
            stmt = select(
                func.count(FaultTrendSnapshot.condition_def_id.distinct()).label('total_tracked'),
                func.max(FaultTrendSnapshot.snapshot_date).label('latest'),
                func.min(FaultTrendSnapshot.snapshot_date).label('first'),
                func.count(FaultTrendSnapshot.snapshot_date.distinct()).label('total_snapshots')
            )
            result = session.execute(stmt).one()

            return {
                'total_tracked_faults': result.total_tracked,
                'latest_snapshot': result.latest,
                'first_snapshot': result.first,
                'total_snapshots': result.total_snapshots
            }

    def get_top_riser_mnemonic(self) -> Optional[str]:
        """Get the mnemonic of the current #1 riser."""
        with get_session() as session:
            max_date_subq = select(func.max(FaultTrendSnapshot.snapshot_date)).scalar_subquery()

            stmt = (
                select(TextDefinition.mnemonic)
                .join(ConditionDefinition, TextDefinition.text_def_id == ConditionDefinition.text_def_id)
                .join(FaultTrendSnapshot, ConditionDefinition.condition_def_id == FaultTrendSnapshot.condition_def_id)
                .where(
                    and_(
                        FaultTrendSnapshot.rank_position == 1,
                        FaultTrendSnapshot.snapshot_date == max_date_subq
                    )
                )
            )
            return session.execute(stmt).scalar_one_or_none()
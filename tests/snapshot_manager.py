# Add to fault_analyzer.py or create new file: snapshot_manager.py

import pandas as pd
from datetime import datetime
from data.repositories.repository import InterlockRepository


class TrendSnapshotManager:
    """Manage periodic snapshots of fault trends for historical tracking."""

    def __init__(self):
        self.repo = InterlockRepository()

    def save_snapshot(self, analyzer, days_recent=7, days_previous=30,
                      snapshot_date=None):
        """
        Save current top risers analysis to database for historical tracking.

        Args:
            analyzer: PatternAnalyzer instance
            days_recent: Recent period in days
            days_previous: Previous period in days
            snapshot_date: Date to record (defaults to today)
        """
        if snapshot_date is None:
            snapshot_date = datetime.now().date()

        # Get current top risers
        result = analyzer.top_risers(
            days_recent=days_recent,
            days_previous=days_previous,
            top_n=50,  # Store top 50 for comprehensive tracking
            min_recent_count=3
        )

        if result.empty:
            print(f"No risers to snapshot for {snapshot_date}")
            return

        # Prepare data for insertion
        records = []
        for idx, row in result.iterrows():
            records.append({
                'snapshot_date': snapshot_date,
                'condition_mnemonic': row['Condition'][:500],  # Truncate if needed
                'days_recent': days_recent,
                'days_previous': days_previous,
                'recent_daily_avg': row['Recent_Daily_Avg'],
                'previous_daily_avg': row['Previous_Daily_Avg'],
                'change_percent': None if row['Change_%'] == 'NEW' else row['Change_%'],
                'absolute_change': row['Absolute_Change'],
                'recent_count': row['Recent_Count'],
                'previous_count': row['Previous_Count'],
                'confidence_score': row['Confidence_Score'],
                'rank_position': row['Rank']
            })

        # Insert into database
        self._insert_snapshots(records)
        print(f"✓ Saved {len(records)} fault trends for {snapshot_date}")

    def _insert_snapshots(self, records):
        """Insert snapshot records into database."""
        query = """
                INSERT INTO fault_trend_snapshots
                (snapshot_date, condition_mnemonic, days_recent, days_previous,
                 recent_daily_avg, previous_daily_avg, change_percent, absolute_change,
                 recent_count, previous_count, confidence_score, rank_position)
                VALUES (%(snapshot_date)s, %(condition_mnemonic)s, %(days_recent)s,
                        %(days_previous)s, %(recent_daily_avg)s, %(previous_daily_avg)s,
                        %(change_percent)s, %(absolute_change)s, %(recent_count)s,
                        %(previous_count)s, %(confidence_score)s, %(rank_position)s) ON DUPLICATE KEY \
                UPDATE \
                    recent_daily_avg = \
                VALUES (recent_daily_avg), previous_daily_avg = \
                VALUES (previous_daily_avg), change_percent = \
                VALUES (change_percent), absolute_change = \
                VALUES (absolute_change), recent_count = \
                VALUES (recent_count), previous_count = \
                VALUES (previous_count), confidence_score = \
                VALUES (confidence_score), rank_position = \
                VALUES (rank_position) \
                """

        with self.repo.get_connection() as conn:
            with conn.cursor() as cursor:
                cursor.executemany(query, records)
            conn.commit()

    def get_fault_trend_history(self, condition_mnemonic, limit=12):
        """
        Get historical trend for a specific fault (last N snapshots).

        Args:
            condition_mnemonic The fault to track
            limit: Number of snapshots to retrieve (default 12 = ~3 months weekly)

        Returns:
            DataFrame with historical trend
        """
        query = """
                SELECT snapshot_date, \
                       recent_daily_avg, \
                       previous_daily_avg, \
                       change_percent, \
                       confidence_score, \
                       rank_position
                FROM fault_trend_snapshots
                WHERE condition_mnemonic = %s
                  AND days_recent = 7 \
                  AND days_previous = 30
                ORDER BY snapshot_date DESC
                    LIMIT %s \
                """

        with self.repo.get_connection() as conn:
            df = pd.read_sql(query, conn, params=(condition_mnemonic, limit))

        return df.sort_values('snapshot_date')  # Ascending for plotting

    def compare_snapshots(self, date1, date2, top_n=10):
        """
        Compare top risers between two snapshot dates.

        Args:
            date1: First date (e.g., '2024-12-01')
            date2: Second date (e.g., '2024-12-10')
            top_n: Number of top faults to compare
        """
        query = """
                SELECT snapshot_date, \
                       condition_mnemonic, \
                       recent_daily_avg, \
                       change_percent, \
                       confidence_score, \
                       rank_position
                FROM fault_trend_snapshots
                WHERE snapshot_date IN (%s, %s)
                  AND days_recent = 7 \
                  AND days_previous = 30
                  AND rank_position <= %s
                ORDER BY snapshot_date, rank_position \
                """

        with self.repo.get_connection() as conn:
            df = pd.read_sql(query, conn, params=(date1, date2, top_n))

        # Pivot for easy comparison
        comparison = df.pivot_table(
            index='condition_mnemonic',
            columns='snapshot_date',
            values=['recent_daily_avg', 'rank_position'],
            aggfunc='first'
        )

        return comparison

    def get_stabilizing_faults(self, weeks_back=4, threshold_decrease=-20):
        """
        Find faults that are decreasing/stabilizing over recent weeks.

        Args:
            weeks_back: How many weeks to look back
            threshold_decrease: % decrease to consider "stabilizing" (e.g., -20%)

        Returns:
            DataFrame of stabilizing faults
        """
        query = """
                WITH recent_trends AS (SELECT condition_mnemonic, \
                                              snapshot_date, \
                                              recent_daily_avg, \
                                              ROW_NUMBER() OVER (PARTITION BY condition_mnemonic ORDER BY snapshot_date DESC) as rn \
                                       FROM fault_trend_snapshots \
                                       WHERE snapshot_date >= DATE_SUB(CURDATE(), INTERVAL %s WEEK) \
                                         AND days_recent = 7 \
                                         AND days_previous = 30),
                     first_last AS (SELECT condition_mnemonic, \
                                           MAX(CASE WHEN rn = 1 THEN recent_daily_avg END) as latest_avg, \
                                           MAX(CASE \
                                                   WHEN rn = (SELECT MAX(rn) \
                                                              FROM recent_trends rt2 \
                                                              WHERE rt2.condition_mnemonic = recent_trends.condition_mnemonic) \
                                                       THEN recent_daily_avg END)          as earliest_avg \
                                    FROM recent_trends \
                                    GROUP BY condition_mnemonic)
                SELECT condition_mnemonic, \
                       earliest_avg, \
                       latest_avg, \
                       ROUND(((latest_avg - earliest_avg) / earliest_avg) * 100, 1) as trend_change_percent
                FROM first_last
                WHERE earliest_avg > 0
                  AND ((latest_avg - earliest_avg) / earliest_avg) * 100 <= %s
                ORDER BY trend_change_percent ASC \
                """

        with self.repo.get_connection() as conn:
            df = pd.read_sql(query, conn, params=(weeks_back, threshold_decrease))

        return df


# Usage in main.py or scheduled job
def weekly_snapshot_job():
    """Run this weekly (e.g., every Monday) to track trends."""
    from ml_pipeline import load_interlock_data, prepare_features
    from fault_analyzer import PatternAnalyzer

    # Load data
    df = load_interlock_data(top_n=100000)
    df = prepare_features(df)

    # Analyze
    analyzer = PatternAnalyzer(df, root_cause_only=True)

    # Save snapshot
    snapshot_mgr = TrendSnapshotManager()
    snapshot_mgr.save_snapshot(analyzer, days_recent=7, days_previous=30)

    print("Weekly snapshot completed!")


# Analysis examples
def analyze_trends():
    """Example: Analyze stabilizing/rising trends."""
    snapshot_mgr = TrendSnapshotManager()

    # Find stabilizing faults
    print("\n=== Stabilizing Faults (last 4 weeks) ===")
    stabilizing = snapshot_mgr.get_stabilizing_faults(weeks_back=4, threshold_decrease=-20)
    print(stabilizing)

    # Track specific fault over time
    print("\n=== Trend for Specific Fault ===")
    history = snapshot_mgr.get_fault_trend_history(
        "Geen aanvraag stop algemeen",
        limit=12
    )
    print(history)

    # Compare two dates
    print("\n=== Compare Two Weeks ===")
    comparison = snapshot_mgr.compare_snapshots('2024-12-01', '2024-12-10')
    print(comparison)
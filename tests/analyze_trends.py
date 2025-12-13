#!/usr/bin/env python3
"""
Trend Analysis Script
Analyze historical fault trends from snapshots.
"""

from snapshot_manager import TrendSnapshotManager
import pandas as pd
from datetime import datetime, timedelta


def print_section(title):
    print(f"\n{'=' * 80}")
    print(f"{title:^80}")
    print(f"{'=' * 80}\n")


def main():
    snapshot_mgr = TrendSnapshotManager()

    # === 1. Stabilizing Faults ===
    print_section("STABILIZING FAULTS (Last 4 Weeks)")
    stabilizing = snapshot_mgr.get_stabilizing_faults(weeks_back=4, threshold_decrease=-20)

    if not stabilizing.empty:
        print("Faults that are DECREASING in frequency:")
        print(stabilizing.to_string(index=False))
        print(f"\n✓ Found {len(stabilizing)} stabilizing faults")
    else:
        print("No stabilizing faults found (all faults are stable or increasing)")

    # === 2. Compare Recent Weeks ===
    print_section("WEEK-OVER-WEEK COMPARISON")

    # Get last two snapshot dates
    query = """
            SELECT DISTINCT snapshot_date
            FROM fault_trend_snapshots
            ORDER BY snapshot_date DESC LIMIT 2 \
            """
    with snapshot_mgr.repo.get_connection() as conn:
        dates_df = pd.read_sql(query, conn)

    if len(dates_df) >= 2:
        date1 = dates_df.iloc[1]['snapshot_date']  # Older
        date2 = dates_df.iloc[0]['snapshot_date']  # Newer

        print(f"Comparing: {date1} vs {date2}")
        comparison = snapshot_mgr.compare_snapshots(date1, date2, top_n=15)
        print("\nTop 15 Faults Comparison:")
        print(comparison)
    else:
        print("Need at least 2 snapshots for comparison. Run weekly snapshot script first.")

    # === 3. Track Specific Fault Over Time ===
    print_section("FAULT HISTORY - TOP RISING FAULT")

    # Get the current #1 riser
    query = """
            SELECT condition_message
            FROM fault_trend_snapshots
            WHERE rank_position = 1
            ORDER BY snapshot_date DESC LIMIT 1 \
            """
    with snapshot_mgr.repo.get_connection() as conn:
        top_fault = pd.read_sql(query, conn)

    if not top_fault.empty:
        fault_name = top_fault.iloc[0]['condition_message']
        print(f"Tracking: {fault_name[:80]}...")

        history = snapshot_mgr.get_fault_trend_history(fault_name, limit=12)

        if not history.empty:
            print("\nLast 12 Snapshots:")
            print(history[['snapshot_date', 'recent_daily_avg', 'change_percent',
                           'confidence_score', 'rank_position']].to_string(index=False))

            # Show trend
            first_avg = history.iloc[0]['recent_daily_avg']
            last_avg = history.iloc[-1]['recent_daily_avg']
            trend_pct = ((last_avg - first_avg) / first_avg * 100) if first_avg > 0 else 0

            print(f"\nOverall Trend: {first_avg:.2f} → {last_avg:.2f} ({trend_pct:+.1f}%)")

            if trend_pct > 20:
                print("⚠️  WARNING: Fault is ACCELERATING")
            elif trend_pct < -20:
                print("✓ GOOD: Fault is DECREASING")
            else:
                print("→ Fault is relatively STABLE")
        else:
            print("No historical data found for this fault")
    else:
        print("No snapshots found. Run weekly snapshot script first.")

    # === 4. Management Summary ===
    print_section("MANAGEMENT SUMMARY")

    query = """
            SELECT COUNT(DISTINCT condition_message) as total_tracked_faults, \
                   MAX(snapshot_date)                as latest_snapshot, \
                   MIN(snapshot_date)                as first_snapshot, \
                   COUNT(DISTINCT snapshot_date)     as total_snapshots
            FROM fault_trend_snapshots \
            """
    with snapshot_mgr.repo.get_connection() as conn:
        summary = pd.read_sql(query, conn)

    if not summary.empty:
        s = summary.iloc[0]
        print(f"Total Faults Tracked: {s['total_tracked_faults']}")
        print(f"Snapshot History: {s['first_snapshot']} to {s['latest_snapshot']}")
        print(f"Total Snapshots: {s['total_snapshots']}")

        # Get current top 5 risers
        query_top5 = """
                     SELECT condition_message, recent_daily_avg, change_percent, confidence_score
                     FROM fault_trend_snapshots
                     WHERE snapshot_date = (SELECT MAX(snapshot_date) FROM fault_trend_snapshots)
                       AND rank_position <= 5
                     ORDER BY rank_position \
                     """
        with snapshot_mgr.repo.get_connection() as conn:
            top5 = pd.read_sql(query_top5, conn)

        print("\nCurrent Top 5 Rising Faults:")
        print(top5.to_string(index=False))
    else:
        print("No snapshot data available yet.")

    print("\n" + "=" * 80 + "\n")


if __name__ == "__main__":
    main()
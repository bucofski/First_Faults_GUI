#!/usr/bin/env python3
"""
Trend Analysis Script - SQLAlchemy 2.0 Version
"""

from snapshot_manager import TrendSnapshotManager


def print_section(title):
    print(f"\n{'=' * 80}")
    print(f"{title:^80}")
    print(f"{'=' * 80}\n")


def main():
    # Configure your connection string
    connection_string = "mssql+pyodbc://user:pass@server/First_Fault?driver=ODBC+Driver+17+for+SQL+Server"
    snapshot_mgr = TrendSnapshotManager(connection_string)

    # === 1. Stabilizing Faults ===
    print_section("STABILIZING FAULTS (Last 4 Weeks)")
    # Note: get_stabilizing_faults would need similar SQLAlchemy conversion
    # For complex CTEs, you might still use text() or keep raw SQL
    print("(Implement with SQLAlchemy CTE if needed)")

    # === 2. Compare Recent Weeks ===
    print_section("WEEK-OVER-WEEK COMPARISON")

    dates = snapshot_mgr.get_latest_snapshot_dates(limit=2)

    if len(dates) >= 2:
        date1, date2 = dates[1], dates[0]  # Older, Newer
        print(f"Comparing: {date1} vs {date2}")
        comparison = snapshot_mgr.compare_snapshots(date1, date2, top_n=15)
        print("\nTop 15 Faults Comparison:")
        print(comparison)
    else:
        print("Need at least 2 snapshots for comparison.")

    # === 3. Track Specific Fault Over Time ===
    print_section("FAULT HISTORY - TOP RISING FAULT")

    fault_name = snapshot_mgr.get_top_riser_mnemonic()

    if fault_name:
        print(f"Tracking: {fault_name[:80]}...")
        history = snapshot_mgr.get_fault_trend_history(fault_name, limit=12)

        if not history.empty:
            print("\nLast 12 Snapshots:")
            print(history.to_string(index=False))

            first_avg = float(history.iloc[0]['recent_daily_avg'])
            last_avg = float(history.iloc[-1]['recent_daily_avg'])
            trend_pct = ((last_avg - first_avg) / first_avg * 100) if first_avg > 0 else 0

            print(f"\nOverall Trend: {first_avg:.2f} → {last_avg:.2f} ({trend_pct:+.1f}%)")
        else:
            print("No historical data found")
    else:
        print("No snapshots found.")

    # === 4. Management Summary ===
    print_section("MANAGEMENT SUMMARY")

    summary = snapshot_mgr.get_summary_stats()

    if summary['total_tracked_faults'] > 0:
        print(f"Total Faults Tracked: {summary['total_tracked_faults']}")
        print(f"Snapshot History: {summary['first_snapshot']} to {summary['latest_snapshot']}")
        print(f"Total Snapshots: {summary['total_snapshots']}")

        top5 = snapshot_mgr.get_top_risers(top_n=5)
        print("\nCurrent Top 5 Rising Faults:")
        print(top5.to_string(index=False))
    else:
        print("No snapshot data available yet.")

    print("\n" + "=" * 80 + "\n")


if __name__ == "__main__":
    main()
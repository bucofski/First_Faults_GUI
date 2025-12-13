#"""
#Weekly Snapshot Script
#Run this every Monday (or any schedule you prefer) to track fault trends over time.

#Schedule with cron (Linux/Mac):
#    0 9 * * 1 /path/to/python /path/to/run_weekly_snapshot.py

#Schedule with Task Scheduler (Windows):
#    - Create Basic Task
#    - Set weekly trigger for Monday 9:00 AM
#    - Action: Start a program
#    - Program: python.exe
#    - Arguments: C:\path\to\run_weekly_snapshot.py
#"""

from ml_pipeline import load_interlock_data, prepare_features
from fault_analyzer import PatternAnalyzer
from snapshot_manager import TrendSnapshotManager
from datetime import datetime


def main():
    print(f"\n{'=' * 60}")
    print(f"Running Weekly Snapshot - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"{'=' * 60}\n")

    try:
        # Load data
        print("Loading interlock data...")
        df = load_interlock_data(top_n=100000)
        df = prepare_features(df)
        print(f"✓ Loaded {len(df)} records")

        # Analyze (root causes only)
        print("\nAnalyzing fault patterns...")
        analyzer = PatternAnalyzer(df, root_cause_only=True)

        # Save snapshot
        print("\nSaving snapshot to database...")
        snapshot_mgr = TrendSnapshotManager()
        snapshot_mgr.save_snapshot(
            analyzer,
            days_recent=7,
            days_previous=30
        )

        # Show summary
        print("\n" + "=" * 60)
        print("SNAPSHOT SUMMARY")
        print("=" * 60)

        result = analyzer.top_risers_with_context(days_recent=7, days_previous=30, top_n=10)
        print(f"\nPeriods analyzed:")
        print(f"  Recent:   {result['analysis_period']['recent']}")
        print(f"  Previous: {result['analysis_period']['previous']}")
        print(f"\nTop 10 Rising Faults:")
        print(result['risers_df'][['Rank', 'Condition', 'Recent_Count', 'Change_%', 'Confidence_Score']].to_string(
            index=False))

        print("\n" + "=" * 60)
        print("✓ Weekly snapshot completed successfully!")
        print("=" * 60 + "\n")

    except Exception as e:
        print(f"\n❌ ERROR: {str(e)}")
        import traceback
        traceback.print_exc()
        raise


if __name__ == "__main__":
    main()
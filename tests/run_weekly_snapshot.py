"""
Weekly Snapshot Script
======================
Runs the fault trend analysis and saves the results to the database.

Schedule with cron (Linux/Mac):
    0 9 * * 1 /path/to/python /path/to/run_weekly_snapshot.py

Schedule with Task Scheduler (Windows):
    Program : python.exe
    Arguments: C:\\path\\to\\run_weekly_snapshot.py
"""

from datetime import datetime

from business.services.reporting_service import ReportingService
from data.orm.trend_orm import init_trend_tables


def main() -> None:
    print(f"\n{'=' * 60}")
    print(f"Weekly Snapshot — {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"{'=' * 60}\n")

    # Ensure trend tables exist
    init_trend_tables()

    service = ReportingService()

    print("Loading data and running analysis...")
    result = service.analyze_and_save(days_recent=7, days_previous=30, top_n=50)

    print(f"\nAnalysis period:")
    print(f"  Recent   : {result.period_recent}")
    print(f"  Previous : {result.period_previous}")
    print(f"  Faults (recent / previous): {result.total_faults_recent} / {result.total_faults_previous}")

    print(f"\nTop 10 Rising Faults:")
    header = f"  {'Rank':>4}  {'Condition':<30}  {'PLC':<15}  {'Change':>8}  {'Confidence':>10}"
    print(header)
    print("  " + "-" * (len(header) - 2))
    for r in result.top_risers[:10]:
        change = f"{r.change_percent}%" if r.change_percent != "NEW" else "NEW"
        print(f"  {r.rank:>4}  {r.condition:<30}  {r.plc:<15}  {change:>8}  {r.confidence_score:>10.1f}")

    print(f"\n{'=' * 60}")
    print("Snapshot completed successfully.")
    print(f"{'=' * 60}\n")


if __name__ == "__main__":
    main()
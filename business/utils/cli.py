"""Command-line interface for interlock analysis."""

import json
from datetime import datetime

from business.services.analyzer import InterlockAnalyzer
from business.core.formatters import DictionaryResultFormatter, ConsoleResultFormatter



def main(
    target_bsid: int | None = None,
    top_n: int | None = None,
    filter_date: datetime | None = None,
    filter_timestamp_start: datetime | None = None,
    filter_timestamp_end: datetime | None = None,
    filter_condition_message: str | None = None,
    filter_plc: str | None = None
) -> int:
    """Command-line interface for interlock analysis."""
    try:
        analyzer = InterlockAnalyzer()

        print("Testing SQL Server connection...")
        if not analyzer.test_connection():
            return 1

        start_time = datetime.now()

        df = analyzer.repository.get_interlock_chain(
            target_bsid=target_bsid,
            top_n=top_n,
            filter_date=filter_date,
            filter_timestamp_start=filter_timestamp_start,
            filter_timestamp_end=filter_timestamp_end,
            filter_condition_message=filter_condition_message,
            filter_plc=filter_plc
        )

        if df.empty:
            print("⚠️  No data found")
            return 0

        trees = analyzer.analyze_interlock(target_bsid,
            top_n,
            filter_date )

        # Dictionary output
        dict_formatter = DictionaryResultFormatter()
        results = dict_formatter.format(trees, target_bsid)
        print(json.dumps(results, indent=2, default=str))

        # Console output
        console_formatter = ConsoleResultFormatter()
        console_output = console_formatter.format(trees, target_bsid)
        print(console_output)

        elapsed = datetime.now() - start_time
        print(f"\n✓ Query + processing time: {elapsed}")

        return 0

    except FileNotFoundError as e:
        print(f"❌ Configuration Error: {e}")
        print("Please ensure Connection.yaml exists in the config directory")
        return 1
    except Exception as e:
        print(f"❌ Error: {e}")
        import traceback
        traceback.print_exc()
        return 1
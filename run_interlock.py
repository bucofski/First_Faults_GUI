"""Runner script for interlock analysis."""
from business.utils.cli import main

if __name__ == "__main__":
    exit(main(
        target_bsid=None,              # All interlocks
        top_n=None,                     # Last 10
        filter_date=None,               # Any date
        filter_timestamp_start=None,    # No start time filter
        filter_timestamp_end=None,      # No end time filter
        filter_condition_message=None,  # No message filter
        filter_plc=None                 # Any PLC
    ))
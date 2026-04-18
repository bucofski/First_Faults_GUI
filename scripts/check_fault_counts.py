"""Print yesterday's fault counts per hour and per PLC."""

import pprint
from business.core.fault_count_service import FaultCountService

service = FaultCountService()

try:
    counts = service.get_yesterday_counts()
except RuntimeError as e:
    print(f"ERROR: {e}")
    raise SystemExit(1)

pprint.pprint(counts.to_dict())
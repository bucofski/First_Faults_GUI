# just for testing
import dataclasses
from typing import Optional
from datetime import datetime

@dataclasses.dataclass
class PlcData:
    tag: str
    value: float
    quality: str = "GOOD"
    unit: Optional[str] = None
    timestamp: datetime = datetime.utcnow()


class PLCDataService:
    def __init__(self):
        self.plc_data = [
            PlcData(tag="Tag1", value=1.0),
            PlcData(tag="Tag2", value=2.0),
            PlcData(tag="Tag3", value=3.0),
            PlcData(tag="Tag4", value=4.0),

        ]

    def get_plc_data(self):
        return self.plc_data
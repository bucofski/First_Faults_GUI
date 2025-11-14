# just for testing
import dataclasses
from datetime import datetime
from typing import Optional

from business.core.singleton import Singleton


@dataclasses.dataclass
class PlcData:
    tag: str
    value: float
    quality: str = "GOOD"
    unit: Optional[str] = None
    timestamp: datetime = dataclasses.field(default_factory=datetime.utcnow)


class PLCDataService(metaclass=Singleton):
    def __init__(self):
        self.plc_data = [
            PlcData(tag="Tag1", value=1.0, quality="BAD", unit="cm"),
            PlcData(tag="Tag2", value=23.5, quality="GOOD", unit="bar"),
            PlcData(tag="Tag3", value=98.6, quality="GOOD", unit="°C"),
            PlcData(tag="Tag4", value=240.0, quality="GOOD", unit="V"),
            PlcData(tag="Tag5", value=50.0, quality="GOOD", unit="Hz"),
            PlcData(tag="Tag6", value=15.7, quality="BAD", unit="A"),
            PlcData(tag="Tag7", value=1013.25, quality="GOOD", unit="hPa"),
            PlcData(tag="Tag8", value=85.0, quality="GOOD", unit="%"),
            PlcData(tag="Tag9", value=3.14, quality="GOOD", unit="rad"),
            PlcData(tag="Tag10", value=42.0, quality="BAD", unit="rpm"),
            PlcData(tag="Tag11", value=750.0, quality="GOOD", unit="mbar"),
            PlcData(tag="Tag12", value=25.4, quality="GOOD", unit="mm"),
            PlcData(tag="Tag13", value=1.333, quality="GOOD", unit="kg/m³"),
            PlcData(tag="Tag14", value=9.81, quality="GOOD", unit="m/s²"),
            PlcData(tag="Tag15", value=273.15, quality="GOOD", unit="K"),
            PlcData(tag="Tag16", value=100.0, quality="BAD", unit="kPa"),
            PlcData(tag="Tag17", value=60.0, quality="GOOD", unit="min"),
            PlcData(tag="Tag18", value=1000.0, quality="GOOD", unit="ml"),
            PlcData(tag="Tag19", value=0.5, quality="GOOD", unit="rad/s"),
            PlcData(tag="Tag20", value=220.0, quality="BAD", unit="VAC"),
            PlcData(tag="Tag21", value=24.0, quality="GOOD", unit="VDC"),
            PlcData(tag="Tag22", value=4.0, quality="GOOD", unit="mA"),
            PlcData(tag="Tag23", value=20.0, quality="GOOD", unit="mA"),
            PlcData(tag="Tag24", value=1.0, quality="BAD", unit="bar"),
            PlcData(tag="Tag25", value=30.0, quality="GOOD", unit="m³/h"),
            PlcData(tag="Tag26", value=55.0, quality="GOOD", unit="dB"),
            PlcData(tag="Tag27", value=90.0, quality="GOOD", unit="°"),
            PlcData(tag="Tag28", value=12.0, quality="BAD", unit="h"),
            PlcData(tag="Tag29", value=365.0, quality="GOOD", unit="days"),
            PlcData(tag="Tag30", value=1.5, quality="GOOD", unit="m/s"),
            PlcData(tag="Tag31", value=500.0, quality="GOOD", unit="W"),
            PlcData(tag="Tag32", value=0.95, quality="GOOD", unit="PF"),
            PlcData(tag="Tag33", value=1800.0, quality="BAD", unit="rpm"),
            PlcData(tag="Tag34", value=50.0, quality="GOOD", unit="Nm"),
            PlcData(tag="Tag35", value=7.0, quality="GOOD", unit="pH"),
            PlcData(tag="Tag36", value=150.0, quality="GOOD", unit="psi"),
            PlcData(tag="Tag37", value=2.54, quality="BAD", unit="in"),
            PlcData(tag="Tag38", value=1.0, quality="GOOD", unit="atm"),
            PlcData(tag="Tag39", value=100.0, quality="GOOD", unit="lx"),
            PlcData(tag="Tag40", value=20.0, quality="GOOD", unit="kg"),
            PlcData(tag="Tag41", value=1.0, quality="BAD", unit="ton"),
            PlcData(tag="Tag42", value=2.2, quality="GOOD", unit="lb"),
            PlcData(tag="Tag43", value=1000.0, quality="GOOD", unit="Pa"),
            PlcData(tag="Tag44", value=0.001, quality="GOOD", unit="μm"),
            PlcData(tag="Tag45", value=5.0, quality="BAD", unit="kW"),
            PlcData(tag="Tag46", value=400.0, quality="GOOD", unit="VAC"),
            PlcData(tag="Tag47", value=1.0, quality="GOOD", unit="MW"),
            PlcData(tag="Tag48", value=60.0, quality="GOOD", unit="s"),
            PlcData(tag="Tag49", value=1000.0, quality="BAD", unit="L"),
            PlcData(tag="Tag50", value=100.0, quality="GOOD", unit="%RH")
        ]

    def get_plc_data(self):
        return self.plc_data

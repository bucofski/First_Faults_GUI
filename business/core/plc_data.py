import dataclasses
from datetime import datetime, timezone

@dataclasses.dataclass
class PlcMessage(slots=True, kw_only=True):
    id:float
    error_message: str
    error_nbr: str
    error_type: str
    bs_comment: str
    vw_mnemonic: str
    plc_messages: list[PlcMessage]
    timestamp: datetime = dataclasses.field(
        default_factory=lambda: datetime.now(timezone.utc)
    )

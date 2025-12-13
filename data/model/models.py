"""Domain models for interlock analysis."""

from dataclasses import dataclass, field
from typing import Any


@dataclass
class InterlockCondition:
    """Represents a single condition within an interlock."""
    type: str
    bit_index: int
    message: str

    def to_dict(self) -> dict[str, Any]:
        return {
            "type": self.type,
            "bit_index": self.bit_index,
            "message": self.message
        }


@dataclass
class InterlockNode:
    """Represents a node in the interlock chain hierarchy."""
    level: int
    interlock_log_id: int
    bsid: str | None
    plc: str | None
    direction: str | None
    timestamp: str | None
    condition_mnemonic: str | None
    interlock_message: str | None
    status: str | None
    conditions: list[InterlockCondition] = field(default_factory=list)
    children: list["InterlockNode"] = field(default_factory=list)

    def to_dict(self) -> list[dict[str, int | str | None | list[dict[str, Any]]] | list[dict[str, Any]]]:
        """Convert node and its children to dictionary representation."""
        result = {
            "level": self.level,
            "interlock_log_id": self.interlock_log_id,
            "bsid": self.bsid,
            "plc": self.plc,
            "direction": self.direction,
            "timestamp": self.timestamp,
            "condition_mnemonic": self.condition_mnemonic,
            "interlock_message": self.interlock_message,
            "status": self.status,
            "conditions": [cond.to_dict() for cond in self.conditions],
        }
        return [result, [child.to_dict() for child in self.children]]

    def add_child(self, child: "InterlockNode") -> None:
        """Add a child node to this node."""
        self.children.append(child)
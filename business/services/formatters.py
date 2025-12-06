"""Result formatters for interlock analysis output."""

from abc import ABC, abstractmethod
from typing import Any

from .models import InterlockNode


class ResultFormatter(ABC):
    """Abstract base class for result formatting."""

    @abstractmethod
    def format(self, trees: list[InterlockNode], interlock_number: int) -> Any:
        """Format results for output."""
        pass


class DictionaryResultFormatter(ResultFormatter):
    """Formats results as hierarchical dictionary/JSON structure."""

    def format(self, trees: list[InterlockNode], interlock_number: int) -> list:
        """Convert interlock trees to dictionary representation."""
        return [tree.to_dict() for tree in trees]


class ConsoleResultFormatter(ResultFormatter):
    """Formats results for console output."""

    def format(self, trees: list[InterlockNode], interlock_number: int) -> str:
        """Format results as readable console output."""
        output = [f"\n{'=' * 80}", f"Root Cause Analysis for Interlock {interlock_number}", f"{'=' * 80}\n"]

        if not trees:
            output.append(f"⚠️  No data found for interlock {interlock_number}")
            return "\n".join(output)

        for idx, tree in enumerate(trees, 1):
            output.append(f"Chain {idx}:")
            output.append(self._format_node(tree, indent=0))
            output.append("")

        return "\n".join(output)

    def _format_node(self, node: InterlockNode, indent: int = 0) -> str:
        """Recursively format a node and its children."""
        prefix = "  " * indent
        lines = [
            f"{prefix}Level {node.level}:",
            f"{prefix}  ID: {node.interlock_log_id}",
            f"{prefix}  BSID: {node.bsid}",
            f"{prefix}  Message: {node.interlock_message}",
            f"{prefix}  Status: {node.status}",
        ]

        if node.conditions:
            lines.append(f"{prefix}  Conditions:")
            for cond in node.conditions:
                lines.append(f"{prefix}    - {cond.message}")

        for child in node.children:
            lines.append(self._format_node(child, indent + 1))

        return "\n".join(lines)
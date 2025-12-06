"""
Interlock Chain Analyzer - Refactored Version

A modular system for analyzing interlock chains with hierarchical root cause tracing.
Follows SOLID principles for maintainability and extensibility.
"""

from dataclasses import dataclass, field
from abc import ABC, abstractmethod
from datetime import datetime
from typing import Any
import json

import pandas as pd
from sqlalchemy import func, select

from DB_Connection import get_session


# ============================================================================
# Domain Models
# ============================================================================

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
            "interlock_message": self.interlock_message,
            "status": self.status,
            "conditions": [cond.to_dict() for cond in self.conditions],
        }
        return [result, [child.to_dict() for child in self.children]]

    def add_child(self, child: "InterlockNode") -> None:
        """Add a child node to this node."""
        self.children.append(child)


# ============================================================================
# Tree Builder Service
# ============================================================================

class InterlockTreeBuilder:
    """Service for building hierarchical interlock trees from flat data."""

    @staticmethod
    def build_from_dataframe(df: pd.DataFrame) -> list[InterlockNode]:
        """
        Convert flat DataFrame into nested hierarchical structure.
        """
        if df.empty:
            return []

        trees = []

        for date, date_group in df.groupby("Date"):
            chains = InterlockTreeBuilder._extract_chains(date_group)
            for chain_df in chains:
                root = InterlockTreeBuilder._build_chain_tree(chain_df)
                if root:
                    trees.append(root)

        return trees

    @staticmethod
    def _extract_chains(date_df: pd.DataFrame) -> list[pd.DataFrame]:
        """Extract individual chains from a date group."""
        chains = []
        anchor_rows = date_df[date_df["Level"] == 0]

        for _, anchor in anchor_rows.drop_duplicates(subset=["Interlock_Log_ID"]).iterrows():
            timestamp = anchor["TIMESTAMP"]
            chain = date_df[date_df["TIMESTAMP"] == timestamp]
            chains.append(chain)

        return chains

    @staticmethod
    def _build_chain_tree(chain_df: pd.DataFrame) -> InterlockNode | None:
        """Build a tree from a single chain DataFrame."""
        levels = sorted(chain_df["Level"].unique(), reverse=False)

        if not levels:
            return None

        root_node = None
        current_parent = None

        for level in levels:
            node = InterlockTreeBuilder._create_node_from_level(chain_df, level)

            if root_node is None:
                root_node = node
                current_parent = node
            else:
                current_parent.add_child(node)
                current_parent = node

        return root_node

    @staticmethod
    def _create_node_from_level(chain_df: pd.DataFrame, level: int) -> InterlockNode:
        """Create a single node from level data."""
        level_data = chain_df[chain_df["Level"] == level]
        first_row = level_data.iloc[0]

        conditions = InterlockTreeBuilder._extract_conditions(level_data)

        return InterlockNode(
            level=int(level),
            interlock_log_id=int(first_row["Interlock_Log_ID"]),
            bsid=first_row.get("BSID"),
            plc=InterlockTreeBuilder._clean_plc(first_row.get("PLC")),
            direction=first_row.get("Direction"),
            timestamp=InterlockTreeBuilder._format_timestamp(first_row.get("TIMESTAMP")),
            interlock_message=first_row.get("Interlock_Message"),
            status=first_row.get("Status"),
            conditions=conditions
        )

    @staticmethod
    def _extract_conditions(level_data: pd.DataFrame) -> list[InterlockCondition]:
        """Extract conditions from level data."""
        conditions = []
        for _, row in level_data.iterrows():
            if pd.notna(row.get("Condition_Message")):
                conditions.append(InterlockCondition(
                    type=row["TYPE"],
                    bit_index=row["BIT_INDEX"],
                    message=row["Condition_Message"]
                ))
        return conditions

    @staticmethod
    def _clean_plc(plc: Any) -> str | None:
        """Clean and format PLC value."""
        if plc is None:
            return None
        return str(plc).strip() or None

    @staticmethod
    def _format_timestamp(timestamp: Any) -> str | None:
        """Format timestamp value."""
        if pd.notna(timestamp):
            return str(timestamp)
        return None


# ============================================================================
# Repository
# ============================================================================

class InterlockRepository:
    """Repository for interlock data access using session context manager."""

    TVF_COLUMNS = (
        "Date", "TIMESTAMP", "Level", "Interlock_Log_ID", "BSID",
        "PLC", "Direction", "Interlock_Message", "Status",
        "TYPE", "BIT_INDEX", "Condition_Message"
    )

    def get_interlock_chain(
        self,
        target_bsid: int | None = None,
        top_n: int | None = None,
        filter_date: datetime | None = None,
        filter_timestamp_start: datetime | None = None,
        filter_timestamp_end: datetime | None = None,
        filter_condition_message: str | None = None,
        filter_plc: str | None = None
    ) -> pd.DataFrame:
        """
        Retrieve interlock chain data with upstream/downstream tracing.

        Args:
            target_bsid: Optional BSID. If NULL, returns last interlocks with their full trees
            top_n: Number of results to return (default in SQL: 10). If None, uses SQL default
            filter_date: Optional filter by specific date
            filter_timestamp_start: Optional filter by timestamp range start
            filter_timestamp_end: Optional filter by timestamp range end
            filter_condition_message: Optional search text in condition message
            filter_plc: Optional filter by PLC name

        Returns:
            DataFrame with interlock chain data
        """
        interlock_func = func.dbo.fn_InterlockChain(
            target_bsid,
            top_n,
            filter_date,
            filter_timestamp_start,
            filter_timestamp_end,
            filter_condition_message,
            filter_plc
        ).table_valued(*self.TVF_COLUMNS)

        stmt = (
            select(interlock_func)
            .order_by(
                interlock_func.c.TIMESTAMP.desc(),
                interlock_func.c.Date.desc(),
                interlock_func.c.Level
            )
        )

        with get_session() as session:
            result = session.execute(stmt)
            return pd.DataFrame(result.fetchall(), columns=result.keys())

    def test_connection(self) -> bool:
        """Test database connection."""
        try:
            with get_session() as session:
                result = session.execute(select(func.db_name().label("CurrentDatabase")))
                row = result.fetchone()
                print(f"✓ Connection successful! Database: {row.CurrentDatabase}")
                return True
        except Exception as e:
            print(f"❌ Connection failed: {e}")
            return False


# ============================================================================
# Result Formatters
# ============================================================================

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


# ============================================================================
# Main Analyzer
# ============================================================================

@dataclass
class InterlockAnalyzer:
    """Main analyzer orchestrating the interlock analysis workflow."""

    repository: InterlockRepository = field(default_factory=InterlockRepository)
    tree_builder: InterlockTreeBuilder = field(default_factory=InterlockTreeBuilder)

    def test_connection(self) -> bool:
        """Test database connection."""
        return self.repository.test_connection()

    def analyze_interlock(
        self,
        interlock_number: int,
        limit: int = 1,
        formatter: ResultFormatter | None = None
    ) -> Any:
        """Perform complete interlock analysis."""
        if formatter is None:
            formatter = DictionaryResultFormatter()

        df = self.repository.get_interlock_chain(interlock_number, limit)

        if df.empty:
            print(f"⚠️  No data found for interlock {interlock_number}")
            return [] if isinstance(formatter, DictionaryResultFormatter) else ""

        trees = self.tree_builder.build_from_dataframe(df)
        return formatter.format(trees, interlock_number)


# ============================================================================
# CLI Interface
# ============================================================================

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

        # Test connection (this also warms up the connection pool)
        print("Testing SQL Server connection...")
        if not analyzer.test_connection():
            return 1

        # Start timing after connection is established
        start_time = datetime.now()

        # Fetch data
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
            print(f"⚠️  No data found")
            return 0

        # Build trees once, format twice
        trees = analyzer.tree_builder.build_from_dataframe(df)

        # Dictionary output
        dict_formatter = DictionaryResultFormatter()
        results = dict_formatter.format(trees, target_bsid)
        print(json.dumps(results, indent=2, default=str))

        # Console output (reuses same trees)
        console_formatter = ConsoleResultFormatter()
        console_output = console_formatter.format(trees, target_bsid)
        print(console_output)

        # Execution time
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


if __name__ == "__main__":
    # Example usage with various filters
    exit(main(
        target_bsid=None,           # All interlocks
        top_n=None,                    # Last 10
        filter_date=None,            # Any date
        filter_timestamp_start=None, # No start time filter
        filter_timestamp_end=None,   # No end time filter
        filter_condition_message=None,  # No message filter
        filter_plc=None              # Any PLC
    ))
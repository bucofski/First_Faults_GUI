"""Service for building hierarchical interlock trees."""

from typing import Any
import pandas as pd
from data.model.models import InterlockCondition, InterlockNode


class InterlockTreeBuilder:
    """Service for building hierarchical interlock trees from flat data."""

    @staticmethod
    def build_from_dataframe(df: pd.DataFrame) -> list[InterlockNode]:
        """Convert flat DataFrame into nested hierarchical structure."""
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

        condition_mnemonic = first_row.get("Condition_Mnemonic")
        if pd.isna(condition_mnemonic) or str(condition_mnemonic).strip() == "":
            condition_mnemonic = first_row.get("Condition_Message")

        conditions = InterlockTreeBuilder._extract_conditions(level_data)

        return InterlockNode(
            level=int(level),
            interlock_log_id=int(first_row["Interlock_Log_ID"]),
            bsid=first_row.get("BSID"),
            plc=InterlockTreeBuilder._clean_plc(first_row.get("PLC")),
            direction=first_row.get("Direction"),
            timestamp=InterlockTreeBuilder._format_timestamp(first_row.get("TIMESTAMP")),
            condition_mnemonic=condition_mnemonic,
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
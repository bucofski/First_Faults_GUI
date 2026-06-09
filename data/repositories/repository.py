"""Repository for interlock data access."""

import logging
from datetime import datetime

import pandas as pd
from sqlalchemy import func, select

from data.repositories.DB_Connection import get_session

logger = logging.getLogger(__name__)

# SQL's TOP has no "unlimited" keyword and fn_InterlockChain's @TopN is an INT,
# so the maximum INT value is used to effectively remove the row cap.
_UNLIMITED_TOP_N = 2_000


def test_connection() -> bool:
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


class InterlockRepository:
    """Repository for interlock data access using session context manager."""

    TVF_COLUMNS = (
        "AnchorReference", "AnchorTimestamp", "Date", "Level", "Direction", "Interlock_Log_ID",
        "TIMESTAMP", "PLC", "BSID", "Interlock_Message",
        "TYPE", "BIT_INDEX", "Condition_Mnemonic", "Condition_Message",
        "UPSTREAM_INTERLOCK_REF", "Status"
    )

    def get_interlock_chain(
        self,
        target_bsid: int | None = None,
        top_n: int | None = None,
        filter_timestamp_start: datetime | None = None,
        filter_timestamp_end: datetime | None = None,
        filter_condition_message: str | None = None,
        filter_plc: str | None = None
    ) -> pd.DataFrame:
        """
        Retrieve interlock chain data with upstream/downstream tracing.

        Args:
            target_bsid: Optional BSID. If NULL, returns last interlocks with their full trees
            top_n: Number of anchor interlocks to return. If None, the SQL function
                defaults to 100; when both filter_timestamp_start and
                filter_timestamp_end are set, the cap is lifted to return all
                interlocks within the range
            filter_timestamp_start: Optional filter by timestamp range start
            filter_timestamp_end: Optional filter by timestamp range end
            filter_condition_message: Optional search text in condition message
            filter_plc: Optional filter by PLC name

        Returns:
            DataFrame with interlock chain data
        """
        # A start + end date range already bounds the result set, so drop the default
        # TOP cap and return every interlock within that range. An explicitly
        # provided top_n still takes precedence.
        if top_n is None and filter_timestamp_start is not None and filter_timestamp_end is not None:
            top_n = _UNLIMITED_TOP_N

        interlock_func = func.dbo.fn_InterlockChain(
            target_bsid,
            top_n,
            filter_timestamp_start,
            filter_timestamp_end,
            filter_condition_message,
            filter_plc
        ).table_valued(*self.TVF_COLUMNS)

        # When a start timestamp filter is active, show results ascending (oldest
        # first, starting at the start time and counting up). Without a start filter,
        # keep the default newest-first ordering. The returned rows are identical
        # either way; only the display order changes.
        ascending = filter_timestamp_start is not None
        anchor_timestamp = interlock_func.c.AnchorTimestamp
        anchor_reference = interlock_func.c.AnchorReference

        stmt = (
            select(interlock_func)
            .order_by(
                anchor_timestamp.asc() if ascending else anchor_timestamp.desc(),
                anchor_reference.asc() if ascending else anchor_reference.desc(),
                interlock_func.c.Level.desc(),
            )
            .suffix_with("OPTION (RECOMPILE)")
        )

        with get_session() as session:
            result = session.execute(stmt)
            return pd.DataFrame(result.fetchall(), columns=result.keys())


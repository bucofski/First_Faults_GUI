"""Repository for interlock data access."""

from datetime import datetime

import pandas as pd
from sqlalchemy import func, select

from data.repositories.DB_Connection import get_session


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
            .suffix_with("OPTION (RECOMPILE)")
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
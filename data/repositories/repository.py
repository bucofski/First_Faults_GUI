"""Repository for interlock data access."""

from datetime import datetime

import pandas as pd
from sqlalchemy import func, select

from data.repositories.DB_Connection import get_session


class InterlockRepository:
    """Repository for interlock data access using session context manager."""

    TVF_COLUMNS = (
        "AnchorReference", "Date", "Level", "Direction", "Interlock_Log_ID",
        "TIMESTAMP", "PLC", "BSID", "Interlock_Message",
        "TYPE", "BIT_INDEX", "Condition_Mnemonic", "Condition_Message",
        "UPSTREAM_INTERLOCK_REF", "Status"
    )

    def get_interlock_chain(
        self,
        target_bsid: int | None = None,
        top_n: int | None = None,
        filter_bit_index: int | None = None,
        filter_date: datetime | None = None,
        filter_timestamp_start: datetime | None = None,
        filter_timestamp_end: datetime | None = None,
        filter_condition_mnemonic: str | None = None,
        filter_condition_message: str | None = None,
        filter_plc: str | None = None
    ) -> pd.DataFrame:
        """
        Retrieve interlock chain data with upstream/downstream tracing.
        """
        # ... existing code ...
        # SQL function supports only ONE condition filter parameter that matches message OR mnemonic.
        # If caller provided mnemonic-only, treat it as the same filter.
        condition_filter = filter_condition_message or filter_condition_mnemonic

        interlock_func = func.dbo.fn_InterlockChain(
            target_bsid,
            top_n,
            filter_date,
            filter_timestamp_start,
            filter_timestamp_end,
            condition_filter,
            filter_plc,
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
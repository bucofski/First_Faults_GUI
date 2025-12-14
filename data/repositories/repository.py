"""Repository for interlock data access."""

from datetime import datetime

import pandas as pd
from sqlalchemy import text

from data.repositories.DB_Connection import get_engine


class InterlockRepository:
    """Repository for interlock data access using direct pandas SQL."""

    def __init__(self):
        """Initialize repository with database engine."""
        self.engine = get_engine()

    def get_interlock_chain(
        self,
        target_bsid: int | None = None,
        top_n: int | None = None,
        filter_date: datetime | None = None,
        filter_timestamp_start: datetime | None = None,
        filter_timestamp_end: datetime | None = None,
        filter_condition_message: str | None = None,
        filter_plc: str | None = None,
    ) -> pd.DataFrame:
        """
        Retrieve interlock chain data with upstream/downstream tracing.

        Args:
            target_bsid: Optional BSID to filter by
            top_n: Number of top interlocks to retrieve
            filter_date: Filter by specific date
            filter_timestamp_start: Filter by timestamp range start
            filter_timestamp_end: Filter by timestamp range end
            filter_condition_message: Search text in condition message or mnemonic
            filter_plc: Filter by PLC name

        Returns:
            DataFrame with interlock chain data
        """
        # Build SQL query
        query = """
        SELECT 
            AnchorReference,
            Date,
            Level,
            Direction,
            Interlock_Log_ID,
            TIMESTAMP,
            PLC,
            BSID,
            Interlock_Message,
            TYPE,
            BIT_INDEX,
            Condition_Mnemonic,
            Condition_Message,
            UPSTREAM_INTERLOCK_REF,
            Status
        FROM dbo.fn_InterlockChain(
            :target_bsid, 
            :top_n, 
            :filter_date, 
            :filter_timestamp_start, 
            :filter_timestamp_end, 
            :filter_condition_message, 
            :filter_plc
        )
        ORDER BY TIMESTAMP DESC, Date DESC, Level
        """

        # Prepare parameters
        params = {
            'target_bsid': target_bsid,
            'top_n': top_n,
            'filter_date': filter_date,
            'filter_timestamp_start': filter_timestamp_start,
            'filter_timestamp_end': filter_timestamp_end,
            'filter_condition_message': filter_condition_message,
            'filter_plc': filter_plc,
        }

        try:
            # Use pandas read_sql with timeout
            df = pd.read_sql(
                sql=text(query),
                con=self.engine,
                params=params
            )
            return df

        except Exception as e:
            print(f"❌ Query failed: {e}")
            # Return empty DataFrame with expected columns
            return pd.DataFrame(columns=[
                "AnchorReference", "Date", "Level", "Direction", "Interlock_Log_ID",
                "TIMESTAMP", "PLC", "BSID", "Interlock_Message",
                "TYPE", "BIT_INDEX", "Condition_Mnemonic", "Condition_Message",
                "UPSTREAM_INTERLOCK_REF", "Status"
            ])

    def test_connection(self) -> bool:
        """Test database connection."""
        try:
            query = "SELECT DB_NAME() as CurrentDatabase"
            df = pd.read_sql(query, self.engine)
            db_name = df.iloc[0]['CurrentDatabase']
            print(f"✓ Connection successful! Database: {db_name}")
            return True
        except Exception as e:
            print(f"❌ Connection failed: {e}")
            return False
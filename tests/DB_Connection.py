from sqlalchemy import create_engine, text, String, Integer, DateTime, ForeignKey, URL, event
from sqlalchemy.orm import Session, DeclarativeBase, relationship, Mapped, mapped_column
from typing import Optional, Dict, Any
import pandas as pd
import pyodbc
import yaml
import os

# ============================================================================
# Get base directory of the script
# ============================================================================
base_dir = os.path.dirname(os.path.abspath(__file__))


# ============================================================================
# Database Connection Class
# ============================================================================

class DBConnection:
    def __init__(self):
        # Determine path to Connection.yaml (one level up, then config/)
        base_dir = os.path.dirname(os.path.abspath(__file__))
        config_path = os.path.join(base_dir, "..", "config", "Connection.yaml")

        # Load the YAML file
        with open(config_path, "r") as file:
            config = yaml.safe_load(file)

        # Build connection string
        conn_config = config["DBconnection"]
        self.connection_string = (
            f"DRIVER={{{conn_config['driver']}}};"
            f"SERVER={conn_config['server']},{conn_config['port']};"
            f"DATABASE={conn_config['database']};"
            f"UID={conn_config['username']};"
            f"PWD={conn_config['password']};"
            f"Encrypt={'yes' if conn_config['encrypt'] else 'no'};"
            f"TrustServerCertificate="
            f"{'yes' if conn_config['trust_server_certificate'] else 'no'};"
        )

        self.conn = None
        self.cursor = None

    def connect(self):
        """Open the database connection."""
        if self.conn is None:
            self.conn = pyodbc.connect(self.connection_string)
            self.cursor = self.conn.cursor()

    def close(self):
        """Close cursor and connection."""
        if self.cursor is not None:
            self.cursor.close()
            self.cursor = None
        if self.conn is not None:
            self.conn.close()
            self.conn = None

    def execute(self, query, params=None):
        """Execute a query and return the cursor."""
        if self.conn is None:
            self.connect()
        if params is None:
            self.cursor.execute(query)
        else:
            self.cursor.execute(query, params)
        return self.cursor

    def fetchone(self):
        return self.cursor.fetchone() if self.cursor is not None else None

    def fetchall(self):
        return self.cursor.fetchall() if self.cursor is not None else None

    def commit(self):
        if self.conn is not None:
            self.conn.commit()

    def __enter__(self):
        """Support with-statement usage."""
        self.connect()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()


# ============================================================================
# SQLAlchemy Engine from DBConnection
# ============================================================================

def create_engine_from_dbconnection():
    """
    Create SQLAlchemy engine using the DBConnection configuration.

    Returns:
        SQLAlchemy Engine object
    """
    # Load configuration from Connection.yaml
    config_path = os.path.join(base_dir, "..", "config", "Connection.yaml")

    with open(config_path, "r") as file:
        config = yaml.safe_load(file)

    conn_config = config["DBconnection"]

    # Build SQLAlchemy connection URL
    connection_url = URL.create(
        "mssql+pyodbc",
        username=conn_config['username'],
        password=conn_config['password'],
        host=conn_config['server'],
        port=conn_config.get('port', 1433),
        database=conn_config['database'],
        query={
            "driver": conn_config['driver'],
            "Encrypt": "yes" if conn_config.get('encrypt', False) else "no",
            "TrustServerCertificate": "yes" if conn_config.get('trust_server_certificate', True) else "no",
        }
    )

    # Create engine
    engine = create_engine(
        connection_url,
        echo=False,
        pool_pre_ping=True,
        pool_size=5,
        pool_recycle=3600,
        fast_executemany=True,
    )

    print(f"✓ Engine created for database: {conn_config['database']} on server: {conn_config['server']}")
    print(f"  Authentication: SQL Server (User: {conn_config['username']})")

    return engine


# ============================================================================
# Initialize Engine
# ============================================================================

engine = create_engine_from_dbconnection()


# ============================================================================
# ORM Models (SQLAlchemy 2.0 style for SQL Server)
# ============================================================================

class Base(DeclarativeBase):
    pass

class PLC(Base):
    __tablename__ = 'PLC'
    __table_args__ = {'schema': 'dbo'}

    PLC_ID: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    PLC_CODE: Mapped[str] = mapped_column(String(50), nullable=False)
    PLC_NAME: Mapped[Optional[str]] = mapped_column(String(100))
    DESCRIPTION: Mapped[Optional[str]] = mapped_column(String(255))


class TextDefinition(Base):
    __tablename__ = 'TEXT_DEFINITION'
    __table_args__ = {'schema': 'dbo'}

    TEXT_DEF_ID: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    MNEMONIC: Mapped[str] = mapped_column(String(255), nullable=False)
    MESSAGE: Mapped[str] = mapped_column(String(500), nullable=False)


class InterlockDefinition(Base):
    __tablename__ = 'INTERLOCK_DEFINITION'
    __table_args__ = {'schema': 'dbo'}

    INTERLOCK_DEF_ID: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    PLC_ID: Mapped[int] = mapped_column(Integer, ForeignKey('dbo.PLC.PLC_ID'), nullable=False)
    NUMBER: Mapped[int] = mapped_column(Integer, nullable=False)
    TEXT_DEF_ID: Mapped[int] = mapped_column(Integer, ForeignKey('dbo.TEXT_DEFINITION.TEXT_DEF_ID'), nullable=False)

    plc: Mapped["PLC"] = relationship()
    text_definition: Mapped["TextDefinition"] = relationship()


class ConditionDefinition(Base):
    __tablename__ = 'CONDITION_DEFINITION'
    __table_args__ = {'schema': 'dbo'}

    CONDITION_DEF_ID: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    PLC_ID: Mapped[int] = mapped_column(Integer, ForeignKey('dbo.PLC.PLC_ID'), nullable=False)
    INTERLOCK_NUMBER: Mapped[int] = mapped_column(Integer, nullable=False)
    TYPE: Mapped[int] = mapped_column(Integer, nullable=False)
    BIT_INDEX: Mapped[int] = mapped_column(Integer, nullable=False)
    TEXT_DEF_ID: Mapped[int] = mapped_column(Integer, ForeignKey('dbo.TEXT_DEFINITION.TEXT_DEF_ID'), nullable=False)

    plc: Mapped["PLC"] = relationship()
    text_definition: Mapped["TextDefinition"] = relationship()


class FFInterlockLog(Base):
    __tablename__ = 'FF_INTERLOCK_LOG'
    __table_args__ = {'schema': 'dbo'}

    ID: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    INTERLOCK_DEF_ID: Mapped[int] = mapped_column(Integer, ForeignKey('dbo.INTERLOCK_DEFINITION.INTERLOCK_DEF_ID'),
                                                  nullable=False)
    TIMESTAMP: Mapped[DateTime] = mapped_column(DateTime, nullable=False)
    TIMESTAMP_LOG: Mapped[DateTime] = mapped_column(DateTime, nullable=False)
    ORDER_LOG: Mapped[int] = mapped_column(Integer, nullable=False)
    UPSTREAM_INTERLOCK_LOG_ID: Mapped[Optional[int]] = mapped_column(Integer, ForeignKey('dbo.FF_INTERLOCK_LOG.ID'))

    interlock_definition: Mapped["InterlockDefinition"] = relationship()


class FFConditionLog(Base):
    __tablename__ = 'FF_CONDITION_LOG'
    __table_args__ = {'schema': 'dbo'}

    ID: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    INTERLOCK_LOG_ID: Mapped[int] = mapped_column(Integer, ForeignKey('dbo.FF_INTERLOCK_LOG.ID'), nullable=False)
    CONDITION_DEF_ID: Mapped[int] = mapped_column(Integer, ForeignKey('dbo.CONDITION_DEFINITION.CONDITION_DEF_ID'),
                                                  nullable=False)

    interlock_log: Mapped["FFInterlockLog"] = relationship()
    condition_definition: Mapped["ConditionDefinition"] = relationship()


# ============================================================================
# Query Functions (Using SQLAlchemy)
# ============================================================================

def get_interlock_root_cause_last_n(interlock_number: int, limit: int = 10) -> pd.DataFrame:
    """
    Get the last N occurrences of an interlock with complete upstream chain trace.
    Uses SQLAlchemy engine with DBConnection configuration.

    Args:
        interlock_number: The interlock number to search for (e.g., 11222)
        limit: Number of most recent occurrences to retrieve (default: 10)

    Returns:
        pandas.DataFrame with the results
    """

    query_text = text("""
        SET NOCOUNT ON;

        -- Get the last N occurrences of the specified interlock
        DECLARE @LastNInterlocks TABLE (ID INT, RowNum INT);

        INSERT INTO @LastNInterlocks (ID, RowNum)
        SELECT TOP (:limit)
            il.ID,
            ROW_NUMBER() OVER (ORDER BY il.TIMESTAMP DESC) AS RowNum
        FROM dbo.FF_INTERLOCK_LOG il
        INNER JOIN dbo.INTERLOCK_DEFINITION id ON il.INTERLOCK_DEF_ID = id.INTERLOCK_DEF_ID
        WHERE id.NUMBER = :interlock_number
        ORDER BY il.TIMESTAMP DESC;

        -- Recursive CTE to trace the complete upstream chain for all N occurrences
        WITH UpstreamChain AS (
            -- Anchor: Start with the last N target interlocks
            SELECT 
                il.ID,
                il.INTERLOCK_DEF_ID,
                il.TIMESTAMP,
                il.ORDER_LOG,
                il.UPSTREAM_INTERLOCK_LOG_ID,
                0 AS Level,
                CAST(NULL AS INT) AS ParentInterlockNumber,
                ln.RowNum AS OccurrenceNumber
            FROM dbo.FF_INTERLOCK_LOG il
            INNER JOIN @LastNInterlocks ln ON il.ID = ln.ID

            UNION ALL

            -- Recursive: Find upstream interlocks
            SELECT 
                il.ID,
                il.INTERLOCK_DEF_ID,
                il.TIMESTAMP,
                il.ORDER_LOG,
                il.UPSTREAM_INTERLOCK_LOG_ID,
                uc.Level + 1,
                id_child.NUMBER AS ParentInterlockNumber,
                uc.OccurrenceNumber
            FROM dbo.FF_INTERLOCK_LOG il WITH (NOLOCK)
            INNER JOIN UpstreamChain uc ON il.ID = uc.UPSTREAM_INTERLOCK_LOG_ID
            INNER JOIN dbo.INTERLOCK_DEFINITION id_child ON uc.INTERLOCK_DEF_ID = id_child.INTERLOCK_DEF_ID
            WHERE uc.Level < 100
        )
        SELECT 
            uc.OccurrenceNumber AS OccurrenceNum,
            uc.Level,
            uc.ID AS InterlockLogID,
            id.NUMBER AS InterlockNumber,
            p.PLC_CODE,
            td_interlock.MNEMONIC AS InterlockMnemonic,
            td_interlock.MESSAGE AS InterlockMessage,
            uc.TIMESTAMP,
            uc.ORDER_LOG,
            uc.ParentInterlockNumber AS CausedInterlockNumber,
            CASE 
                WHEN uc.ParentInterlockNumber IS NOT NULL 
                THEN N'→ Caused Interlock ' + CAST(uc.ParentInterlockNumber AS NVARCHAR(10))
                ELSE N'(Target Interlock)'
            END AS UpstreamLink,
            cd.TYPE AS ConditionType,
            cd.BIT_INDEX AS ConditionBitIndex,
            td_condition.MNEMONIC AS ConditionMnemonic,
            td_condition.MESSAGE AS ConditionMessage,
            CASE WHEN uc.UPSTREAM_INTERLOCK_LOG_ID IS NULL THEN N'✓ ROOT CAUSE' ELSE N'' END AS IsRootCause
        FROM 
            UpstreamChain uc
            INNER JOIN dbo.INTERLOCK_DEFINITION id WITH (NOLOCK) ON uc.INTERLOCK_DEF_ID = id.INTERLOCK_DEF_ID
            INNER JOIN dbo.PLC p WITH (NOLOCK) ON id.PLC_ID = p.PLC_ID
            INNER JOIN dbo.TEXT_DEFINITION td_interlock WITH (NOLOCK) ON id.TEXT_DEF_ID = td_interlock.TEXT_DEF_ID
            LEFT JOIN dbo.FF_CONDITION_LOG cl WITH (NOLOCK) ON uc.ID = cl.INTERLOCK_LOG_ID
            LEFT JOIN dbo.CONDITION_DEFINITION cd WITH (NOLOCK) ON cl.CONDITION_DEF_ID = cd.CONDITION_DEF_ID
            LEFT JOIN dbo.TEXT_DEFINITION td_condition WITH (NOLOCK) ON cd.TEXT_DEF_ID = td_condition.TEXT_DEF_ID
        ORDER BY 
            uc.OccurrenceNumber,
            uc.Level DESC,
            cd.TYPE,
            cd.BIT_INDEX
        OPTION (MAXRECURSION 100);
    """)

    with engine.connect() as conn:
        result = conn.execute(
            query_text,
            {"interlock_number": interlock_number, "limit": limit}
        )
        df = pd.DataFrame(result.fetchall(), columns=result.keys())

    return df


# ============================================================================
# Alternative: Using DBConnection directly (without SQLAlchemy)
# ============================================================================

def get_interlock_root_cause_last_n_pyodbc(interlock_number: int, limit: int = 10) -> pd.DataFrame:
    """
    Alternative method using DBConnection class directly with pyodbc.

    Args:
        interlock_number: The interlock number to search for (e.g., 11222)
        limit: Number of most recent occurrences to retrieve (default: 10)

    Returns:
        pandas.DataFrame with the results
    """

    query = """
        SET NOCOUNT ON;

        DECLARE @LastNInterlocks TABLE (ID INT, RowNum INT);

        INSERT INTO @LastNInterlocks (ID, RowNum)
        SELECT TOP (?)
            il.ID,
            ROW_NUMBER() OVER (ORDER BY il.TIMESTAMP DESC) AS RowNum
        FROM dbo.FF_INTERLOCK_LOG il
        INNER JOIN dbo.INTERLOCK_DEFINITION id ON il.INTERLOCK_DEF_ID = id.INTERLOCK_DEF_ID
        WHERE id.NUMBER = ?
        ORDER BY il.TIMESTAMP DESC;

        WITH UpstreamChain AS (
            SELECT 
                il.ID, il.INTERLOCK_DEF_ID, il.TIMESTAMP, il.ORDER_LOG,
                il.UPSTREAM_INTERLOCK_LOG_ID, 0 AS Level,
                CAST(NULL AS INT) AS ParentInterlockNumber,
                ln.RowNum AS OccurrenceNumber
            FROM dbo.FF_INTERLOCK_LOG il
            INNER JOIN @LastNInterlocks ln ON il.ID = ln.ID

            UNION ALL

            SELECT 
                il.ID, il.INTERLOCK_DEF_ID, il.TIMESTAMP, il.ORDER_LOG,
                il.UPSTREAM_INTERLOCK_LOG_ID, uc.Level + 1,
                id_child.NUMBER AS ParentInterlockNumber, uc.OccurrenceNumber
            FROM dbo.FF_INTERLOCK_LOG il WITH (NOLOCK)
            INNER JOIN UpstreamChain uc ON il.ID = uc.UPSTREAM_INTERLOCK_LOG_ID
            INNER JOIN dbo.INTERLOCK_DEFINITION id_child ON uc.INTERLOCK_DEF_ID = id_child.INTERLOCK_DEF_ID
            WHERE uc.Level < 100
        )
        SELECT 
            uc.OccurrenceNumber AS OccurrenceNum, uc.Level, uc.ID AS InterlockLogID,
            id.NUMBER AS InterlockNumber, p.PLC_CODE,
            td_interlock.MNEMONIC AS InterlockMnemonic,
            td_interlock.MESSAGE AS InterlockMessage,
            uc.TIMESTAMP, uc.ORDER_LOG,
            uc.ParentInterlockNumber AS CausedInterlockNumber,
            CASE 
                WHEN uc.ParentInterlockNumber IS NOT NULL 
                THEN N'→ Caused Interlock ' + CAST(uc.ParentInterlockNumber AS NVARCHAR(10))
                ELSE N'(Target Interlock)'
            END AS UpstreamLink,
            cd.TYPE AS ConditionType, cd.BIT_INDEX AS ConditionBitIndex,
            td_condition.MNEMONIC AS ConditionMnemonic,
            td_condition.MESSAGE AS ConditionMessage,
            CASE WHEN uc.UPSTREAM_INTERLOCK_LOG_ID IS NULL THEN N'✓ ROOT CAUSE' ELSE N'' END AS IsRootCause
        FROM UpstreamChain uc
        INNER JOIN dbo.INTERLOCK_DEFINITION id WITH (NOLOCK) ON uc.INTERLOCK_DEF_ID = id.INTERLOCK_DEF_ID
        INNER JOIN dbo.PLC p WITH (NOLOCK) ON id.PLC_ID = p.PLC_ID
        INNER JOIN dbo.TEXT_DEFINITION td_interlock WITH (NOLOCK) ON id.TEXT_DEF_ID = td_interlock.TEXT_DEF_ID
        LEFT JOIN dbo.FF_CONDITION_LOG cl WITH (NOLOCK) ON uc.ID = cl.INTERLOCK_LOG_ID
        LEFT JOIN dbo.CONDITION_DEFINITION cd WITH (NOLOCK) ON cl.CONDITION_DEF_ID = cd.CONDITION_DEF_ID
        LEFT JOIN dbo.TEXT_DEFINITION td_condition WITH (NOLOCK) ON cd.TEXT_DEF_ID = td_condition.TEXT_DEF_ID
        ORDER BY uc.OccurrenceNumber, uc.Level DESC, cd.TYPE, cd.BIT_INDEX
        OPTION (MAXRECURSION 100);
    """

    with DBConnection() as db:
        cursor = db.execute(query, (limit, interlock_number))
        columns = [column[0] for column in cursor.description]
        rows = cursor.fetchall()
        df = pd.DataFrame.from_records(rows, columns=columns)

    return df


# ============================================================================
# Utility Functions
# ============================================================================

def print_root_cause_analysis(df: pd.DataFrame, interlock_number: int):
    """Pretty print the root cause analysis results."""
    if df.empty:
        print(f"No occurrences found for interlock {interlock_number}")
        return

    print(f"Found {len(df)} rows")
    print("\n" + "=" * 80)
    print(f"ROOT CAUSE ANALYSIS - Last {df['OccurrenceNum'].nunique()} Occurrences of Interlock {interlock_number}")
    print("=" * 80 + "\n")

    for occurrence_num in sorted(df['OccurrenceNum'].unique()):
        occurrence_data = df[df['OccurrenceNum'] == occurrence_num]

        print(f"\n{'=' * 80}")
        print(f"OCCURRENCE #{occurrence_num}")
        print(f"{'=' * 80}")

        for level in sorted(occurrence_data['Level'].unique(), reverse=True):
            level_data = occurrence_data[occurrence_data['Level'] == level]
            first_row = level_data.iloc[0]

            indent = "  " * (1 + level)
            print(f"\n{indent}Level {level}: Interlock {first_row['InterlockNumber']}")
            print(f"{indent}PLC: {first_row['PLC_CODE']}")
            print(f"{indent}Time: {first_row['TIMESTAMP']}")
            print(f"{indent}Message: {first_row['InterlockMessage']}")
            print(f"{indent}{first_row['UpstreamLink']}")
            if first_row['IsRootCause']:
                print(f"{indent}🎯 {first_row['IsRootCause']}")

            conditions = level_data[level_data['ConditionMnemonic'].notna()]
            if not conditions.empty:
                print(f"\n{indent}Active Conditions:")
                for _, cond in conditions.iterrows():
                    print(
                        f"{indent}  - Type {cond['ConditionType']}, Bit {cond['ConditionBitIndex']}: {cond['ConditionMessage']}")


def test_connection() -> bool:
    """Test the SQL Server connection."""
    try:
        with engine.connect() as conn:
            result = conn.execute(text("SELECT @@VERSION AS Version, DB_NAME() AS CurrentDatabase"))
            row = result.fetchone()
            print("✓ Connection successful!")
            print(f"Database: {row.CurrentDatabase}")
            print(f"Version: {row.Version}")
            return True
    except Exception as e:
        print(f"❌ Connection failed: {e}")
        return False


def get_available_interlocks() -> pd.DataFrame:
    """Get list of all available interlocks in the database."""
    query = text("""
                 SELECT DISTINCT id.NUMBER         AS InterlockNumber,
                                 p.PLC_CODE,
                                 td.MNEMONIC,
                                 td.MESSAGE,
                                 COUNT(il.ID)      AS OccurrenceCount,
                                 MAX(il.TIMESTAMP) AS LastOccurrence
                 FROM dbo.INTERLOCK_DEFINITION id
                          INNER JOIN dbo.PLC p ON id.PLC_ID = p.PLC_ID
                          INNER JOIN dbo.TEXT_DEFINITION td ON id.TEXT_DEF_ID = td.TEXT_DEF_ID
                          LEFT JOIN dbo.FF_INTERLOCK_LOG il ON id.INTERLOCK_DEF_ID = il.INTERLOCK_DEF_ID
                 GROUP BY id.NUMBER, p.PLC_CODE, td.MNEMONIC, td.MESSAGE
                 ORDER BY COUNT(il.ID) DESC, id.NUMBER;
                 """)

    with engine.connect() as conn:
        result = conn.execute(query)
        df = pd.DataFrame(result.fetchall(), columns=result.keys())

    return df


def save_results(df: pd.DataFrame, interlock_number: int, output_dir: Optional[str] = None):
    """Save results to CSV and Excel files."""
    if output_dir is None:
        output_dir = base_dir

    os.makedirs(output_dir, exist_ok=True)

    csv_filename = f'interlock_{interlock_number}_root_cause_analysis.csv'
    xlsx_filename = f'interlock_{interlock_number}_root_cause_analysis.xlsx'

    csv_path = os.path.join(output_dir, csv_filename)
    xlsx_path = os.path.join(output_dir, xlsx_filename)

    df.to_csv(csv_path, index=False)
    print(f"✓ Results saved to: {csv_path}")

    try:
        df.to_excel(xlsx_path, index=False, sheet_name='Root Cause Analysis')
        print(f"✓ Results saved to: {xlsx_path}")
    except ImportError:
        print("ℹ️  Install 'openpyxl' to export to Excel: uv add openpyxl")


# ============================================================================
# Usage Example
# ============================================================================

if __name__ == "__main__":
    from datetime import datetime
    try:

        start=datetime.now()
        print(f"Script directory: {base_dir}")
        print(f"Config path: {os.path.join(base_dir, '..', 'config', 'Connection.yaml')}\n")

        # Test connection
        print("Testing SQL Server connection...")
        if not test_connection():
            exit(1)

        print("\n" + "=" * 80)
        print("Available Interlocks:")
        print("=" * 80)
        interlocks = get_available_interlocks()
        print(interlocks.head(20))

        print("\n" + "=" * 80)
        print("Root Cause Analysis for Interlock 11222")
        print("=" * 80)

        # Method 1: Using SQLAlchemy (recommended)
        results = get_interlock_root_cause_last_n(11222, limit=10)

        # Method 2: Using DBConnection directly (alternative)
        # results = get_interlock_root_cause_last_n_pyodbc(11222, limit=10)

        print_root_cause_analysis(results, 11222)
        save_results(results, 11222)

        end=datetime.now()
        print(f"Execution time: {end-start}")

    except FileNotFoundError as e:
        print(f"❌ Configuration Error: {e}")
        print(f"\nPlease create 'Connection.yaml' in: {os.path.join(base_dir, '..', 'config')}")
    except Exception as e:
        print(f"❌ Error: {e}")
        import traceback

        traceback.print_exc()
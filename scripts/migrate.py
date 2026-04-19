"""
Fast Python migration: TD2 → First_Fault
=========================================
Exact Python translation of migrate_database.sql.

The only difference from the SQL script is Step 6: instead of a cursor
inserting one row at a time, we load the source rows into a SQL Server
temp table (including the Old_ID) and then use INSERT ... OUTPUT to
capture all new IDs in one bulk operation.

Every join condition, filter, and ordering matches the original SQL exactly.

Run from the project root:
    python scripts/migrate.py
"""

import sys
from pathlib import Path

from sqlalchemy import text

sys.path.insert(0, str(Path(__file__).parent.parent))

from data.repositories.DB_Connection import get_engine

SOURCE_DB = "TD2"
TARGET_DB = "First_Fault"
TOP_N = None          # None = all records, or set e.g. 10_000 for a partial run
CHUNK_SIZE = 2_000    # rows per bulk insert into the staging temp table


def run():
    engine = get_engine()

    # ----------------------------------------------------------------
    # CONNECTION CHECK
    # ----------------------------------------------------------------
    with engine.begin() as conn:
        row = conn.execute(text("""
            SELECT
                @@SERVERNAME           AS server_name,
                DB_NAME()              AS current_db,
                DB_ID(:src)            AS src_exists,
                DB_ID(:tgt)            AS tgt_exists
        """), {"src": SOURCE_DB, "tgt": TARGET_DB}).fetchone()

        print(f"\nServer  : {row.server_name}")
        print(f"Default : {row.current_db}")
        print(f"Source  : {SOURCE_DB}  ({'FOUND' if row.src_exists else '*** NOT FOUND ***'})")
        print(f"Target  : {TARGET_DB}  ({'FOUND' if row.tgt_exists else '*** NOT FOUND ***'})")

        if not row.src_exists or not row.tgt_exists:
            raise RuntimeError("One or both databases not found — check Connection.yaml")

        print()

    # ----------------------------------------------------------------
    # STEP 0: Clear all existing data — committed immediately so a
    # failed migration doesn't restore stale data via rollback.
    # ----------------------------------------------------------------
    print("Step 0: Clearing existing data...")
    with engine.begin() as conn:
        # Null the self-reference first — only if table exists and has rows
        conn.execute(text(f"""
            IF OBJECT_ID('{TARGET_DB}.dbo.FF_INTERLOCK_LOG') IS NOT NULL
                UPDATE {TARGET_DB}.dbo.FF_INTERLOCK_LOG
                SET UPSTREAM_INTERLOCK_LOG_ID = NULL
                WHERE UPSTREAM_INTERLOCK_LOG_ID IS NOT NULL
        """))

        tables = [
            "FF_CONDITION_LOG",
            "FF_INTERLOCK_LOG",
            "FAULT_TREND_SNAPSHOTS",
            "TREND_ANALYSIS_CONFIG",
            "CONDITION_DEFINITION",
            "INTERLOCK_DEFINITION",
            "TEXT_DEFINITION",
            "PLC",
        ]
        for table in tables:
            # Delete and capture rowcount in a single batch
            n = conn.execute(text(f"""
                SET NOCOUNT ON
                DECLARE @n INT = 0
                IF OBJECT_ID('{TARGET_DB}.dbo.{table}') IS NOT NULL
                BEGIN
                    DELETE FROM {TARGET_DB}.dbo.{table}
                    SET @n = @@ROWCOUNT
                END
                SELECT @n
            """)).scalar()
            print(f"  Deleted {n:>8} rows from {table}")

        # Add REPORTING column to TEXT_DEFINITION if it doesn't exist yet
        conn.execute(text(f"""
            IF NOT EXISTS (
                SELECT 1 FROM sys.columns
                WHERE object_id = OBJECT_ID('{TARGET_DB}.dbo.TEXT_DEFINITION')
                  AND name = 'REPORTING'
            )
            ALTER TABLE {TARGET_DB}.dbo.TEXT_DEFINITION
                ADD REPORTING bit NOT NULL DEFAULT 1
        """))
        print("  TEXT_DEFINITION.REPORTING column ready")

    # ----------------------------------------------------------------
    # MIGRATION — runs in its own transaction. Rolls back on error
    # but Step 0 is already committed above.
    # ----------------------------------------------------------------
    with engine.begin() as conn:

        # Cleanup temp tables from any previous failed run
        for tmp in ("#TempInterlockLog", "#IDMapping", "#TempConditionLog", "#UpstreamMapping", "#StagedLog"):
            conn.execute(text(f"IF OBJECT_ID('tempdb..{tmp}') IS NOT NULL DROP TABLE {tmp}"))

        # ----------------------------------------------------------------
        # STEP 1: Populate PLC  (identical to SQL)
        # ----------------------------------------------------------------
        print("Step 1: Migrating PLC data from TD2...")
        conn.execute(text(f"""
            INSERT INTO {TARGET_DB}.dbo.PLC (PLC_NAME)
            SELECT DISTINCT PLC COLLATE Latin1_General_CI_AS AS PLC_NAME
            FROM {SOURCE_DB}.dbo.FF_INTERLOCK_LOG
            WHERE PLC IS NOT NULL
            ORDER BY PLC
        """))
        n = conn.execute(text(f"SELECT @@ROWCOUNT")).scalar()
        print(f"  PLCs migrated: {n}")

        # ----------------------------------------------------------------
        # STEP 2: Populate TEXT_DEFINITION  (identical to SQL + REPORTING=1)
        # ----------------------------------------------------------------
        print("Step 2: Migrating TEXT_DEFINITION...")
        conn.execute(text(f"""
            INSERT INTO {TARGET_DB}.dbo.TEXT_DEFINITION (MNEMONIC, MESSAGE, REPORTING)
            SELECT DISTINCT
                CAST(MNEMONIC AS NVARCHAR(MAX)) COLLATE Latin1_General_CI_AS AS MNEMONIC,
                CAST(MESSAGE  AS NVARCHAR(MAX)) COLLATE Latin1_General_CI_AS AS MESSAGE,
                1 AS REPORTING
            FROM (
                SELECT DISTINCT
                    CAST(MNEMONIC AS NVARCHAR(MAX)) AS MNEMONIC,
                    CAST(MESSAGE  AS NVARCHAR(MAX)) AS MESSAGE
                FROM {SOURCE_DB}.dbo.FF_INTERLOCK_LOG
                WHERE MNEMONIC IS NOT NULL AND MESSAGE IS NOT NULL

                UNION

                SELECT DISTINCT
                    CAST(MNEMONIC AS NVARCHAR(MAX)) AS MNEMONIC,
                    CAST(MESSAGE  AS NVARCHAR(MAX)) AS MESSAGE
                FROM {SOURCE_DB}.dbo.FF_CONDITION_LOG
                WHERE MNEMONIC IS NOT NULL AND MESSAGE IS NOT NULL
            ) AS AllTexts
            ORDER BY MNEMONIC, MESSAGE
        """))
        n = conn.execute(text(f"SELECT @@ROWCOUNT")).scalar()
        print(f"  Unique text definitions migrated: {n}")

        # ----------------------------------------------------------------
        # STEP 3: Populate INTERLOCK_DEFINITION  (identical to SQL)
        # ----------------------------------------------------------------
        print("Step 3: Migrating Interlock Definitions...")
        conn.execute(text(f"""
            ;WITH LatestInterlockText AS (
                SELECT
                    il.PLC,
                    il.NUMBER,
                    CAST(il.MNEMONIC AS NVARCHAR(MAX)) AS MNEMONIC,
                    CAST(il.MESSAGE  AS NVARCHAR(MAX)) AS MESSAGE,
                    ROW_NUMBER() OVER (
                        PARTITION BY il.PLC, il.NUMBER
                        ORDER BY il.TIMESTAMP DESC, il.ID DESC
                    ) AS RowNum
                FROM {SOURCE_DB}.dbo.FF_INTERLOCK_LOG il
                WHERE il.NUMBER IS NOT NULL
                  AND il.MNEMONIC IS NOT NULL
                  AND il.MESSAGE IS NOT NULL
            )
            INSERT INTO {TARGET_DB}.dbo.INTERLOCK_DEFINITION (PLC_ID, NUMBER, TEXT_DEF_ID)
            SELECT p.PLC_ID, lit.NUMBER, td.TEXT_DEF_ID
            FROM LatestInterlockText lit
            INNER JOIN {TARGET_DB}.dbo.PLC p
                ON lit.PLC COLLATE Latin1_General_CI_AS = p.PLC_NAME
            INNER JOIN {TARGET_DB}.dbo.TEXT_DEFINITION td
                ON lit.MNEMONIC COLLATE Latin1_General_CI_AS = td.MNEMONIC
               AND lit.MESSAGE  COLLATE Latin1_General_CI_AS = td.MESSAGE
            WHERE lit.RowNum = 1
        """))
        n = conn.execute(text(f"SELECT @@ROWCOUNT")).scalar()
        print(f"  Interlock Definitions migrated: {n}")

        # ----------------------------------------------------------------
        # STEP 4: Populate CONDITION_DEFINITION  (identical to SQL)
        # ----------------------------------------------------------------
        print("Step 4: Migrating Condition Definitions (WITH INTERLOCK_NUMBER)...")
        conn.execute(text(f"""
            ;WITH LatestConditionText AS (
                SELECT
                    il.PLC,
                    il.NUMBER AS INTERLOCK_NUMBER,
                    cl.TYPE,
                    cl.BIT_INDEX,
                    CAST(cl.MNEMONIC AS NVARCHAR(MAX)) AS MNEMONIC,
                    CAST(cl.MESSAGE  AS NVARCHAR(MAX)) AS MESSAGE,
                    ROW_NUMBER() OVER (
                        PARTITION BY il.PLC, il.NUMBER, cl.TYPE, cl.BIT_INDEX
                        ORDER BY il.TIMESTAMP DESC, cl.ID DESC
                    ) AS RowNum
                FROM {SOURCE_DB}.dbo.FF_CONDITION_LOG cl
                INNER JOIN {SOURCE_DB}.dbo.FF_INTERLOCK_LOG il
                    ON cl.INTERLOCK_REF = il.ID
                WHERE cl.TYPE IS NOT NULL
                  AND cl.BIT_INDEX IS NOT NULL
                  AND cl.MNEMONIC IS NOT NULL
                  AND cl.MESSAGE IS NOT NULL
                  AND il.NUMBER IS NOT NULL
            )
            INSERT INTO {TARGET_DB}.dbo.CONDITION_DEFINITION
                (PLC_ID, INTERLOCK_NUMBER, TYPE, BIT_INDEX, TEXT_DEF_ID)
            SELECT p.PLC_ID, lct.INTERLOCK_NUMBER, lct.TYPE, lct.BIT_INDEX, td.TEXT_DEF_ID
            FROM LatestConditionText lct
            INNER JOIN {TARGET_DB}.dbo.PLC p
                ON lct.PLC COLLATE Latin1_General_CI_AS = p.PLC_NAME
            INNER JOIN {TARGET_DB}.dbo.TEXT_DEFINITION td
                ON lct.MNEMONIC COLLATE Latin1_General_CI_AS = td.MNEMONIC
               AND lct.MESSAGE  COLLATE Latin1_General_CI_AS = td.MESSAGE
            WHERE lct.RowNum = 1
        """))
        n = conn.execute(text(f"SELECT @@ROWCOUNT")).scalar()
        print(f"  Condition Definitions migrated: {n}")

        # ----------------------------------------------------------------
        # STEP 5: Prepare #TempInterlockLog  (identical to SQL, TOP 10000)
        # ----------------------------------------------------------------
        top_clause = f"TOP {TOP_N}" if TOP_N else ""
        print(f"Step 5: Preparing Interlock Log data ({top_clause or 'ALL'})...")
        conn.execute(text(f"""
            SELECT {top_clause}
                old_il.ID              AS Old_ID,
                idef.INTERLOCK_DEF_ID,
                old_il.TIMESTAMP,
                old_il.TIMESTAMP_LOG,
                ISNULL(old_il.ORDER_LOG, 0) AS ORDER_LOG
            INTO #TempInterlockLog
            FROM {SOURCE_DB}.dbo.FF_INTERLOCK_LOG old_il
            INNER JOIN {TARGET_DB}.dbo.PLC p
                ON old_il.PLC COLLATE Latin1_General_CI_AS = p.PLC_NAME
            INNER JOIN {TARGET_DB}.dbo.INTERLOCK_DEFINITION idef
                ON p.PLC_ID = idef.PLC_ID
               AND old_il.NUMBER = idef.NUMBER
            WHERE old_il.NUMBER IS NOT NULL
            ORDER BY old_il.TIMESTAMP DESC, old_il.ID DESC
        """))
        n = conn.execute(text("SELECT COUNT(*) FROM #TempInterlockLog")).scalar()
        print(f"  Temp table created with {n} records")

        # Create empty #IDMapping — filled by MERGE OUTPUT in Step 6
        conn.execute(text("""
            CREATE TABLE #IDMapping (
                Old_ID  UNIQUEIDENTIFIER,
                New_ID  INT
            )
        """))

        # ----------------------------------------------------------------
        # STEP 6: Insert FF_INTERLOCK_LOG and build #IDMapping
        #
        # SQL used a row-by-row cursor + SCOPE_IDENTITY().
        # We replace that with:
        #   1. Stage rows in a temp table that includes Old_ID
        #   2. INSERT ... OUTPUT INSERTED.ID + Old_ID INTO #IDMapping
        # Same result, no cursor needed.
        # ----------------------------------------------------------------
        print("Step 6: Inserting FF_INTERLOCK_LOG records (bulk with OUTPUT)...")

        conn.execute(text("""
            IF OBJECT_ID('tempdb..#StagedLog') IS NOT NULL DROP TABLE #StagedLog;
            CREATE TABLE #StagedLog (
                Old_ID           UNIQUEIDENTIFIER,
                INTERLOCK_DEF_ID INT,
                TIMESTAMP       DATETIME,
                TIMESTAMP_LOG   DATETIME,
                ORDER_LOG       INT
            );
        """))

        # Load source rows into Python in chunks and stage them back to SQL Server
        source_rows = conn.execute(
            text("SELECT Old_ID, INTERLOCK_DEF_ID, TIMESTAMP, TIMESTAMP_LOG, ORDER_LOG FROM #TempInterlockLog")
        ).fetchall()

        total = len(source_rows)
        for i in range(0, total, CHUNK_SIZE):
            chunk = source_rows[i: i + CHUNK_SIZE]
            params = [
                {
                    "old_id": r.Old_ID,
                    "idef_id": r.INTERLOCK_DEF_ID,
                    "ts": r.TIMESTAMP,
                    "ts_log": r.TIMESTAMP_LOG,
                    "order_log": r.ORDER_LOG,
                }
                for r in chunk
            ]
            conn.execute(
                text("INSERT INTO #StagedLog VALUES (:old_id, :idef_id, :ts, :ts_log, :order_log)"),
                params,
            )
            print(f"  Staged {min(i + CHUNK_SIZE, total)} / {total}", end="\r")

        print()

        # Bulk insert with ID capture — MERGE allows source columns in OUTPUT clause
        # (plain INSERT ... OUTPUT cannot reference source table columns in SQL Server)
        conn.execute(text(f"""
            MERGE INTO {TARGET_DB}.dbo.FF_INTERLOCK_LOG AS tgt
            USING #StagedLog AS src ON 1 = 0
            WHEN NOT MATCHED THEN
                INSERT (INTERLOCK_DEF_ID, TIMESTAMP, TIMESTAMP_LOG, ORDER_LOG, UPSTREAM_INTERLOCK_LOG_ID)
                VALUES (src.INTERLOCK_DEF_ID, src.TIMESTAMP, src.TIMESTAMP_LOG, src.ORDER_LOG, NULL)
            OUTPUT INSERTED.ID, src.Old_ID INTO #IDMapping (New_ID, Old_ID);
        """))

        n = conn.execute(text(f"SELECT COUNT(*) FROM {TARGET_DB}.dbo.FF_INTERLOCK_LOG")).scalar()
        print(f"  FF_INTERLOCK_LOG rows inserted: {n}")

        conn.execute(text("DROP TABLE #StagedLog"))

        # ----------------------------------------------------------------
        # STEP 7: Populate FF_CONDITION_LOG via #TempConditionLog  (identical to SQL)
        # ----------------------------------------------------------------
        print("Step 7: Migrating Condition Log...")
        conn.execute(text(f"""
            SELECT
                old_cl.ID                       AS Old_Condition_ID,
                map.New_ID                       AS New_Interlock_Log_ID,
                cdef.CONDITION_DEF_ID,
                old_cl.UPSTREAM_INTERLOCK_REF,
                old_cl.INTERLOCK_REF             AS Current_Interlock_ID
            INTO #TempConditionLog
            FROM {SOURCE_DB}.dbo.FF_CONDITION_LOG old_cl
            INNER JOIN #IDMapping map
                ON old_cl.INTERLOCK_REF = map.Old_ID
            INNER JOIN {SOURCE_DB}.dbo.FF_INTERLOCK_LOG old_il
                ON old_cl.INTERLOCK_REF = old_il.ID
            INNER JOIN {TARGET_DB}.dbo.PLC p
                ON old_il.PLC COLLATE Latin1_General_CI_AS = p.PLC_NAME
            INNER JOIN {TARGET_DB}.dbo.CONDITION_DEFINITION cdef
                ON p.PLC_ID = cdef.PLC_ID
               AND old_il.NUMBER = cdef.INTERLOCK_NUMBER
               AND old_cl.TYPE = cdef.TYPE
               AND old_cl.BIT_INDEX = cdef.BIT_INDEX
            WHERE old_cl.INTERLOCK_REF IS NOT NULL
              AND old_cl.TYPE IS NOT NULL
              AND old_cl.BIT_INDEX IS NOT NULL
        """))
        n = conn.execute(text("SELECT @@ROWCOUNT")).scalar()
        print(f"  Condition log temp data prepared: {n}")

        conn.execute(text(f"""
            INSERT INTO {TARGET_DB}.dbo.FF_CONDITION_LOG (INTERLOCK_LOG_ID, CONDITION_DEF_ID)
            SELECT DISTINCT New_Interlock_Log_ID, CONDITION_DEF_ID
            FROM #TempConditionLog
        """))
        n = conn.execute(text("SELECT @@ROWCOUNT")).scalar()
        print(f"  Condition Log entries migrated: {n}")

        # ----------------------------------------------------------------
        # STEP 8: Build #UpstreamMapping  (identical to SQL)
        # ----------------------------------------------------------------
        print("Step 8: Mapping upstream interlock references...")
        conn.execute(text("""
            SELECT DISTINCT
                tcl.Current_Interlock_ID    AS Current_Old_ID,
                map_current.New_ID          AS Current_New_ID,
                tcl.UPSTREAM_INTERLOCK_REF  AS Upstream_Old_ID,
                map_upstream.New_ID         AS Upstream_New_ID
            INTO #UpstreamMapping
            FROM #TempConditionLog tcl
            INNER JOIN #IDMapping map_current
                ON tcl.Current_Interlock_ID = map_current.Old_ID
            LEFT JOIN #IDMapping map_upstream
                ON tcl.UPSTREAM_INTERLOCK_REF = map_upstream.Old_ID
            WHERE tcl.UPSTREAM_INTERLOCK_REF IS NOT NULL
        """))
        n = conn.execute(text("SELECT @@ROWCOUNT")).scalar()
        print(f"  Upstream mappings prepared: {n}")

        # ----------------------------------------------------------------
        # STEP 9: Update upstream references  (identical to SQL)
        # ----------------------------------------------------------------
        print("Step 9: Updating upstream interlock references...")
        conn.execute(text(f"""
            UPDATE il
            SET UPSTREAM_INTERLOCK_LOG_ID = um.Upstream_New_ID
            FROM {TARGET_DB}.dbo.FF_INTERLOCK_LOG il
            INNER JOIN #UpstreamMapping um
                ON il.ID = um.Current_New_ID
            WHERE um.Upstream_New_ID IS NOT NULL
        """))
        n = conn.execute(text("SELECT @@ROWCOUNT")).scalar()
        print(f"  Upstream references updated: {n}")

        # ----------------------------------------------------------------
        # STEP 10: Cleanup  (identical to SQL)
        # ----------------------------------------------------------------
        print("Step 10: Cleaning up temporary tables...")
        for tmp in ("#TempInterlockLog", "#TempConditionLog", "#IDMapping", "#UpstreamMapping"):
            conn.execute(text(f"DROP TABLE {tmp}"))

        # ----------------------------------------------------------------
        # STEP 11: Summary  (identical to SQL)
        # ----------------------------------------------------------------
        print()
        print("=" * 72)
        print("MIGRATION SUMMARY: TD2 → First_Fault")
        print("=" * 72)

        src_interlock = conn.execute(text(f"SELECT COUNT(*) FROM {SOURCE_DB}.dbo.FF_INTERLOCK_LOG")).scalar()
        src_condition = conn.execute(text(f"SELECT COUNT(*) FROM {SOURCE_DB}.dbo.FF_CONDITION_LOG")).scalar()
        print(f"  SOURCE (TD2)")
        print(f"    Total Interlock Log : {src_interlock}")
        print(f"    Total Condition Log : {src_condition}")
        print()

        for table in ("PLC", "TEXT_DEFINITION", "INTERLOCK_DEFINITION",
                      "CONDITION_DEFINITION", "FF_INTERLOCK_LOG", "FF_CONDITION_LOG"):
            n = conn.execute(text(f"SELECT COUNT(*) FROM {TARGET_DB}.dbo.{table}")).scalar()
            print(f"    {table:<30} {n:>10} rows")

        upstream = conn.execute(text(f"""
            SELECT COUNT(*) FROM {TARGET_DB}.dbo.FF_INTERLOCK_LOG
            WHERE UPSTREAM_INTERLOCK_LOG_ID IS NOT NULL
        """)).scalar()
        print(f"    {'Upstream References':<30} {upstream:>10}")

        print("=" * 72)
        print("MIGRATION COMPLETED SUCCESSFULLY")
        print("=" * 72)


if __name__ == "__main__":
    try:
        run()
    except Exception as e:
        print(f"\nERROR: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)

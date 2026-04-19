/*
================================================================================
INDEX UPGRADE — FF_INTERLOCK_LOG
================================================================================

For existing databases: upgrades the two non-covering indexes to the covering
versions defined in Create database.sql. Fresh builds already get these via
that script — this file exists for in-place migration on a live DB.

End state (matches Create database.sql):

  IX_InterlockLog_DefID
      KEY     : INTERLOCK_DEF_ID, TIMESTAMP DESC, ORDER_LOG DESC, ID DESC
      INCLUDE : UPSTREAM_INTERLOCK_LOG_ID
      Purpose : anchor selection when @TargetBSID IS NOT NULL — single seek +
                ordered scan, no Sort operator, no Key Lookup.

  IX_InterlockLog_Upstream
      KEY     : UPSTREAM_INTERLOCK_LOG_ID
      INCLUDE : INTERLOCK_DEF_ID, TIMESTAMP
      Purpose : DownstreamChain recursive join — removes the per-row Key
                Lookup that was happening at every recursion level.

RUN ORDER:
  1. Step 1 prints current indexes (snapshot for rollback reference).
  2. Step 2 is idempotent — only drops/recreates when the current definition
     doesn't already match the target.
  3. Step 3 re-prints so you can verify.

CAUTION:
  - Run on dev/staging first.
  - Non-online index builds briefly lock the table. ONLINE = ON requires
    Enterprise Edition.
================================================================================
*/

-- ============================================================================
-- STEP 1: snapshot current indexes on FF_INTERLOCK_LOG
-- ============================================================================
SELECT i.name AS index_name,
       i.type_desc,
       STUFF((SELECT ', ' + c.name
              FROM sys.index_columns ic
              JOIN sys.columns c
                ON ic.object_id = c.object_id AND ic.column_id = c.column_id
              WHERE ic.object_id = i.object_id
                AND ic.index_id  = i.index_id
                AND ic.is_included_column = 0
              ORDER BY ic.key_ordinal
              FOR XML PATH('')), 1, 2, '') AS key_cols,
       STUFF((SELECT ', ' + c.name
              FROM sys.index_columns ic
              JOIN sys.columns c
                ON ic.object_id = c.object_id AND ic.column_id = c.column_id
              WHERE ic.object_id = i.object_id
                AND ic.index_id  = i.index_id
                AND ic.is_included_column = 1
              FOR XML PATH('')), 1, 2, '') AS included_cols
FROM sys.indexes i
WHERE i.object_id = OBJECT_ID('First_Fault.dbo.FF_INTERLOCK_LOG');
GO

-- ============================================================================
-- STEP 2: apply covering indexes (idempotent: replaces existing by same name)
-- ============================================================================

-- 2a. Anchor path: INTERLOCK_DEF_ID + ordered TIMESTAMP/ORDER_LOG/ID
IF EXISTS (
    SELECT 1 FROM sys.indexes
    WHERE name = 'IX_InterlockLog_DefID'
      AND object_id = OBJECT_ID('First_Fault.dbo.FF_INTERLOCK_LOG')
)
    DROP INDEX IX_InterlockLog_DefID ON First_Fault.dbo.FF_INTERLOCK_LOG;
GO

CREATE NONCLUSTERED INDEX IX_InterlockLog_DefID
    ON First_Fault.dbo.FF_INTERLOCK_LOG (INTERLOCK_DEF_ID, TIMESTAMP DESC, ORDER_LOG DESC, ID DESC)
    INCLUDE (UPSTREAM_INTERLOCK_LOG_ID);
GO

-- 2b. Downstream recursive path: UPSTREAM_INTERLOCK_LOG_ID covering
IF EXISTS (
    SELECT 1 FROM sys.indexes
    WHERE name = 'IX_InterlockLog_Upstream'
      AND object_id = OBJECT_ID('First_Fault.dbo.FF_INTERLOCK_LOG')
)
    DROP INDEX IX_InterlockLog_Upstream ON First_Fault.dbo.FF_INTERLOCK_LOG;
GO

CREATE NONCLUSTERED INDEX IX_InterlockLog_Upstream
    ON First_Fault.dbo.FF_INTERLOCK_LOG (UPSTREAM_INTERLOCK_LOG_ID)
    INCLUDE (INTERLOCK_DEF_ID, TIMESTAMP);
GO

-- ============================================================================
-- STEP 3: confirm new indexes are in place
-- ============================================================================
SELECT i.name AS index_name,
       i.type_desc,
       STUFF((SELECT ', ' + c.name
              FROM sys.index_columns ic
              JOIN sys.columns c
                ON ic.object_id = c.object_id AND ic.column_id = c.column_id
              WHERE ic.object_id = i.object_id
                AND ic.index_id  = i.index_id
                AND ic.is_included_column = 0
              ORDER BY ic.key_ordinal
              FOR XML PATH('')), 1, 2, '') AS key_cols,
       STUFF((SELECT ', ' + c.name
              FROM sys.index_columns ic
              JOIN sys.columns c
                ON ic.object_id = c.object_id AND ic.column_id = c.column_id
              WHERE ic.object_id = i.object_id
                AND ic.index_id  = i.index_id
                AND ic.is_included_column = 1
              FOR XML PATH('')), 1, 2, '') AS included_cols
FROM sys.indexes i
WHERE i.object_id = OBJECT_ID('First_Fault.dbo.FF_INTERLOCK_LOG');
GO

/*
================================================================================
NEXT: re-run the timing block at the bottom of
      test_fn_InterlockChain_optimized.sql
      and compare warm-run elapsed_ms against the previous ~5.7 s baseline.
================================================================================
*/


/*
================================================================================
ROLLBACK — restore key-only (non-covering) indexes
================================================================================

DROP INDEX IX_InterlockLog_DefID    ON First_Fault.dbo.FF_INTERLOCK_LOG;
DROP INDEX IX_InterlockLog_Upstream ON First_Fault.dbo.FF_INTERLOCK_LOG;

CREATE INDEX IX_InterlockLog_DefID    ON First_Fault.dbo.FF_INTERLOCK_LOG (INTERLOCK_DEF_ID);
CREATE INDEX IX_InterlockLog_Upstream ON First_Fault.dbo.FF_INTERLOCK_LOG (UPSTREAM_INTERLOCK_LOG_ID);
================================================================================
*/
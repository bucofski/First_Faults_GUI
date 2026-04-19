/*
================================================================================
INDEX UPGRADE — FF_CONDITION_LOG
================================================================================

For existing databases: upgrades IX_ConditionLog_InterlockID to a covering
index. Fresh builds already get this via Create database.sql.

End state (matches Create database.sql):

  IX_ConditionLog_InterlockID
      KEY     : INTERLOCK_LOG_ID
      INCLUDE : CONDITION_DEF_ID
      Purpose : removes Key Lookup on every row of the final LEFT JOIN in
                fn_InterlockChain.

CAUTION:
  - Run on dev/staging first.
================================================================================
*/

-- Step 1: snapshot
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
WHERE i.object_id = OBJECT_ID('First_Fault.dbo.FF_CONDITION_LOG');
GO

-- Step 2: replace
IF EXISTS (
    SELECT 1 FROM sys.indexes
    WHERE name = 'IX_ConditionLog_InterlockID'
      AND object_id = OBJECT_ID('First_Fault.dbo.FF_CONDITION_LOG')
)
    DROP INDEX IX_ConditionLog_InterlockID ON First_Fault.dbo.FF_CONDITION_LOG;
GO

CREATE NONCLUSTERED INDEX IX_ConditionLog_InterlockID
    ON First_Fault.dbo.FF_CONDITION_LOG (INTERLOCK_LOG_ID)
    INCLUDE (CONDITION_DEF_ID);
GO

-- Step 3: confirm
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
WHERE i.object_id = OBJECT_ID('First_Fault.dbo.FF_CONDITION_LOG');
GO

/*
================================================================================
ROLLBACK
================================================================================

DROP INDEX IX_ConditionLog_InterlockID ON First_Fault.dbo.FF_CONDITION_LOG;
CREATE INDEX IX_ConditionLog_InterlockID ON First_Fault.dbo.FF_CONDITION_LOG(INTERLOCK_LOG_ID);
================================================================================
*/
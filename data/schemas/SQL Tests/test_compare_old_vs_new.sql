-- ============================================================================
-- TEST: fn_InterlockChain  OLD vs NEW  (regression check)
--
-- Goal: prove that the edits to prod_sql.sql are output-preserving.
--   OLD = committed version (SELECT DISTINCT, 15 columns, no AnchorTimestamp)
--   NEW = edited version    (no DISTINCT, 16 columns, + AnchorTimestamp)
--
-- Method:
--   1. Create both versions as temporary functions (_OLD / _NEW).
--   2. For several parameter sets, compare on the 15 SHARED columns
--      (AnchorTimestamp is excluded — it only exists in NEW).
--   3. EXCEPT in both directions; any non-zero count = a difference.
--      Expected result for every row: OnlyInOld = 0 AND OnlyInNew = 0.
--
-- Safe to run repeatedly: it drops the temp functions at the end.
-- ============================================================================
USE First_Fault;
GO

IF OBJECT_ID('dbo.fn_InterlockChain_OLD', 'TF') IS NOT NULL DROP FUNCTION dbo.fn_InterlockChain_OLD;
GO
IF OBJECT_ID('dbo.fn_InterlockChain_NEW', 'TF') IS NOT NULL DROP FUNCTION dbo.fn_InterlockChain_NEW;
GO

-- ============================================================================
-- OLD version (as committed: SELECT DISTINCT, no AnchorTimestamp)
-- ============================================================================
CREATE FUNCTION dbo.fn_InterlockChain_OLD (
    @TargetBSID INT = NULL,
    @TopN INT = NULL,
    @FilterTimestampStart DATETIME = NULL,
    @FilterTimestampEnd DATETIME = NULL,
    @FilterConditionMessage NVARCHAR(255) = NULL,
    @FilterPLC NVARCHAR(50) = NULL
)
RETURNS TABLE
AS
RETURN
(
    WITH AnchorInterlock AS (
        SELECT DISTINCT TOP (ISNULL(@TopN, 100))
            il.ID          AS AnchorID,
            il.TIMESTAMP   AS AnchorTimestamp,
            CAST(il.TIMESTAMP AS DATE) AS AnchorDate,
            il.ORDER_LOG   AS AnchorOrderLog
        FROM First_Fault.dbo.FF_INTERLOCK_LOG il
        INNER JOIN First_Fault.dbo.INTERLOCK_DEFINITION idef
            ON il.INTERLOCK_DEF_ID = idef.INTERLOCK_DEF_ID
        INNER JOIN First_Fault.dbo.PLC p
            ON idef.PLC_ID = p.PLC_ID
        WHERE (@TargetBSID IS NULL OR idef.NUMBER = @TargetBSID)
            AND (@TargetBSID IS NOT NULL OR il.UPSTREAM_INTERLOCK_LOG_ID IS NULL)
            AND (
                @FilterTimestampStart IS NULL
                OR il.TIMESTAMP >= CASE
                    WHEN CAST(@FilterTimestampStart AS TIME) = '00:00:00.000'
                    THEN CAST(CAST(@FilterTimestampStart AS DATE) AS DATETIME)
                    ELSE @FilterTimestampStart
                END
            )
            AND (
                @FilterTimestampEnd IS NULL
                OR il.TIMESTAMP <= CASE
                    WHEN CAST(@FilterTimestampEnd AS TIME) = '00:00:00.000'
                    THEN DATEADD(SECOND, -1, DATEADD(DAY, 1, CAST(CAST(@FilterTimestampEnd AS DATE) AS DATETIME)))
                    ELSE @FilterTimestampEnd
                END
            )
            AND (@FilterPLC IS NULL OR p.PLC_NAME = @FilterPLC)
            AND (@FilterConditionMessage IS NULL
                OR EXISTS (
                    SELECT 1
                    FROM First_Fault.dbo.FF_CONDITION_LOG cl2
                    INNER JOIN First_Fault.dbo.CONDITION_DEFINITION cdef2
                        ON cl2.CONDITION_DEF_ID = cdef2.CONDITION_DEF_ID
                    INNER JOIN First_Fault.dbo.TEXT_DEFINITION td_condition2
                        ON cdef2.TEXT_DEF_ID = td_condition2.TEXT_DEF_ID
                    WHERE cl2.INTERLOCK_LOG_ID = il.ID
                        AND (td_condition2.MESSAGE LIKE '%' + @FilterConditionMessage + '%'
                             OR td_condition2.MNEMONIC LIKE '%' + @FilterConditionMessage + '%')
                ))
        ORDER BY il.TIMESTAMP DESC, il.ORDER_LOG DESC, il.ID DESC
    ),
    UpstreamChain AS (
        SELECT
            il.ID                              AS AnchorReference,
            0                                  AS Level,
            il.ID,
            il.TIMESTAMP,
            CAST(il.TIMESTAMP AS DATE)         AS Date,
            p.PLC_NAME                         AS PLC,
            idef.NUMBER                        AS BSID,
            td_interlock.MESSAGE               AS Interlock_Message,
            il.UPSTREAM_INTERLOCK_LOG_ID       AS UPSTREAM_INTERLOCK_REF,
            CAST('ANCHOR' AS NVARCHAR(20))     AS Direction
        FROM First_Fault.dbo.FF_INTERLOCK_LOG il
        INNER JOIN First_Fault.dbo.INTERLOCK_DEFINITION idef
            ON il.INTERLOCK_DEF_ID = idef.INTERLOCK_DEF_ID
        INNER JOIN First_Fault.dbo.PLC p
            ON idef.PLC_ID = p.PLC_ID
        INNER JOIN First_Fault.dbo.TEXT_DEFINITION td_interlock
            ON idef.TEXT_DEF_ID = td_interlock.TEXT_DEF_ID
        INNER JOIN AnchorInterlock a
            ON il.ID = a.AnchorID

        UNION ALL

        SELECT
            uc.AnchorReference,
            uc.Level - 1,
            upstream_il.ID,
            upstream_il.TIMESTAMP,
            uc.Date,
            p.PLC_NAME,
            idef.NUMBER,
            td_interlock.MESSAGE,
            upstream_il.UPSTREAM_INTERLOCK_LOG_ID,
            CAST('UPSTREAM' AS NVARCHAR(20))
        FROM UpstreamChain uc
        INNER JOIN First_Fault.dbo.FF_INTERLOCK_LOG upstream_il
            ON uc.UPSTREAM_INTERLOCK_REF = upstream_il.ID
        INNER JOIN First_Fault.dbo.INTERLOCK_DEFINITION idef
            ON upstream_il.INTERLOCK_DEF_ID = idef.INTERLOCK_DEF_ID
        INNER JOIN First_Fault.dbo.PLC p
            ON idef.PLC_ID = p.PLC_ID
        INNER JOIN First_Fault.dbo.TEXT_DEFINITION td_interlock
            ON idef.TEXT_DEF_ID = td_interlock.TEXT_DEF_ID
        WHERE uc.UPSTREAM_INTERLOCK_REF IS NOT NULL
          AND uc.Level > -100
    ),
    DownstreamChain AS (
        SELECT
            il.ID                              AS AnchorReference,
            0                                  AS Level,
            il.ID,
            il.TIMESTAMP,
            CAST(il.TIMESTAMP AS DATE)         AS Date,
            p.PLC_NAME                         AS PLC,
            idef.NUMBER                        AS BSID,
            td_interlock.MESSAGE               AS Interlock_Message,
            il.UPSTREAM_INTERLOCK_LOG_ID       AS UPSTREAM_INTERLOCK_REF,
            CAST('ANCHOR' AS NVARCHAR(20))     AS Direction
        FROM First_Fault.dbo.FF_INTERLOCK_LOG il
        INNER JOIN First_Fault.dbo.INTERLOCK_DEFINITION idef
            ON il.INTERLOCK_DEF_ID = idef.INTERLOCK_DEF_ID
        INNER JOIN First_Fault.dbo.PLC p
            ON idef.PLC_ID = p.PLC_ID
        INNER JOIN First_Fault.dbo.TEXT_DEFINITION td_interlock
            ON idef.TEXT_DEF_ID = td_interlock.TEXT_DEF_ID
        INNER JOIN AnchorInterlock a
            ON il.ID = a.AnchorID

        UNION ALL

        SELECT
            dc.AnchorReference,
            dc.Level + 1,
            downstream_il.ID,
            downstream_il.TIMESTAMP,
            dc.Date,
            p.PLC_NAME,
            idef.NUMBER,
            td_interlock.MESSAGE,
            downstream_il.UPSTREAM_INTERLOCK_LOG_ID,
            CAST('DOWNSTREAM' AS NVARCHAR(20))
        FROM DownstreamChain dc
        INNER JOIN First_Fault.dbo.FF_INTERLOCK_LOG downstream_il
            ON dc.ID = downstream_il.UPSTREAM_INTERLOCK_LOG_ID
        INNER JOIN First_Fault.dbo.INTERLOCK_DEFINITION idef
            ON downstream_il.INTERLOCK_DEF_ID = idef.INTERLOCK_DEF_ID
        INNER JOIN First_Fault.dbo.PLC p
            ON idef.PLC_ID = p.PLC_ID
        INNER JOIN First_Fault.dbo.TEXT_DEFINITION td_interlock
            ON idef.TEXT_DEF_ID = td_interlock.TEXT_DEF_ID
        WHERE dc.Level < 100
    ),
    CombinedChain AS (
        SELECT * FROM UpstreamChain
        UNION ALL
        SELECT * FROM DownstreamChain
        WHERE Direction <> 'ANCHOR'
    )
    SELECT
        cc.AnchorReference,
        cc.Date,
        cc.Level - MAX(cc.Level) OVER (PARTITION BY cc.AnchorReference) AS Level,
        cc.Direction,
        cc.ID as Interlock_Log_ID,
        cc.TIMESTAMP,
        cc.PLC,
        cc.BSID,
        cc.Interlock_Message,
        cdef.TYPE,
        cdef.BIT_INDEX,
        td_condition.MNEMONIC as Condition_Mnemonic,
        CASE
            WHEN td_condition.MESSAGE IS NULL OR LTRIM(RTRIM(td_condition.MESSAGE)) = ''
            THEN td_condition.MNEMONIC
            ELSE td_condition.MESSAGE
        END as Condition_Message,
        cc.UPSTREAM_INTERLOCK_REF,
        CASE
            WHEN cc.UPSTREAM_INTERLOCK_REF IS NULL AND cc.Direction = 'UPSTREAM' THEN '*** ROOT CAUSE ***'
            WHEN cc.Direction = 'ANCHOR' THEN '*** STARTING POINT ***'
            WHEN cc.Direction = 'DOWNSTREAM' THEN 'EFFECT'
            ELSE ''
        END as Status
    FROM CombinedChain cc
    LEFT JOIN First_Fault.dbo.FF_CONDITION_LOG cl
        ON cc.ID = cl.INTERLOCK_LOG_ID
    LEFT JOIN First_Fault.dbo.CONDITION_DEFINITION cdef
        ON cl.CONDITION_DEF_ID = cdef.CONDITION_DEF_ID
    LEFT JOIN First_Fault.dbo.TEXT_DEFINITION td_condition
        ON cdef.TEXT_DEF_ID = td_condition.TEXT_DEF_ID
);
GO

-- ============================================================================
-- NEW version (current prod_sql.sql: no DISTINCT, + AnchorTimestamp)
-- ============================================================================
CREATE FUNCTION dbo.fn_InterlockChain_NEW (
    @TargetBSID INT = NULL,
    @TopN INT = NULL,
    @FilterTimestampStart DATETIME = NULL,
    @FilterTimestampEnd DATETIME = NULL,
    @FilterConditionMessage NVARCHAR(255) = NULL,
    @FilterPLC NVARCHAR(50) = NULL
)
RETURNS TABLE
AS
RETURN
(
    WITH AnchorInterlock AS (
        SELECT TOP (ISNULL(@TopN, 100))
            il.ID          AS AnchorID,
            il.TIMESTAMP   AS AnchorTimestamp,
            CAST(il.TIMESTAMP AS DATE) AS AnchorDate,
            il.ORDER_LOG   AS AnchorOrderLog
        FROM First_Fault.dbo.FF_INTERLOCK_LOG il
        INNER JOIN First_Fault.dbo.INTERLOCK_DEFINITION idef
            ON il.INTERLOCK_DEF_ID = idef.INTERLOCK_DEF_ID
        INNER JOIN First_Fault.dbo.PLC p
            ON idef.PLC_ID = p.PLC_ID
        WHERE (@TargetBSID IS NULL OR idef.NUMBER = @TargetBSID)
            AND (@TargetBSID IS NOT NULL OR il.UPSTREAM_INTERLOCK_LOG_ID IS NULL)
            AND (
                @FilterTimestampStart IS NULL
                OR il.TIMESTAMP >= CASE
                    WHEN CAST(@FilterTimestampStart AS TIME) = '00:00:00.000'
                    THEN CAST(CAST(@FilterTimestampStart AS DATE) AS DATETIME)
                    ELSE @FilterTimestampStart
                END
            )
            AND (
                @FilterTimestampEnd IS NULL
                OR il.TIMESTAMP <= CASE
                    WHEN CAST(@FilterTimestampEnd AS TIME) = '00:00:00.000'
                    THEN DATEADD(SECOND, -1, DATEADD(DAY, 1, CAST(CAST(@FilterTimestampEnd AS DATE) AS DATETIME)))
                    ELSE @FilterTimestampEnd
                END
            )
            AND (@FilterPLC IS NULL OR p.PLC_NAME = @FilterPLC)
            AND (@FilterConditionMessage IS NULL
                OR EXISTS (
                    SELECT 1
                    FROM First_Fault.dbo.FF_CONDITION_LOG cl2
                    INNER JOIN First_Fault.dbo.CONDITION_DEFINITION cdef2
                        ON cl2.CONDITION_DEF_ID = cdef2.CONDITION_DEF_ID
                    INNER JOIN First_Fault.dbo.TEXT_DEFINITION td_condition2
                        ON cdef2.TEXT_DEF_ID = td_condition2.TEXT_DEF_ID
                    WHERE cl2.INTERLOCK_LOG_ID = il.ID
                        AND (td_condition2.MESSAGE LIKE '%' + @FilterConditionMessage + '%'
                             OR td_condition2.MNEMONIC LIKE '%' + @FilterConditionMessage + '%')
                ))
        ORDER BY il.TIMESTAMP DESC, il.ORDER_LOG DESC, il.ID DESC
    ),
    UpstreamChain AS (
        SELECT
            il.ID                              AS AnchorReference,
            a.AnchorTimestamp                  AS AnchorTimestamp,
            0                                  AS Level,
            il.ID,
            il.TIMESTAMP,
            CAST(il.TIMESTAMP AS DATE)         AS Date,
            p.PLC_NAME                         AS PLC,
            idef.NUMBER                        AS BSID,
            td_interlock.MESSAGE               AS Interlock_Message,
            il.UPSTREAM_INTERLOCK_LOG_ID       AS UPSTREAM_INTERLOCK_REF,
            CAST('ANCHOR' AS NVARCHAR(20))     AS Direction
        FROM First_Fault.dbo.FF_INTERLOCK_LOG il
        INNER JOIN First_Fault.dbo.INTERLOCK_DEFINITION idef
            ON il.INTERLOCK_DEF_ID = idef.INTERLOCK_DEF_ID
        INNER JOIN First_Fault.dbo.PLC p
            ON idef.PLC_ID = p.PLC_ID
        INNER JOIN First_Fault.dbo.TEXT_DEFINITION td_interlock
            ON idef.TEXT_DEF_ID = td_interlock.TEXT_DEF_ID
        INNER JOIN AnchorInterlock a
            ON il.ID = a.AnchorID

        UNION ALL

        SELECT
            uc.AnchorReference,
            uc.AnchorTimestamp,
            uc.Level - 1,
            upstream_il.ID,
            upstream_il.TIMESTAMP,
            uc.Date,
            p.PLC_NAME,
            idef.NUMBER,
            td_interlock.MESSAGE,
            upstream_il.UPSTREAM_INTERLOCK_LOG_ID,
            CAST('UPSTREAM' AS NVARCHAR(20))
        FROM UpstreamChain uc
        INNER JOIN First_Fault.dbo.FF_INTERLOCK_LOG upstream_il
            ON uc.UPSTREAM_INTERLOCK_REF = upstream_il.ID
        INNER JOIN First_Fault.dbo.INTERLOCK_DEFINITION idef
            ON upstream_il.INTERLOCK_DEF_ID = idef.INTERLOCK_DEF_ID
        INNER JOIN First_Fault.dbo.PLC p
            ON idef.PLC_ID = p.PLC_ID
        INNER JOIN First_Fault.dbo.TEXT_DEFINITION td_interlock
            ON idef.TEXT_DEF_ID = td_interlock.TEXT_DEF_ID
        WHERE uc.UPSTREAM_INTERLOCK_REF IS NOT NULL
          AND uc.Level > -100
    ),
    DownstreamChain AS (
        SELECT
            il.ID                              AS AnchorReference,
            a.AnchorTimestamp                  AS AnchorTimestamp,
            0                                  AS Level,
            il.ID,
            il.TIMESTAMP,
            CAST(il.TIMESTAMP AS DATE)         AS Date,
            p.PLC_NAME                         AS PLC,
            idef.NUMBER                        AS BSID,
            td_interlock.MESSAGE               AS Interlock_Message,
            il.UPSTREAM_INTERLOCK_LOG_ID       AS UPSTREAM_INTERLOCK_REF,
            CAST('ANCHOR' AS NVARCHAR(20))     AS Direction
        FROM First_Fault.dbo.FF_INTERLOCK_LOG il
        INNER JOIN First_Fault.dbo.INTERLOCK_DEFINITION idef
            ON il.INTERLOCK_DEF_ID = idef.INTERLOCK_DEF_ID
        INNER JOIN First_Fault.dbo.PLC p
            ON idef.PLC_ID = p.PLC_ID
        INNER JOIN First_Fault.dbo.TEXT_DEFINITION td_interlock
            ON idef.TEXT_DEF_ID = td_interlock.TEXT_DEF_ID
        INNER JOIN AnchorInterlock a
            ON il.ID = a.AnchorID

        UNION ALL

        SELECT
            dc.AnchorReference,
            dc.AnchorTimestamp,
            dc.Level + 1,
            downstream_il.ID,
            downstream_il.TIMESTAMP,
            dc.Date,
            p.PLC_NAME,
            idef.NUMBER,
            td_interlock.MESSAGE,
            downstream_il.UPSTREAM_INTERLOCK_LOG_ID,
            CAST('DOWNSTREAM' AS NVARCHAR(20))
        FROM DownstreamChain dc
        INNER JOIN First_Fault.dbo.FF_INTERLOCK_LOG downstream_il
            ON dc.ID = downstream_il.UPSTREAM_INTERLOCK_LOG_ID
        INNER JOIN First_Fault.dbo.INTERLOCK_DEFINITION idef
            ON downstream_il.INTERLOCK_DEF_ID = idef.INTERLOCK_DEF_ID
        INNER JOIN First_Fault.dbo.PLC p
            ON idef.PLC_ID = p.PLC_ID
        INNER JOIN First_Fault.dbo.TEXT_DEFINITION td_interlock
            ON idef.TEXT_DEF_ID = td_interlock.TEXT_DEF_ID
        WHERE dc.Level < 100
    ),
    CombinedChain AS (
        SELECT * FROM UpstreamChain
        UNION ALL
        SELECT * FROM DownstreamChain
        WHERE Direction <> 'ANCHOR'
    )
    SELECT
        cc.AnchorReference,
        cc.AnchorTimestamp,
        cc.Date,
        cc.Level - MAX(cc.Level) OVER (PARTITION BY cc.AnchorReference) AS Level,
        cc.Direction,
        cc.ID as Interlock_Log_ID,
        cc.TIMESTAMP,
        cc.PLC,
        cc.BSID,
        cc.Interlock_Message,
        cdef.TYPE,
        cdef.BIT_INDEX,
        td_condition.MNEMONIC as Condition_Mnemonic,
        CASE
            WHEN td_condition.MESSAGE IS NULL OR LTRIM(RTRIM(td_condition.MESSAGE)) = ''
            THEN td_condition.MNEMONIC
            ELSE td_condition.MESSAGE
        END as Condition_Message,
        cc.UPSTREAM_INTERLOCK_REF,
        CASE
            WHEN cc.UPSTREAM_INTERLOCK_REF IS NULL AND cc.Direction = 'UPSTREAM' THEN '*** ROOT CAUSE ***'
            WHEN cc.Direction = 'ANCHOR' THEN '*** STARTING POINT ***'
            WHEN cc.Direction = 'DOWNSTREAM' THEN 'EFFECT'
            ELSE ''
        END as Status
    FROM CombinedChain cc
    LEFT JOIN First_Fault.dbo.FF_CONDITION_LOG cl
        ON cc.ID = cl.INTERLOCK_LOG_ID
    LEFT JOIN First_Fault.dbo.CONDITION_DEFINITION cdef
        ON cl.CONDITION_DEF_ID = cdef.CONDITION_DEF_ID
    LEFT JOIN First_Fault.dbo.TEXT_DEFINITION td_condition
        ON cdef.TEXT_DEF_ID = td_condition.TEXT_DEF_ID
);
GO

-- ============================================================================
-- Comparison harness — runs several parameter sets and counts row diffs
-- on the 15 SHARED columns. Expected: every row 0 / 0.
-- ============================================================================
SET NOCOUNT ON;

IF OBJECT_ID('tempdb..#Results') IS NOT NULL DROP TABLE #Results;
CREATE TABLE #Results (
    TestCase    NVARCHAR(100),
    OldRowCount INT,
    NewRowCount INT,
    OnlyInOld   INT,   -- rows in OLD not in NEW (on shared cols)
    OnlyInNew   INT    -- rows in NEW not in OLD (on shared cols)
);

DECLARE @TargetBSID INT, @TopN INT, @StartTS DATETIME, @EndTS DATETIME,
        @CondMsg NVARCHAR(255), @PLC NVARCHAR(50), @Case NVARCHAR(100);

-- Reusable block via a cursor over a parameter table -------------------------
DECLARE @Params TABLE (
    seq INT IDENTITY(1,1), Caption NVARCHAR(100),
    BSID INT, TopN INT, StartTS DATETIME, EndTS DATETIME, CondMsg NVARCHAR(255), PLC NVARCHAR(50)
);

INSERT INTO @Params (Caption, BSID, TopN, StartTS, EndTS, CondMsg, PLC) VALUES
    ('default (all NULL)',        NULL, NULL, NULL, NULL, NULL, NULL),
    ('TopN = 5',                  NULL, 5,    NULL, NULL, NULL, NULL),
    ('TopN = 50',                 NULL, 50,   NULL, NULL, NULL, NULL),
    ('BSID only (first found)',   NULL, NULL, NULL, NULL, NULL, NULL); -- BSID filled below

-- Fill the BSID test case with a real BSID if one exists
UPDATE p
SET p.BSID = (SELECT TOP 1 idef.NUMBER
              FROM First_Fault.dbo.FF_INTERLOCK_LOG il
              INNER JOIN First_Fault.dbo.INTERLOCK_DEFINITION idef
                  ON il.INTERLOCK_DEF_ID = idef.INTERLOCK_DEF_ID
              ORDER BY il.TIMESTAMP DESC)
FROM @Params p
WHERE p.Caption = 'BSID only (first found)';

-- Add a PLC-filtered case using a real PLC name if one exists
INSERT INTO @Params (Caption, BSID, TopN, StartTS, EndTS, CondMsg, PLC)
SELECT TOP 1 'PLC filter = ' + p.PLC_NAME, NULL, 20, NULL, NULL, NULL, p.PLC_NAME
FROM First_Fault.dbo.PLC p;

DECLARE cur CURSOR LOCAL FAST_FORWARD FOR
    SELECT Caption, BSID, TopN, StartTS, EndTS, CondMsg, PLC FROM @Params ORDER BY seq;
OPEN cur;
FETCH NEXT FROM cur INTO @Case, @TargetBSID, @TopN, @StartTS, @EndTS, @CondMsg, @PLC;

WHILE @@FETCH_STATUS = 0
BEGIN
    ;WITH O AS (
        SELECT AnchorReference, Date, Level, Direction, Interlock_Log_ID, TIMESTAMP,
               PLC, BSID, Interlock_Message, TYPE, BIT_INDEX, Condition_Mnemonic,
               Condition_Message, UPSTREAM_INTERLOCK_REF, Status
        FROM dbo.fn_InterlockChain_OLD(@TargetBSID, @TopN, @StartTS, @EndTS, @CondMsg, @PLC)
    ),
    N AS (
        SELECT AnchorReference, Date, Level, Direction, Interlock_Log_ID, TIMESTAMP,
               PLC, BSID, Interlock_Message, TYPE, BIT_INDEX, Condition_Mnemonic,
               Condition_Message, UPSTREAM_INTERLOCK_REF, Status
        FROM dbo.fn_InterlockChain_NEW(@TargetBSID, @TopN, @StartTS, @EndTS, @CondMsg, @PLC)
    )
    INSERT INTO #Results (TestCase, OldRowCount, NewRowCount, OnlyInOld, OnlyInNew)
    SELECT
        @Case,
        (SELECT COUNT(*) FROM O),
        (SELECT COUNT(*) FROM N),
        (SELECT COUNT(*) FROM (SELECT * FROM O EXCEPT SELECT * FROM N) d),
        (SELECT COUNT(*) FROM (SELECT * FROM N EXCEPT SELECT * FROM O) d);

    FETCH NEXT FROM cur INTO @Case, @TargetBSID, @TopN, @StartTS, @EndTS, @CondMsg, @PLC;
END
CLOSE cur;
DEALLOCATE cur;

-- ============================================================================
-- Report
-- ============================================================================
SELECT
    TestCase,
    OldRowCount,
    NewRowCount,
    OnlyInOld,
    OnlyInNew,
    CASE WHEN OnlyInOld = 0 AND OnlyInNew = 0 AND OldRowCount = NewRowCount
         THEN 'PASS' ELSE 'FAIL' END AS Verdict
FROM #Results
ORDER BY TestCase;

DECLARE @Failures INT = (SELECT COUNT(*) FROM #Results
                         WHERE OnlyInOld <> 0 OR OnlyInNew <> 0 OR OldRowCount <> NewRowCount);
IF @Failures = 0
    PRINT '=== ALL TESTS PASSED: OLD and NEW produce identical rows on the 15 shared columns. ===';
ELSE
    PRINT '=== ' + CAST(@Failures AS VARCHAR) + ' TEST(S) FAILED — outputs differ. ===';

-- ============================================================================
-- Cleanup
-- ============================================================================
DROP TABLE #Results;
GO
IF OBJECT_ID('dbo.fn_InterlockChain_OLD', 'TF') IS NOT NULL DROP FUNCTION dbo.fn_InterlockChain_OLD;
GO
IF OBJECT_ID('dbo.fn_InterlockChain_NEW', 'TF') IS NOT NULL DROP FUNCTION dbo.fn_InterlockChain_NEW;
GO
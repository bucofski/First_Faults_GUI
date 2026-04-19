/*
================================================================================
TEST FILE: Optimized version of dbo.fn_InterlockChain
================================================================================

Creates a side-by-side function dbo.fn_InterlockChain_Opt so you can compare
outputs and performance against dbo.fn_InterlockChain without touching prod.

Optimizations applied (semantics-preserving):
  1. DownstreamChain base case no longer joins INTERLOCK_DEFINITION / PLC /
     TEXT_DEFINITION / FF_INTERLOCK_LOG. Those columns are discarded later
     by "WHERE Direction <> 'ANCHOR'" in CombinedChain. Base now pulls
     AnchorID and AnchorDate directly from AnchorInterlock.
  2. Removed redundant DISTINCT from AnchorInterlock (il.ID is unique and
     both joins are many-to-one on PKs, so no duplicates can arise).

After deployment, run the verification block at the bottom of this file to
confirm the two functions return identical rows for representative inputs.
================================================================================
*/

-- Drop if exists
IF OBJECT_ID('dbo.fn_InterlockChain', 'TF') IS NOT NULL
    DROP FUNCTION dbo.fn_InterlockChain_Opt;
GO

CREATE FUNCTION dbo.fn_InterlockChain_Opt (
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
        -- OPT #2: DISTINCT removed — il.ID is unique; joins are FK many-to-one.
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
        -- Base: the anchor row itself (kept in final output).
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
        -- OPT #1: base case pulls only from AnchorInterlock.
        -- Columns besides AnchorReference/Level/ID/Date are never read:
        --   * recursive step only references dc.AnchorReference, dc.Level,
        --     dc.ID, dc.Date
        --   * the ANCHOR row is discarded in CombinedChain via
        --     WHERE Direction <> 'ANCHOR'
        SELECT
            a.AnchorID                          AS AnchorReference,
            0                                   AS Level,
            a.AnchorID                          AS ID,
            CAST(NULL AS DATETIME)              AS TIMESTAMP,
            a.AnchorDate                        AS Date,
            CAST(NULL AS NVARCHAR(50))          AS PLC,
            CAST(NULL AS INT)                   AS BSID,
            CAST(NULL AS NVARCHAR(500))         AS Interlock_Message,
            CAST(NULL AS INT)                   AS UPSTREAM_INTERLOCK_REF,
            CAST('ANCHOR' AS NVARCHAR(20))      AS Direction
        FROM AnchorInterlock a

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

/*
================================================================================
VERIFICATION — run these to confirm identical output vs. dbo.fn_InterlockChain
================================================================================

Each block should return 0 rows (i.e. no differences). If any block returns
rows, outputs diverged for that input set.

The column list mirrors the final SELECT of both functions. EXCEPT is order-
independent, so we don't need ORDER BY here.
*/

-- Test 1: default (last 100 root interlocks)
;WITH a AS (SELECT * FROM dbo.fn_InterlockChain(NULL, NULL, NULL, NULL, NULL, NULL)),
      b AS (SELECT * FROM dbo.fn_InterlockChain_Opt(NULL, NULL, NULL, NULL, NULL, NULL))
SELECT 'Test 1: in original not in opt' AS diff, * FROM a EXCEPT SELECT 'Test 1: in original not in opt', * FROM b
UNION ALL
SELECT 'Test 1: in opt not in original', * FROM b EXCEPT SELECT 'Test 1: in opt not in original', * FROM a;

-- Test 2: small TopN
;WITH a AS (SELECT * FROM dbo.fn_InterlockChain(NULL, 5, NULL, NULL, NULL, NULL)),
      b AS (SELECT * FROM dbo.fn_InterlockChain_Opt(NULL, 5, NULL, NULL, NULL, NULL))
SELECT * FROM a EXCEPT SELECT * FROM b
UNION ALL
SELECT * FROM b EXCEPT SELECT * FROM a;

-- Test 3: specific BSID (replace 12345 with a real BSID in your data)
;WITH a AS (SELECT * FROM dbo.fn_InterlockChain(12345, NULL, NULL, NULL, NULL, NULL)),
      b AS (SELECT * FROM dbo.fn_InterlockChain_Opt(12345, NULL, NULL, NULL, NULL, NULL))
SELECT * FROM a EXCEPT SELECT * FROM b
UNION ALL
SELECT * FROM b EXCEPT SELECT * FROM a;

-- Test 4: date range (replace with a real range in your data)
;WITH a AS (SELECT * FROM dbo.fn_InterlockChain(NULL, 20, '2025-11-25', '2025-11-25', NULL, NULL)),
      b AS (SELECT * FROM dbo.fn_InterlockChain_Opt(NULL, 20, '2025-11-25', '2025-11-25', NULL, NULL))
SELECT * FROM a EXCEPT SELECT * FROM b
UNION ALL
SELECT * FROM b EXCEPT SELECT * FROM a;

-- Test 5: PLC + condition message filters
;WITH a AS (SELECT * FROM dbo.fn_InterlockChain(NULL, NULL, NULL, NULL, 'GeenStop', 'TDS')),
      b AS (SELECT * FROM dbo.fn_InterlockChain_Opt(NULL, NULL, NULL, NULL, 'GeenStop', 'TDS'))
SELECT * FROM a EXCEPT SELECT * FROM b
UNION ALL
SELECT * FROM b EXCEPT SELECT * FROM a;

-- Row-count sanity check across the tests above
SELECT 'Test 1' AS test,
       (SELECT COUNT(*) FROM dbo.fn_InterlockChain(NULL, NULL, NULL, NULL, NULL, NULL))   AS original_rows,
       (SELECT COUNT(*) FROM dbo.fn_InterlockChain_Opt(NULL, NULL, NULL, NULL, NULL, NULL)) AS opt_rows
UNION ALL
SELECT 'Test 2',
       (SELECT COUNT(*) FROM dbo.fn_InterlockChain(NULL, 5, NULL, NULL, NULL, NULL)),
       (SELECT COUNT(*) FROM dbo.fn_InterlockChain_Opt(NULL, 5, NULL, NULL, NULL, NULL))
UNION ALL
SELECT 'Test 5',
       (SELECT COUNT(*) FROM dbo.fn_InterlockChain(NULL, NULL, NULL, NULL, 'GeenStop', 'TDS')),
       (SELECT COUNT(*) FROM dbo.fn_InterlockChain_Opt(NULL, NULL, NULL, NULL, 'GeenStop', 'TDS'));

/*
================================================================================
PERFORMANCE COMPARISON — BSID 1208
================================================================================
Runs each function twice: the first run warms the cache, the second is the
fair comparison. Results are dumped into #tmp tables so timing measures the
server-side work and not the client-side row rendering.

Run the whole block once and compare the "RUN 2" elapsed_ms values.
================================================================================
*/

SET NOCOUNT ON;

IF OBJECT_ID('tempdb..#tmp_orig1') IS NOT NULL DROP TABLE #tmp_orig1;
IF OBJECT_ID('tempdb..#tmp_opt1')  IS NOT NULL DROP TABLE #tmp_opt1;
IF OBJECT_ID('tempdb..#tmp_orig2') IS NOT NULL DROP TABLE #tmp_orig2;
IF OBJECT_ID('tempdb..#tmp_opt2')  IS NOT NULL DROP TABLE #tmp_opt2;

DECLARE @t0 DATETIME2(7), @t1 DATETIME2(7);
DECLARE @results TABLE (run_label VARCHAR(40), row_count INT, elapsed_ms DECIMAL(12,3));

-- ORIGINAL — run 1 (cold)
SET @t0 = SYSDATETIME();
SELECT * INTO #tmp_orig1 FROM dbo.fn_InterlockChain(1208, NULL, NULL, NULL, NULL, NULL);
SET @t1 = SYSDATETIME();
INSERT INTO @results VALUES ('ORIGINAL  run 1 (cold)', (SELECT COUNT(*) FROM #tmp_orig1),
                             DATEDIFF(MICROSECOND, @t0, @t1) / 1000.0);

-- ORIGINAL — run 2 (warm)
SET @t0 = SYSDATETIME();
SELECT * INTO #tmp_orig2 FROM dbo.fn_InterlockChain(1208, NULL, NULL, NULL, NULL, NULL);
SET @t1 = SYSDATETIME();
INSERT INTO @results VALUES ('ORIGINAL  run 2 (warm)', (SELECT COUNT(*) FROM #tmp_orig2),
                             DATEDIFF(MICROSECOND, @t0, @t1) / 1000.0);


DROP TABLE #tmp_orig1, #tmp_opt1, #tmp_orig2, #tmp_opt2;

SELECT * FROM @results;

SET NOCOUNT OFF;

SET STATISTICS IO OFF;
SET STATISTICS TIME OFF;

/*
================================================================================
CLEANUP — drop the test function when done
================================================================================
-- DROP FUNCTION dbo.fn_InterlockChain_Opt;
*/
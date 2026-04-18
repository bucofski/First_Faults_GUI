-- ============================================================================
-- TEST: Anchor selection fix v2 — strict link-based only
--
-- Problem: When filtering by date/PLC/topN (no BSID), interlocks that happen
--   at the same time get cross-linked. The old query used a ±5s time window
--   for downstream and treated non-root interlocks as anchors.
--
-- Fix:
--   1. Anchors = ONLY interlocks with UPSTREAM_INTERLOCK_LOG_ID IS NULL
--      (true root faults, no time-based guessing)
--   2. Downstream = follow ONLY explicit UPSTREAM_INTERLOCK_LOG_ID pointers
--      (removed the ±5 second time window)
--   3. Upstream = follow UPSTREAM_INTERLOCK_LOG_ID chain upward (same as before)
--
-- When @BSID is set: that BSID becomes the anchor regardless of upstream,
--   and both upstream + downstream chains are built from it.
--
-- Adjust variables at the top to test.
-- ============================================================================

DECLARE @TopN     INT            = 20;
DECLARE @StartTS  DATETIME       = NULL;  -- e.g. '2025-11-25'
DECLARE @EndTS    DATETIME       = NULL;  -- e.g. '2025-11-25'
DECLARE @PLC      NVARCHAR(50)   = NULL;  -- e.g. 'TDS'
DECLARE @BSID     INT            = NULL;  -- e.g. 11710
DECLARE @CondMsg  NVARCHAR(255)  = NULL;  -- e.g. 'GeenStop'

; WITH AnchorInterlock AS (
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
    WHERE (@BSID IS NULL OR idef.NUMBER = @BSID)
      -- When no BSID: only true root interlocks (no upstream parent)
      AND (@BSID IS NOT NULL OR il.UPSTREAM_INTERLOCK_LOG_ID IS NULL)
      -- Timestamp filters
      AND (@StartTS IS NULL OR il.TIMESTAMP >= CASE
              WHEN CAST(@StartTS AS TIME) = '00:00:00.000'
              THEN CAST(CAST(@StartTS AS DATE) AS DATETIME)
              ELSE @StartTS END)
      AND (@EndTS IS NULL OR il.TIMESTAMP <= CASE
              WHEN CAST(@EndTS AS TIME) = '00:00:00.000'
              THEN DATEADD(SECOND, -1, DATEADD(DAY, 1, CAST(CAST(@EndTS AS DATE) AS DATETIME)))
              ELSE @EndTS END)
      AND (@PLC IS NULL OR p.PLC_NAME = @PLC)
      -- Condition message filter
      AND (@CondMsg IS NULL
           OR EXISTS (
               SELECT 1
               FROM First_Fault.dbo.FF_CONDITION_LOG cl2
               INNER JOIN First_Fault.dbo.CONDITION_DEFINITION cdef2
                   ON cl2.CONDITION_DEF_ID = cdef2.CONDITION_DEF_ID
               INNER JOIN First_Fault.dbo.TEXT_DEFINITION td_condition2
                   ON cdef2.TEXT_DEF_ID = td_condition2.TEXT_DEF_ID
               WHERE cl2.INTERLOCK_LOG_ID = il.ID
                 AND (td_condition2.MESSAGE  LIKE '%' + @CondMsg + '%'
                      OR td_condition2.MNEMONIC LIKE '%' + @CondMsg + '%')
           ))
    ORDER BY il.TIMESTAMP DESC, il.ORDER_LOG DESC, il.ID DESC
),

-- ============================================================================
-- UPSTREAM: follow UPSTREAM_INTERLOCK_LOG_ID chain upward from anchor
-- ============================================================================
UpstreamChain AS (
    -- Level 0 = the anchor itself
    SELECT
        il.ID                              AS AnchorReference,
        il.TIMESTAMP                       AS AnchorTimestamp,
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

    -- Recursive: follow upstream (Level +1, +2, …)
    SELECT
        uc.AnchorReference,
        uc.AnchorTimestamp,
        uc.Level + 1,
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
      AND uc.Level < 100
),

-- ============================================================================
-- DOWNSTREAM: follow ONLY explicit UPSTREAM_INTERLOCK_LOG_ID links downward
-- (no time window — purely relationship-based)
-- ============================================================================
DownstreamChain AS (
    -- Level 0 = same anchor starting point
    SELECT
        il.ID                              AS AnchorReference,
        il.TIMESTAMP                       AS AnchorTimestamp,
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

    -- Recursive: find interlocks whose UPSTREAM_INTERLOCK_LOG_ID points to current row
    SELECT
        dc.AnchorReference,
        dc.AnchorTimestamp,
        dc.Level - 1,
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
    WHERE dc.Level > -100
),

-- ============================================================================
-- COMBINE: merge upstream + downstream, avoid duplicate anchor rows
-- ============================================================================
CombinedChain AS (
    SELECT * FROM UpstreamChain
    UNION ALL
    SELECT * FROM DownstreamChain WHERE Direction <> 'ANCHOR'
)

SELECT
    cc.AnchorReference,
    cc.AnchorTimestamp,
    cc.Date,
    cc.Level,
    cc.Direction,
    cc.ID                                                   AS Interlock_Log_ID,
    cc.TIMESTAMP,
    cc.PLC,
    cc.BSID,
    cc.Interlock_Message,
    cdef.TYPE,
    cdef.BIT_INDEX,
    td_condition.MNEMONIC                                   AS Condition_Mnemonic,
    CASE
        WHEN td_condition.MESSAGE IS NULL OR LTRIM(RTRIM(td_condition.MESSAGE)) = ''
        THEN td_condition.MNEMONIC
        ELSE td_condition.MESSAGE
    END                                                     AS Condition_Message,
    cc.UPSTREAM_INTERLOCK_REF,
    CASE
        WHEN cc.UPSTREAM_INTERLOCK_REF IS NULL AND cc.Direction = 'UPSTREAM' THEN '*** ROOT CAUSE ***'
        WHEN cc.Direction = 'ANCHOR'     THEN '*** STARTING POINT ***'
        WHEN cc.Direction = 'DOWNSTREAM' THEN 'EFFECT'
        ELSE ''
    END                                                     AS Status
FROM CombinedChain cc
LEFT JOIN First_Fault.dbo.FF_CONDITION_LOG cl
    ON cc.ID = cl.INTERLOCK_LOG_ID
LEFT JOIN First_Fault.dbo.CONDITION_DEFINITION cdef
    ON cl.CONDITION_DEF_ID = cdef.CONDITION_DEF_ID
LEFT JOIN First_Fault.dbo.TEXT_DEFINITION td_condition
    ON cdef.TEXT_DEF_ID = td_condition.TEXT_DEF_ID
ORDER BY cc.AnchorReference DESC, cc.Level DESC, cc.TIMESTAMP, cdef.TYPE, cdef.BIT_INDEX
OPTION (RECOMPILE);
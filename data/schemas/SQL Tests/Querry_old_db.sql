SELECT TOP 1
    i.[TIMESTAMP],
    i.[MESSAGE] AS BS_comment,
    i.[PLC],
    i.[NUMBER] AS BSID,
    c.[BIT_INDEX] AS VW_index,
    c.[MESSAGE] AS VW_comment,
    c.[UPSTREAM_INTERLOCK_REF]
FROM [TD2].[dbo].[FF_INTERLOCK_LOG] i
LEFT JOIN [TD2].[dbo].[FF_CONDITION_LOG] c
    ON i.ID = c.INTERLOCK_REF
WHERE i.NUMBER = 1300  -- Replace with your parameter or value
ORDER BY i.TIMESTAMP DESC, i.ORDER_LOG DESC


-- Recursive query to test the same logic as the function
-- This shows the complete interlock chain (upstream and downstream)

DECLARE @TargetBSID INT = 11222;  -- Change this to test different BSIDs
DECLARE @TopN INT = 100;  -- Number of anchor interlocks to start from

WITH AnchorInterlock AS (
    -- Get the anchor interlock (most recent by default)
    SELECT TOP (@TopN)
        il.ID as AnchorID,
        il.TIMESTAMP as AnchorTimestamp,
        CAST(il.TIMESTAMP AS DATE) as AnchorDate
    FROM First_Fault.dbo.FF_INTERLOCK_LOG il
    INNER JOIN First_Fault.dbo.INTERLOCK_DEFINITION idef
        ON il.INTERLOCK_DEF_ID = idef.INTERLOCK_DEF_ID
    WHERE idef.NUMBER = @TargetBSID
    ORDER BY il.TIMESTAMP DESC, il.ORDER_LOG DESC
),
UpstreamChain AS (
    -- Anchor: Start with selected interlock
    SELECT
        0 as Level,
        il.ID,
        il.TIMESTAMP,
        CAST(il.TIMESTAMP AS DATE) as Date,
        p.PLC_NAME as PLC,
        idef.NUMBER as BSID,
        td_interlock.MESSAGE as Interlock_Message,
        il.UPSTREAM_INTERLOCK_LOG_ID as UPSTREAM_INTERLOCK_REF,
        CAST('ANCHOR' AS NVARCHAR(20)) as Direction
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

    -- Recursive: Follow upstream references
    SELECT
        uc.Level + 1,
        upstream_il.ID,
        upstream_il.TIMESTAMP,
        uc.Date,
        p.PLC_NAME as PLC,
        idef.NUMBER as BSID,
        td_interlock.MESSAGE as Interlock_Message,
        upstream_il.UPSTREAM_INTERLOCK_LOG_ID as UPSTREAM_INTERLOCK_REF,
        CAST('UPSTREAM' AS NVARCHAR(20)) as Direction
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
DownstreamChain AS (
    -- Start from anchor point
    SELECT
        0 as Level,
        il.ID,
        il.TIMESTAMP,
        CAST(il.TIMESTAMP AS DATE) as Date,
        p.PLC_NAME as PLC,
        idef.NUMBER as BSID,
        td_interlock.MESSAGE as Interlock_Message,
        il.UPSTREAM_INTERLOCK_LOG_ID as UPSTREAM_INTERLOCK_REF,
        CAST('ANCHOR' AS NVARCHAR(20)) as Direction
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

    -- Recursive: Follow downstream references
    SELECT
        dc.Level - 1,
        downstream_il.ID,
        downstream_il.TIMESTAMP,
        dc.Date,
        p.PLC_NAME as PLC,
        idef.NUMBER as BSID,
        td_interlock.MESSAGE as Interlock_Message,
        downstream_il.UPSTREAM_INTERLOCK_LOG_ID as UPSTREAM_INTERLOCK_REF,
        CAST('DOWNSTREAM' AS NVARCHAR(20)) as Direction
    FROM DownstreamChain dc
    INNER JOIN First_Fault.dbo.FF_INTERLOCK_LOG downstream_il
        ON dc.ID = downstream_il.UPSTREAM_INTERLOCK_LOG_ID
    INNER JOIN First_Fault.dbo.INTERLOCK_DEFINITION idef
        ON downstream_il.INTERLOCK_DEF_ID = idef.INTERLOCK_DEF_ID
    INNER JOIN First_Fault.dbo.PLC p
        ON idef.PLC_ID = p.PLC_ID
    INNER JOIN First_Fault.dbo.TEXT_DEFINITION td_interlock
        ON idef.TEXT_DEF_ID = td_interlock.TEXT_DEF_ID
    WHERE downstream_il.TIMESTAMP >= DATEADD(SECOND, -5, dc.TIMESTAMP)
      AND downstream_il.TIMESTAMP <= DATEADD(SECOND, 5, dc.TIMESTAMP)
      AND dc.Level > -100
),
CombinedChain AS (
    SELECT * FROM UpstreamChain
    UNION ALL
    SELECT * FROM DownstreamChain
    WHERE Direction <> 'ANCHOR'
)
SELECT
    cc.Date,
    cc.Level,
    cc.Direction,
    cc.ID as Interlock_Log_ID,
    cc.TIMESTAMP,
    cc.PLC,
    cc.BSID,
    cc.Interlock_Message,
    cdef.TYPE,
    cdef.BIT_INDEX,
    td_condition.MESSAGE as Condition_Message,
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
ORDER BY cc.TIMESTAMP, cc.Level DESC;
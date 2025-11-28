DECLARE @AnchorID INT;
DECLARE @AnchorTimestamp DATETIME;
DECLARE @TargetBSID INT = 11221;  -- Change this to the BSID you want to query

-- Get the anchor interlock
SELECT TOP 10
    @AnchorID = il.ID,
    @AnchorTimestamp = il.TIMESTAMP
FROM First_Fault.dbo.FF_INTERLOCK_LOG il
INNER JOIN First_Fault.dbo.INTERLOCK_DEFINITION idef
    ON il.INTERLOCK_DEF_ID = idef.INTERLOCK_DEF_ID
WHERE idef.NUMBER = @TargetBSID
ORDER BY il.TIMESTAMP DESC, il.ORDER_LOG DESC;

-- Check if we found anything
IF @AnchorID IS NULL
BEGIN
    PRINT 'No interlock found with BSID = ' + CAST(@TargetBSID AS VARCHAR(10));
    PRINT 'Available BSIDs:';
    SELECT DISTINCT idef.NUMBER FROM First_Fault.dbo.INTERLOCK_DEFINITION idef ORDER BY idef.NUMBER;
END
ELSE
BEGIN
    PRINT 'Found anchor interlock ID: ' + CAST(@AnchorID AS VARCHAR(10));
    PRINT 'Timestamp: ' + CAST(@AnchorTimestamp AS VARCHAR(30));

    ;WITH UpstreamChain AS (
        -- Anchor: Start with selected interlock
        SELECT
            0 as Level,
            il.ID,
            il.TIMESTAMP,
            p.PLC_CODE as PLC,
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
        WHERE il.ID = @AnchorID

        UNION ALL

        -- Recursive: Follow upstream references
        SELECT
            uc.Level + 1,
            upstream_il.ID,
            upstream_il.TIMESTAMP,
            p.PLC_CODE as PLC,
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
    ),
    DownstreamChain AS (
        -- Start from anchor point
        SELECT
            0 as Level,
            il.ID,
            il.TIMESTAMP,
            p.PLC_CODE as PLC,
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
        WHERE il.ID = @AnchorID

        UNION ALL

        -- Recursive: Follow downstream references
        SELECT
            dc.Level - 1,
            downstream_il.ID,
            downstream_il.TIMESTAMP,
            p.PLC_CODE as PLC,
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
        WHERE ABS(DATEDIFF(SECOND, dc.TIMESTAMP, downstream_il.TIMESTAMP)) <= 5  -- Within 5 seconds
    ),
    CombinedChain AS (
        SELECT * FROM UpstreamChain
        UNION
        SELECT * FROM DownstreamChain
    )
    -- Add condition details
    SELECT
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
    ORDER BY cc.Level DESC, cc.TIMESTAMP, cdef.TYPE, cdef.BIT_INDEX;
END
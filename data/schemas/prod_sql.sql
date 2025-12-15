-- Drop function if it exists
IF OBJECT_ID('dbo.fn_InterlockChain', 'TF') IS NOT NULL
    DROP FUNCTION dbo.fn_InterlockChain;
GO

CREATE FUNCTION dbo.fn_InterlockChain (
    @TargetBSID INT = NULL,  -- Optional: If NULL, returns last N interlocks with their full trees
    @TopN INT = NULL,  -- If NULL, defaults to 100
    @FilterTimestampStart DATETIME = NULL,  -- Optional: Filter by timestamp range start (can be date or datetime)
    @FilterTimestampEnd DATETIME = NULL,  -- Optional: Filter by timestamp range end (can be date or datetime)
    @FilterConditionMessage NVARCHAR(255) = NULL,  -- Optional: Search text in condition message or mnemonic
    @FilterPLC NVARCHAR(50) = NULL  -- Optional: Filter by PLC name
)
RETURNS TABLE
AS
RETURN
(
    WITH AnchorInterlock AS (
        -- Get DISTINCT anchor interlocks (TOP N based on parameter) with optional filters
        SELECT DISTINCT TOP (ISNULL(@TopN, 100))
            il.ID as AnchorID,
            il.TIMESTAMP as AnchorTimestamp,
            CAST(il.TIMESTAMP AS DATE) as AnchorDate,
            il.ORDER_LOG as AnchorOrderLog
        FROM First_Fault.dbo.FF_INTERLOCK_LOG il
        INNER JOIN First_Fault.dbo.INTERLOCK_DEFINITION idef
            ON il.INTERLOCK_DEF_ID = idef.INTERLOCK_DEF_ID
        INNER JOIN First_Fault.dbo.PLC p
            ON idef.PLC_ID = p.PLC_ID
        WHERE (@TargetBSID IS NULL OR idef.NUMBER = @TargetBSID)
            -- Apply timestamp range filters with intelligent handling
            AND (
                @FilterTimestampStart IS NULL
                OR il.TIMESTAMP >= CASE
                    -- If time component is midnight (00:00:00.000), treat as start of day
                    WHEN CAST(@FilterTimestampStart AS TIME) = '00:00:00.000'
                    THEN CAST(CAST(@FilterTimestampStart AS DATE) AS DATETIME)
                    ELSE @FilterTimestampStart
                END
            )
            AND (
                @FilterTimestampEnd IS NULL
                OR il.TIMESTAMP <= CASE
                    -- If time component is midnight (00:00:00.000), treat as end of that day
                    WHEN CAST(@FilterTimestampEnd AS TIME) = '00:00:00.000'
                    THEN DATEADD(SECOND, -1, DATEADD(DAY, 1, CAST(CAST(@FilterTimestampEnd AS DATE) AS DATETIME)))
                    ELSE @FilterTimestampEnd
                END
            )
            AND (@FilterPLC IS NULL OR p.PLC_NAME = @FilterPLC)
            -- Condition message filter
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
        -- Anchor: Start with selected interlocks
        SELECT
            il.ID as AnchorReference,
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
            uc.AnchorReference,
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
        -- Start from anchor points
        SELECT
            il.ID as AnchorReference,
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
            dc.AnchorReference,
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
        cc.AnchorReference,
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

/*
USAGE EXAMPLES:

-- Get last 10 interlocks (any BSID) with their FULL TREES (NULL defaults to 10):
SELECT * FROM dbo.fn_InterlockChain( NULL, NULL, NULL, NULL, NULL, NULL)
ORDER BY TIMESTAMP DESC, Date DESC, Level;

-- Get specific BSID with its tree (NULL defaults to 10):
SELECT * FROM dbo.fn_InterlockChain(12345,  NULL, NULL, NULL, NULL, NULL)
ORDER BY TIMESTAMP DESC, Date DESC, Level;

-- Get top 5 of specific BSID with trees:
SELECT * FROM dbo.fn_InterlockChain(12345, 5,  NULL, NULL, NULL, NULL)
ORDER BY TIMESTAMP DESC, Date DESC, Level;

-- Filter by date and get trees (NULL defaults to 10):
SELECT * FROM dbo.fn_InterlockChain(NULL,  '2024-12-05', NULL, NULL, NULL, NULL)
ORDER BY TIMESTAMP DESC, Date DESC, Level;

-- Filter by PLC and condition message:
SELECT * FROM dbo.fn_InterlockChain( NULL, NULL, NULL, NULL, 'Emergency', 'PLC_001')
ORDER BY TIMESTAMP DESC, Date DESC, Level;
*/
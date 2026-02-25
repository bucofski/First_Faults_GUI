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
            -- When no BSID: only true root interlocks (no upstream parent)
            AND (@TargetBSID IS NOT NULL OR il.UPSTREAM_INTERLOCK_LOG_ID IS NULL)
            -- Timestamp filters
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
        -- Level 0 = the anchor itself
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

        -- Recursive: follow upstream (Level +1, +2, …)
        SELECT
            uc.AnchorReference,
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
    DownstreamChain AS (
        -- Level 0 = same anchor starting point
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

        -- Recursive: follow ONLY explicit UPSTREAM_INTERLOCK_LOG_ID links downward
        SELECT
            dc.AnchorReference,
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

-- Get last 10 interlocks (any BSID) with their FULL TREES (NULL defaults to 100):
SELECT * FROM dbo.fn_InterlockChain(NULL, NULL, NULL, NULL, NULL, NULL)
ORDER BY AnchorReference DESC, Level DESC, TIMESTAMP;

-- Get specific BSID with its tree:
SELECT * FROM dbo.fn_InterlockChain(12345, NULL, NULL, NULL, NULL, NULL)
ORDER BY AnchorReference DESC, Level DESC, TIMESTAMP;

-- Get top 5 of specific BSID with trees:
SELECT * FROM dbo.fn_InterlockChain(12345, 5, NULL, NULL, NULL, NULL)
ORDER BY AnchorReference DESC, Level DESC, TIMESTAMP;

-- Filter by date range:
SELECT * FROM dbo.fn_InterlockChain(NULL, 20, '2025-11-25', '2025-11-25', NULL, NULL)
ORDER BY AnchorReference DESC, Level DESC, TIMESTAMP;

-- Filter by PLC and condition message:
SELECT * FROM dbo.fn_InterlockChain(NULL, NULL, NULL, NULL, 'GeenStop', 'TDS')
ORDER BY AnchorReference DESC, Level DESC, TIMESTAMP;

-- Filter by date + PLC:
SELECT * FROM dbo.fn_InterlockChain(NULL, 20, '2025-11-25', '2025-11-25', NULL, 'TDS')
ORDER BY AnchorReference DESC, Level DESC, TIMESTAMP;
*/
USE First_Fault;
GO

-- ============================================================================
-- ROOT CAUSE RISERS ANALYSIS
-- Identifies ROOT CAUSES that are occurring more frequently than baseline
-- Filters out "normal" root causes that occur at a steady rate
-- ============================================================================

-- Parameters you can adjust:
DECLARE @CurrentPeriodDays INT = 7;        -- Current period to analyze (e.g., this week)
DECLARE @BaselinePeriodDays INT = 28;      -- Historical baseline period (e.g., 4 weeks)
DECLARE @MinBaselineCount INT = 3;         -- Minimum occurrences needed in baseline
DECLARE @SignificanceThreshold FLOAT = 2.0; -- Must be X times higher than baseline
DECLARE @TopN INT = 10;                    -- Show top N risers

WITH RootCauseInterlocks AS (
    -- Identify all root causes (interlocks with no upstream reference)
    SELECT
        il.ID,
        il.INTERLOCK_DEF_ID,
        il.TIMESTAMP,
        il.UPSTREAM_INTERLOCK_LOG_ID
    FROM First_Fault.dbo.FF_INTERLOCK_LOG il
    WHERE il.UPSTREAM_INTERLOCK_LOG_ID IS NULL  -- This is a root cause
        AND il.TIMESTAMP >= DATEADD(DAY, -@BaselinePeriodDays - @CurrentPeriodDays, GETDATE())
),

RootCauseCounts AS (
    -- Count root cause occurrences in current and baseline periods
    SELECT
        rc.INTERLOCK_DEF_ID,
        p.PLC_NAME,
        id.NUMBER AS BSID,
        td.MNEMONIC,
        td.MESSAGE AS Interlock_Message,

        -- Current period count
        COUNT(CASE
            WHEN rc.TIMESTAMP >= DATEADD(DAY, -@CurrentPeriodDays, GETDATE())
            THEN 1
        END) AS CurrentPeriodCount,

        -- Baseline period count (excluding current period)
        COUNT(CASE
            WHEN rc.TIMESTAMP >= DATEADD(DAY, -@BaselinePeriodDays - @CurrentPeriodDays, GETDATE())
            AND rc.TIMESTAMP < DATEADD(DAY, -@CurrentPeriodDays, GETDATE())
            THEN 1
        END) AS BaselinePeriodCount,

        -- Total count for reference
        COUNT(*) AS TotalCount,

        -- Most recent occurrence
        MAX(rc.TIMESTAMP) AS LastOccurrence,

        -- Most recent root cause ID for drilling down
        MAX(CASE
            WHEN rc.TIMESTAMP >= DATEADD(DAY, -@CurrentPeriodDays, GETDATE())
            THEN rc.ID
        END) AS MostRecentRootCauseID

    FROM RootCauseInterlocks rc
    INNER JOIN First_Fault.dbo.INTERLOCK_DEFINITION id
        ON rc.INTERLOCK_DEF_ID = id.INTERLOCK_DEF_ID
    INNER JOIN First_Fault.dbo.PLC p
        ON id.PLC_ID = p.PLC_ID
    INNER JOIN First_Fault.dbo.TEXT_DEFINITION td
        ON id.TEXT_DEF_ID = td.TEXT_DEF_ID

    GROUP BY
        rc.INTERLOCK_DEF_ID,
        p.PLC_NAME,
        id.NUMBER,
        td.MNEMONIC,
        td.MESSAGE
),

RiserAnalysis AS (
    SELECT
        *,
        -- Calculate daily averages
        CAST(CurrentPeriodCount AS FLOAT) / @CurrentPeriodDays AS CurrentDailyAvg,
        CAST(BaselinePeriodCount AS FLOAT) / @BaselinePeriodDays AS BaselineDailyAvg,

        -- Calculate increase ratio
        CASE
            WHEN BaselinePeriodCount > 0 THEN
                (CAST(CurrentPeriodCount AS FLOAT) / @CurrentPeriodDays) /
                (CAST(BaselinePeriodCount AS FLOAT) / @BaselinePeriodDays)
            ELSE 999.99  -- New root cause (no baseline)
        END AS IncreaseRatio,

        -- Calculate absolute increase
        (CAST(CurrentPeriodCount AS FLOAT) / @CurrentPeriodDays) -
        (CAST(BaselinePeriodCount AS FLOAT) / @BaselinePeriodDays) AS DailyIncrease

    FROM RootCauseCounts

    WHERE
        -- Must have occurred in current period
        CurrentPeriodCount > 0
        -- Filter out steady-state "normal" root causes
        AND (
            -- Either it's significantly higher than baseline
            (BaselinePeriodCount >= @MinBaselineCount
             AND (CAST(CurrentPeriodCount AS FLOAT) / @CurrentPeriodDays) /
                 (CAST(BaselinePeriodCount AS FLOAT) / @BaselinePeriodDays) >= @SignificanceThreshold)
            -- Or it's a new root cause
            OR (BaselinePeriodCount < @MinBaselineCount AND CurrentPeriodCount >= 2)
        )
)

-- ============================================================================
-- FINAL RESULTS: TOP ROOT CAUSE RISERS
-- ============================================================================
SELECT TOP (@TopN)
    RANK() OVER (ORDER BY IncreaseRatio DESC, DailyIncrease DESC) AS [Rank],
    PLC_NAME,
    BSID,
    MNEMONIC,
    Interlock_Message AS [Root_Cause_Message],

    -- Current period stats
    CurrentPeriodCount AS [Count_Last_' + CAST(@CurrentPeriodDays AS VARCHAR) + '_Days],
    CAST(CurrentDailyAvg AS DECIMAL(10,2)) AS [Avg_Per_Day_Current],

    -- Baseline stats
    BaselinePeriodCount AS [Count_Baseline_' + CAST(@BaselinePeriodDays AS VARCHAR) + '_Days],
    CAST(BaselineDailyAvg AS DECIMAL(10,2)) AS [Avg_Per_Day_Baseline],

    -- Change metrics
    CAST(IncreaseRatio AS DECIMAL(10,2)) AS [Increase_Ratio],
    CAST(DailyIncrease AS DECIMAL(10,2)) AS [Daily_Increase],

    -- Status indicator
    CASE
        WHEN BaselinePeriodCount < @MinBaselineCount THEN '🆕 NEW ROOT CAUSE'
        WHEN IncreaseRatio >= 5.0 THEN '🔥 CRITICAL RISER'
        WHEN IncreaseRatio >= 3.0 THEN '⚠️ HIGH RISER'
        ELSE '📈 MODERATE RISER'
    END AS [Status],

    LastOccurrence AS [Last_Occurrence],
    MostRecentRootCauseID AS [Sample_Root_Cause_ID]

FROM RiserAnalysis

ORDER BY
    IncreaseRatio DESC,
    DailyIncrease DESC;

-- ============================================================================
-- DETAILED CONDITIONS FOR TOP RISERS
-- Shows which conditions are triggering most often for each root cause riser
-- ============================================================================
PRINT '';
PRINT '========================================================================';
PRINT 'TOP CONDITIONS FOR ROOT CAUSE RISERS';
PRINT '========================================================================';

WITH RootCauseInterlocks AS (
    SELECT
        il.ID,
        il.INTERLOCK_DEF_ID,
        il.TIMESTAMP
    FROM First_Fault.dbo.FF_INTERLOCK_LOG il
    WHERE il.UPSTREAM_INTERLOCK_LOG_ID IS NULL
        AND il.TIMESTAMP >= DATEADD(DAY, -@CurrentPeriodDays, GETDATE())
),
TopRiserConditions AS (
    SELECT
        p.PLC_NAME,
        id.NUMBER AS BSID,
        itd.MNEMONIC AS Interlock_Mnemonic,
        itd.MESSAGE AS Interlock_Message,
        cdef.TYPE,
        cdef.BIT_INDEX,
        ctd.MNEMONIC AS Condition_Mnemonic,
        ctd.MESSAGE AS Condition_Message,
        COUNT(*) AS ConditionCount,
        MAX(rc.TIMESTAMP) AS LastOccurrence,
        COUNT(DISTINCT rc.ID) AS NumRootCauseEvents
    FROM RootCauseInterlocks rc
    INNER JOIN First_Fault.dbo.FF_CONDITION_LOG cl
        ON rc.ID = cl.INTERLOCK_LOG_ID
    INNER JOIN First_Fault.dbo.CONDITION_DEFINITION cdef
        ON cl.CONDITION_DEF_ID = cdef.CONDITION_DEF_ID
    INNER JOIN First_Fault.dbo.INTERLOCK_DEFINITION id
        ON rc.INTERLOCK_DEF_ID = id.INTERLOCK_DEF_ID
    INNER JOIN First_Fault.dbo.PLC p
        ON id.PLC_ID = p.PLC_ID
    INNER JOIN First_Fault.dbo.TEXT_DEFINITION itd
        ON id.TEXT_DEF_ID = itd.TEXT_DEF_ID
    INNER JOIN First_Fault.dbo.TEXT_DEFINITION ctd
        ON cdef.TEXT_DEF_ID = ctd.TEXT_DEF_ID
    GROUP BY
        p.PLC_NAME,
        id.NUMBER,
        itd.MNEMONIC,
        itd.MESSAGE,
        cdef.TYPE,
        cdef.BIT_INDEX,
        ctd.MNEMONIC,
        ctd.MESSAGE
)
SELECT TOP 20
    PLC_NAME,
    BSID,
    Interlock_Mnemonic,
    Condition_Mnemonic,
    Condition_Message,
    ConditionCount AS [Times_Triggered],
    NumRootCauseEvents AS [Root_Cause_Events],
    LastOccurrence
FROM TopRiserConditions
ORDER BY ConditionCount DESC;

-- ============================================================================
-- USAGE: TO INVESTIGATE A SPECIFIC ROOT CAUSE RISER
-- ============================================================================
PRINT '';
PRINT '========================================================================';
PRINT 'TO INVESTIGATE A SPECIFIC ROOT CAUSE:';
PRINT '';
PRINT 'Use the Sample_Root_Cause_ID from above with fn_InterlockChain:';
PRINT '';
PRINT '  SELECT * FROM dbo.fn_InterlockChain(NULL, NULL, NULL, NULL, NULL, NULL, NULL)';
PRINT '  WHERE AnchorReference = <Sample_Root_Cause_ID>';
PRINT '  ORDER BY Level;';
PRINT '';
PRINT 'Or to see all instances of a specific BSID root cause:';
PRINT '  SELECT * FROM dbo.fn_InterlockChain(<BSID>, 20, NULL, NULL, NULL, NULL, NULL)';
PRINT '  WHERE Status = ''*** ROOT CAUSE ***''';
PRINT '  ORDER BY TIMESTAMP DESC;';
PRINT '========================================================================';
PRINT '';

-- ============================================================================
-- CONFIGURATION OPTIONS
-- ============================================================================
PRINT 'CONFIGURATION:';
PRINT '  Current Period: ' + CAST(@CurrentPeriodDays AS VARCHAR) + ' days';
PRINT '  Baseline Period: ' + CAST(@BaselinePeriodDays AS VARCHAR) + ' days';
PRINT '  Significance Threshold: ' + CAST(@SignificanceThreshold AS VARCHAR) + 'x';
PRINT '  Top N: ' + CAST(@TopN AS VARCHAR);
PRINT '';
PRINT 'For monthly analysis: Set @CurrentPeriodDays = 30, @BaselinePeriodDays = 90';
PRINT '========================================================================';
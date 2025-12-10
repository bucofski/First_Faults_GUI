USE First_Fault;
GO

-- ============================================================================
-- INTERLOCK RISERS ANALYSIS
-- Identifies interlocks that are occurring more frequently than their baseline
-- Filters out "normal" faults that occur at a steady rate
-- ============================================================================

-- Parameters you can adjust:
DECLARE @CurrentPeriodDays INT = 7;        -- Current period to analyze (e.g., this week)
DECLARE @BaselinePeriodDays INT = 28;      -- Historical baseline period (e.g., 4 weeks)
DECLARE @MinBaselineCount INT = 5;         -- Minimum occurrences needed in baseline
DECLARE @SignificanceThreshold FLOAT = 2.0; -- Must be X times higher than baseline
DECLARE @TopN INT = 10;                    -- Show top N risers

WITH PeriodCounts AS (
    -- Count occurrences in current period and baseline period
    SELECT
        il.INTERLOCK_DEF_ID,
        p.PLC_NAME,
        id.NUMBER AS INTERLOCK_NUMBER,
        td.MNEMONIC,
        td.MESSAGE,

        -- Current period count
        COUNT(CASE
            WHEN il.TIMESTAMP >= DATEADD(DAY, -@CurrentPeriodDays, GETDATE())
            THEN 1
        END) AS CurrentPeriodCount,

        -- Baseline period count (excluding current period)
        COUNT(CASE
            WHEN il.TIMESTAMP >= DATEADD(DAY, -@BaselinePeriodDays - @CurrentPeriodDays, GETDATE())
            AND il.TIMESTAMP < DATEADD(DAY, -@CurrentPeriodDays, GETDATE())
            THEN 1
        END) AS BaselinePeriodCount,

        -- Total count for reference
        COUNT(*) AS TotalCount,

        -- Most recent occurrence
        MAX(il.TIMESTAMP) AS LastOccurrence

    FROM FF_INTERLOCK_LOG il
    INNER JOIN INTERLOCK_DEFINITION id ON il.INTERLOCK_DEF_ID = id.INTERLOCK_DEF_ID
    INNER JOIN PLC p ON id.PLC_ID = p.PLC_ID
    INNER JOIN TEXT_DEFINITION td ON id.TEXT_DEF_ID = td.TEXT_DEF_ID

    -- Only look at data within our analysis window
    WHERE il.TIMESTAMP >= DATEADD(DAY, -@BaselinePeriodDays - @CurrentPeriodDays, GETDATE())

    GROUP BY
        il.INTERLOCK_DEF_ID,
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
            ELSE 999.99  -- New fault (no baseline)
        END AS IncreaseRatio,

        -- Calculate absolute increase
        (CAST(CurrentPeriodCount AS FLOAT) / @CurrentPeriodDays) -
        (CAST(BaselinePeriodCount AS FLOAT) / @BaselinePeriodDays) AS DailyIncrease

    FROM PeriodCounts

    WHERE
        -- Must have occurred in current period
        CurrentPeriodCount > 0
        -- Filter out steady-state "normal" faults
        AND (
            -- Either it's significantly higher than baseline
            (BaselinePeriodCount >= @MinBaselineCount
             AND (CAST(CurrentPeriodCount AS FLOAT) / @CurrentPeriodDays) /
                 (CAST(BaselinePeriodCount AS FLOAT) / @BaselinePeriodDays) >= @SignificanceThreshold)
            -- Or it's a new fault
            OR (BaselinePeriodCount < @MinBaselineCount AND CurrentPeriodCount >= 3)
        )
)

-- ============================================================================
-- FINAL RESULTS: TOP RISERS
-- ============================================================================
SELECT TOP (@TopN)
    RANK() OVER (ORDER BY IncreaseRatio DESC, DailyIncrease DESC) AS [Rank],
    PLC_NAME,
    INTERLOCK_NUMBER,
    MNEMONIC,
    MESSAGE,

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
        WHEN BaselinePeriodCount < @MinBaselineCount THEN '🆕 NEW FAULT'
        WHEN IncreaseRatio >= 5.0 THEN '🔥 CRITICAL RISER'
        WHEN IncreaseRatio >= 3.0 THEN '⚠️ HIGH RISER'
        ELSE '📈 MODERATE RISER'
    END AS [Status],

    LastOccurrence AS [Last_Occurrence]

FROM RiserAnalysis

ORDER BY
    IncreaseRatio DESC,
    DailyIncrease DESC;

-- ============================================================================
-- ALTERNATIVE VIEW: Monthly Analysis
-- ============================================================================
PRINT '';
PRINT '========================================================================';
PRINT 'For monthly analysis, change @CurrentPeriodDays = 30';
PRINT 'and @BaselinePeriodDays = 90 (3 months baseline)';
PRINT '========================================================================';
PRINT '';

-- Example: Get top conditions for each riser
-- Uncomment to see which conditions are triggering most often
/*
SELECT TOP 20
    p.PLC_NAME,
    id.NUMBER AS INTERLOCK_NUMBER,
    itd.MNEMONIC AS INTERLOCK_MNEMONIC,
    ctd.MNEMONIC AS CONDITION_MNEMONIC,
    ctd.MESSAGE AS CONDITION_MESSAGE,
    COUNT(*) AS ConditionCount,
    MAX(il.TIMESTAMP) AS LastOccurrence
FROM FF_CONDITION_LOG cl
INNER JOIN FF_INTERLOCK_LOG il ON cl.INTERLOCK_LOG_ID = il.ID
INNER JOIN CONDITION_DEFINITION cd ON cl.CONDITION_DEF_ID = cd.CONDITION_DEF_ID
INNER JOIN INTERLOCK_DEFINITION id ON il.INTERLOCK_DEF_ID = id.INTERLOCK_DEF_ID
INNER JOIN PLC p ON id.PLC_ID = p.PLC_ID
INNER JOIN TEXT_DEFINITION itd ON id.TEXT_DEF_ID = itd.TEXT_DEF_ID
INNER JOIN TEXT_DEFINITION ctd ON cd.TEXT_DEF_ID = ctd.TEXT_DEF_ID
WHERE il.TIMESTAMP >= DATEADD(DAY, -7, GETDATE())
GROUP BY
    p.PLC_NAME,
    id.NUMBER,
    itd.MNEMONIC,
    ctd.MNEMONIC,
    ctd.MESSAGE
ORDER BY ConditionCount DESC;
*/
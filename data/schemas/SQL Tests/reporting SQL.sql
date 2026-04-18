CREATE PROCEDURE sp_InsertFaultTrendSnapshot
    @SnapshotDate DATE,
    @ConditionMessage NVARCHAR(500),
    @DaysRecent INT,
    @DaysPrevious INT,
    @RecentDailyAvg DECIMAL(10,2),
    @PreviousDailyAvg DECIMAL(10,2),
    @ChangePercent DECIMAL(10,2),
    @AbsoluteChange DECIMAL(10,2),
    @RecentCount INT,
    @PreviousCount INT,
    @ConfidenceScore DECIMAL(15,2),
    @RankPosition INT
AS
BEGIN
    DECLARE @ConditionDefID INT;
    DECLARE @ConfigID INT;
    
    -- Lookup CONDITION_DEF_ID from message text
    SELECT TOP 1 @ConditionDefID = cd.CONDITION_DEF_ID
    FROM CONDITION_DEFINITION cd
    INNER JOIN TEXT_DEFINITION td ON cd.TEXT_DEF_ID = td.TEXT_DEF_ID
    WHERE td.MESSAGE = @ConditionMessage;
    
    -- If not found, raise error
    IF @ConditionDefID IS NULL
    BEGIN
        RAISERROR('Condition message not found in database', 16, 1);
        RETURN;
    END
    
    -- Lookup or create CONFIG_ID
    SELECT @ConfigID = CONFIG_ID
    FROM TREND_ANALYSIS_CONFIG
    WHERE DAYS_RECENT = @DaysRecent AND DAYS_PREVIOUS = @DaysPrevious;
    
    IF @ConfigID IS NULL
    BEGIN
        INSERT INTO TREND_ANALYSIS_CONFIG (DAYS_RECENT, DAYS_PREVIOUS)
        VALUES (@DaysRecent, @DaysPrevious);
        SET @ConfigID = SCOPE_IDENTITY();
    END
    
    -- Insert the snapshot
    INSERT INTO FAULT_TREND_SNAPSHOTS (
        SNAPSHOT_DATE,
        CONDITION_DEF_ID,
        CONFIG_ID,
        RECENT_DAILY_AVG,
        PREVIOUS_DAILY_AVG,
        CHANGE_PERCENT,
        ABSOLUTE_CHANGE,
        RECENT_COUNT,
        PREVIOUS_COUNT,
        CONFIDENCE_SCORE,
        RANK_POSITION
    )
    VALUES (
        @SnapshotDate,
        @ConditionDefID,
        @ConfigID,
        @RecentDailyAvg,
        @PreviousDailyAvg,
        @ChangePercent,
        @AbsoluteChange,
        @RecentCount,
        @PreviousCount,
        @ConfidenceScore,
        @RankPosition
    );
END;


CREATE VIEW vw_FaultTrendSnapshots AS
SELECT
    fts.SNAPSHOT_ID,
    fts.SNAPSHOT_DATE,
    td.MESSAGE AS CONDITION_MESSAGE,
    td.MNEMONIC,
    tac.DAYS_RECENT,
    tac.DAYS_PREVIOUS,
    fts.RECENT_DAILY_AVG,
    fts.PREVIOUS_DAILY_AVG,
    fts.CHANGE_PERCENT,
    fts.ABSOLUTE_CHANGE,
    fts.RECENT_COUNT,
    fts.PREVIOUS_COUNT,
    fts.CONFIDENCE_SCORE,
    fts.RANK_POSITION,
    fts.CREATED_AT
FROM FAULT_TREND_SNAPSHOTS fts
INNER JOIN CONDITION_DEFINITION cd ON fts.CONDITION_DEF_ID = cd.CONDITION_DEF_ID
INNER JOIN TEXT_DEFINITION td ON cd.TEXT_DEF_ID = td.TEXT_DEF_ID
INNER JOIN TREND_ANALYSIS_CONFIG tac ON fts.CONFIG_ID = tac.CONFIG_ID;

SELECT * FROM vw_FaultTrendSnapshots
WHERE CONDITION_MESSAGE LIKE '%pump%'
ORDER BY SNAPSHOT_DATE DESC;


CREATE OR ALTER VIEW VW_FAULT_TREND_SNAPSHOTS AS
SELECT
    fts.SNAPSHOT_ID,
    fts.SNAPSHOT_DATE,
    p.PLC_NAME,
    td.MNEMONIC,
    td.MESSAGE,
    tac.DAYS_RECENT,
    tac.DAYS_PREVIOUS,
    fts.RECENT_DAILY_AVG,
    fts.PREVIOUS_DAILY_AVG,
    COALESCE(CAST(fts.CHANGE_PERCENT AS VARCHAR), 'NEW') AS CHANGE_PERCENT_DISPLAY,
    fts.CHANGE_PERCENT,
    fts.ABSOLUTE_CHANGE,
    fts.RECENT_COUNT,
    fts.PREVIOUS_COUNT,
    fts.CONFIDENCE_SCORE,
    fts.RANK_POSITION,
    fts.CREATED_AT
FROM FAULT_TREND_SNAPSHOTS fts
INNER JOIN CONDITION_DEFINITION cd ON fts.CONDITION_DEF_ID = cd.CONDITION_DEF_ID
INNER JOIN TEXT_DEFINITION td ON cd.TEXT_DEF_ID = td.TEXT_DEF_ID
INNER JOIN PLC p ON cd.PLC_ID = p.PLC_ID
INNER JOIN TREND_ANALYSIS_CONFIG tac ON fts.CONFIG_ID = tac.CONFIG_ID;

SELECT * FROM VW_FAULT_TREND_SNAPSHOTS ORDER BY SNAPSHOT_DATE DESC, RANK_POSITION;

-- See latest top 10
SELECT TOP 10 *
FROM VW_FAULT_TREND_SNAPSHOTS
WHERE SNAPSHOT_DATE = (SELECT MAX(SNAPSHOT_DATE) FROM FAULT_TREND_SNAPSHOTS)
ORDER BY RANK_POSITION;

-- See trend for specific fault
SELECT *
FROM VW_FAULT_TREND_SNAPSHOTS
WHERE MNEMONIC = 'W06452'
ORDER BY SNAPSHOT_DATE DESC;
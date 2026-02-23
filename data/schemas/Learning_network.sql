
-- ... existing code ...

-- ============================================================================
-- Create FAULT_TREND_SNAPSHOTS Table
-- Stores periodic snapshots of fault trend analysis
-- ============================================================================
-- Step 1: Create a table for unique trend analysis configurations
CREATE TABLE TREND_ANALYSIS_CONFIG (
    CONFIG_ID INT PRIMARY KEY IDENTITY(1,1),
    DAYS_RECENT INT NOT NULL,
    DAYS_PREVIOUS INT NOT NULL,
    CONSTRAINT UQ_Days_Configuration UNIQUE (DAYS_RECENT, DAYS_PREVIOUS)
);

-- Step 2: Normalized snapshot table
CREATE TABLE FAULT_TREND_SNAPSHOTS (
    SNAPSHOT_ID INT PRIMARY KEY IDENTITY(1,1),
    SNAPSHOT_DATE DATE NOT NULL,
    CONDITION_DEF_ID INT NOT NULL,  -- Links to existing CONDITION_DEFINITION
    CONFIG_ID INT NOT NULL,          -- Links to analysis configuration

    -- Metrics from analysis (only the calculated results)
    RECENT_DAILY_AVG DECIMAL(10,2) NULL,
    PREVIOUS_DAILY_AVG DECIMAL(10,2) NULL,
    CHANGE_PERCENT DECIMAL(10,2) NULL,
    ABSOLUTE_CHANGE DECIMAL(10,2) NULL,
    RECENT_COUNT INT NULL,
    PREVIOUS_COUNT INT NULL,
    CONFIDENCE_SCORE DECIMAL(15,2) NULL,
    RANK_POSITION INT NULL,

    -- Metadata
    CREATED_AT DATETIME NOT NULL DEFAULT GETDATE(),

    -- Foreign keys
    CONSTRAINT FK_FaultTrendSnapshots_ConditionDef
        FOREIGN KEY (CONDITION_DEF_ID) REFERENCES CONDITION_DEFINITION(CONDITION_DEF_ID),
    CONSTRAINT FK_FaultTrendSnapshots_Config
        FOREIGN KEY (CONFIG_ID) REFERENCES TREND_ANALYSIS_CONFIG(CONFIG_ID),

    -- Prevent duplicates
    CONSTRAINT UQ_Snapshot_Date_Condition_Config
        UNIQUE (SNAPSHOT_DATE, CONDITION_DEF_ID, CONFIG_ID)
);

-- ============================================================================
-- Create Indexes for Performance
-- ============================================================================
CREATE INDEX IX_InterlockLog_Timestamp ON FF_INTERLOCK_LOG(TIMESTAMP DESC);
CREATE INDEX IX_InterlockLog_DefID ON FF_INTERLOCK_LOG(INTERLOCK_DEF_ID);
CREATE INDEX IX_InterlockLog_Upstream ON FF_INTERLOCK_LOG(UPSTREAM_INTERLOCK_LOG_ID);
CREATE INDEX IX_ConditionLog_InterlockID ON FF_CONDITION_LOG(INTERLOCK_LOG_ID);
CREATE INDEX IX_InterlockDef_Number ON INTERLOCK_DEFINITION(NUMBER);
CREATE INDEX IX_InterlockDef_PLC ON INTERLOCK_DEFINITION(PLC_ID);
CREATE INDEX IX_InterlockDef_Text ON INTERLOCK_DEFINITION(TEXT_DEF_ID);
CREATE INDEX IX_ConditionDef_PLC_InterlockNum ON CONDITION_DEFINITION(PLC_ID, INTERLOCK_NUMBER);
CREATE INDEX IX_ConditionDef_Text ON CONDITION_DEFINITION(TEXT_DEF_ID);

-- Fixed indexes for FAULT_TREND_SNAPSHOTS (using correct column names)
CREATE INDEX IX_FaultTrendSnapshots_SnapshotDate ON FAULT_TREND_SNAPSHOTS(SNAPSHOT_DATE DESC);
CREATE INDEX IX_FaultTrendSnapshots_ConditionDefID ON FAULT_TREND_SNAPSHOTS(CONDITION_DEF_ID);
CREATE INDEX IX_FaultTrendSnapshots_DateCondition ON FAULT_TREND_SNAPSHOTS(SNAPSHOT_DATE, CONDITION_DEF_ID);
CREATE INDEX IX_FaultTrendSnapshots_RankPosition ON FAULT_TREND_SNAPSHOTS(SNAPSHOT_DATE, RANK_POSITION);
CREATE INDEX IX_FaultTrendSnapshots_ConfigID ON FAULT_TREND_SNAPSHOTS(CONFIG_ID);

-- ============================================================================
-- Create View for Fault Trend History (joins to get message text)
-- ============================================================================
GO
CREATE VIEW VW_FAULT_TREND_HISTORY AS
SELECT
    td.MESSAGE AS CONDITION_MESSAGE,
    td.MNEMONIC,
    fts.SNAPSHOT_DATE,
    fts.RECENT_DAILY_AVG,
    fts.CHANGE_PERCENT,
    fts.CONFIDENCE_SCORE,
    fts.RANK_POSITION,
    tac.DAYS_RECENT,
    tac.DAYS_PREVIOUS,
    LAG(fts.RECENT_DAILY_AVG) OVER (
        PARTITION BY fts.CONDITION_DEF_ID, fts.CONFIG_ID 
        ORDER BY fts.SNAPSHOT_DATE
    ) AS PREV_WEEK_AVG,
    fts.RECENT_DAILY_AVG - LAG(fts.RECENT_DAILY_AVG) OVER (
        PARTITION BY fts.CONDITION_DEF_ID, fts.CONFIG_ID 
        ORDER BY fts.SNAPSHOT_DATE
    ) AS WEEK_OVER_WEEK_CHANGE
FROM FAULT_TREND_SNAPSHOTS fts
INNER JOIN CONDITION_DEFINITION cd ON fts.CONDITION_DEF_ID = cd.CONDITION_DEF_ID
INNER JOIN TEXT_DEFINITION td ON cd.TEXT_DEF_ID = td.TEXT_DEF_ID
INNER JOIN TREND_ANALYSIS_CONFIG tac ON fts.CONFIG_ID = tac.CONFIG_ID;
GO
-- ============================================================================
-- REPORTING VIEWS AND TABLES
-- Run section by section in DBeaver (select each block, Ctrl+Enter).
-- ============================================================================

-- ----------------------------------------------------------------------------
-- 1. View — select this block alone and run it
-- ----------------------------------------------------------------------------
CREATE OR ALTER VIEW vw_root_cause_faults AS
SELECT
    fil.ID             AS fault_id,
    fil.TIMESTAMP      AS utc_timestamp,
    id_.PLC_ID,
    id_.TEXT_DEF_ID,
    p.PLC_NAME,
    td.MNEMONIC
FROM dbo.FF_INTERLOCK_LOG   fil
JOIN dbo.INTERLOCK_DEFINITION id_ ON id_.INTERLOCK_DEF_ID = fil.INTERLOCK_DEF_ID
JOIN dbo.PLC                p     ON p.PLC_ID              = id_.PLC_ID
JOIN dbo.TEXT_DEFINITION    td    ON td.TEXT_DEF_ID         = id_.TEXT_DEF_ID
WHERE fil.UPSTREAM_INTERLOCK_LOG_ID IS NULL
  AND td.REPORTING = 1;

-- ----------------------------------------------------------------------------
-- 2. Snapshot tables — select all three and run together
-- ----------------------------------------------------------------------------
CREATE TABLE dbo.daily_hour_snapshot (
    id            INT      PRIMARY KEY IDENTITY(1,1),
    snapshot_date DATE     NOT NULL,
    hour          TINYINT  NOT NULL,   -- 0-23 Brussels local
    fault_count   INT      NOT NULL,
    CONSTRAINT UQ_hour_snapshot UNIQUE (snapshot_date, hour)
);

CREATE TABLE dbo.daily_plc_snapshot (
    id            INT      PRIMARY KEY IDENTITY(1,1),
    snapshot_date DATE     NOT NULL,
    plc_id        INT      NOT NULL,
    fault_count   INT      NOT NULL,
    CONSTRAINT FK_plc_snapshot_plc  FOREIGN KEY (plc_id) REFERENCES dbo.PLC(PLC_ID),
    CONSTRAINT UQ_plc_snapshot      UNIQUE (snapshot_date, plc_id)
);

CREATE TABLE dbo.top_riser_snapshot (
    id             INT      PRIMARY KEY IDENTITY(1,1),
    snapshot_date  DATE     NOT NULL,
    recent_days    TINYINT  NOT NULL,
    baseline_days  SMALLINT NOT NULL,
    plc_id         INT      NOT NULL,
    text_def_id    INT      NOT NULL,
    recent_count   INT      NOT NULL,
    baseline_count INT      NOT NULL,
    delta_pct      FLOAT    NOT NULL,
    CONSTRAINT FK_top_riser_plc  FOREIGN KEY (plc_id)      REFERENCES dbo.PLC(PLC_ID),
    CONSTRAINT FK_top_riser_text FOREIGN KEY (text_def_id) REFERENCES dbo.TEXT_DEFINITION(TEXT_DEF_ID),
    CONSTRAINT UQ_top_riser      UNIQUE (snapshot_date, recent_days, baseline_days, plc_id, text_def_id)
);
-- ============================================================================
-- REPORTING STORED PROCEDURES
-- Run each block separately in DBeaver (select block, Ctrl+Enter).
-- ============================================================================

-- ----------------------------------------------------------------------------
-- 1. sp_hour_snapshot
-- ----------------------------------------------------------------------------
CREATE OR ALTER PROCEDURE sp_hour_snapshot
    @reference_date DATE
AS
BEGIN
    SET NOCOUNT ON;
    SELECT
        s.snapshot_date,
        s.hour,
        s.fault_count
    FROM dbo.daily_hour_snapshot s
    WHERE s.snapshot_date = @reference_date
    ORDER BY s.hour;
END;

-- ----------------------------------------------------------------------------
-- 2. sp_plc_snapshot
-- ----------------------------------------------------------------------------
CREATE OR ALTER PROCEDURE sp_plc_snapshot
    @reference_date DATE
AS
BEGIN
    SET NOCOUNT ON;
    SELECT
        s.snapshot_date,
        p.PLC_NAME  AS plc_name,
        s.fault_count
    FROM dbo.daily_plc_snapshot s
    JOIN dbo.PLC p ON p.PLC_ID = s.plc_id
    WHERE s.snapshot_date = @reference_date
    ORDER BY s.fault_count DESC;
END;

-- ----------------------------------------------------------------------------
-- 3. sp_top_riser_snapshot
-- ----------------------------------------------------------------------------
CREATE OR ALTER PROCEDURE sp_top_riser_snapshot
    @reference_date DATE,
    @recent_days    TINYINT  = 7,
    @baseline_days  SMALLINT = 30,
    @top_n          INT      = 10
AS
BEGIN
    SET NOCOUNT ON;
    SELECT TOP (@top_n)
        s.snapshot_date,
        p.PLC_NAME              AS plc_name,
        td.MNEMONIC             AS mnemonic,
        s.recent_count,
        s.baseline_count,
        s.delta_pct
    FROM dbo.top_riser_snapshot s
    JOIN dbo.PLC             p  ON p.PLC_ID       = s.plc_id
    JOIN dbo.TEXT_DEFINITION td ON td.TEXT_DEF_ID = s.text_def_id
    WHERE s.snapshot_date  = @reference_date
      AND s.recent_days    = @recent_days
      AND s.baseline_days  = @baseline_days
    ORDER BY s.delta_pct DESC;
END;

-- ----------------------------------------------------------------------------
-- 4. sp_mtbf_snapshot
-- ----------------------------------------------------------------------------
CREATE OR ALTER PROCEDURE sp_mtbf_snapshot
    @reference_date DATE,
    @days_window    SMALLINT = 30
AS
BEGIN
    SET NOCOUNT ON;
    SELECT
        s.snapshot_date,
        p.PLC_NAME  AS plc_name,
        s.avg_hours,
        s.fault_count
    FROM dbo.mtbf_snapshot s
    JOIN dbo.PLC p ON p.PLC_ID = s.plc_id
    WHERE s.snapshot_date = @reference_date
      AND s.days_window   = @days_window
    ORDER BY s.avg_hours;
END;

-- ----------------------------------------------------------------------------
-- 5. sp_repeat_offender_snapshot
-- ----------------------------------------------------------------------------
CREATE OR ALTER PROCEDURE sp_repeat_offender_snapshot
    @reference_date DATE,
    @days_window    SMALLINT = 30,
    @top_n          INT      = 10
AS
BEGIN
    SET NOCOUNT ON;
    SELECT TOP (@top_n)
        s.snapshot_date,
        p.PLC_NAME              AS plc_name,
        td.MNEMONIC             AS mnemonic,
        s.max_per_hour
    FROM dbo.repeat_offender_snapshot s
    JOIN dbo.PLC             p  ON p.PLC_ID       = s.plc_id
    JOIN dbo.TEXT_DEFINITION td ON td.TEXT_DEF_ID = s.text_def_id
    WHERE s.snapshot_date = @reference_date
      AND s.days_window   = @days_window
    ORDER BY s.max_per_hour DESC;
END;
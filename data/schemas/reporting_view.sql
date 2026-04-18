-- ============================================================================
-- REPORTING SNAPSHOT VIEWS
-- Run each block separately in DBeaver (select block, Ctrl+Enter).
-- ============================================================================

-- ----------------------------------------------------------------------------
-- 1. vw_daily_hour_snapshot
-- ----------------------------------------------------------------------------
CREATE OR ALTER VIEW vw_daily_hour_snapshot AS
SELECT
    id,
    snapshot_date,
    hour,
    fault_count
FROM dbo.daily_hour_snapshot;

-- ----------------------------------------------------------------------------
-- 2. vw_daily_plc_snapshot
-- ----------------------------------------------------------------------------
CREATE OR ALTER VIEW vw_daily_plc_snapshot AS
SELECT
    s.id,
    s.snapshot_date,
    s.plc_id,
    p.PLC_NAME  AS plc_name,
    s.fault_count
FROM dbo.daily_plc_snapshot s
JOIN dbo.PLC p ON p.PLC_ID = s.plc_id;

-- ----------------------------------------------------------------------------
-- 3. vw_top_riser_snapshot
-- ----------------------------------------------------------------------------
CREATE OR ALTER VIEW vw_top_riser_snapshot AS
SELECT
    s.id,
    s.snapshot_date,
    s.recent_days,
    s.baseline_days,
    s.plc_id,
    p.PLC_NAME              AS plc_name,
    s.text_def_id,
    td.MNEMONIC             AS mnemonic,
    s.recent_count,
    s.baseline_count,
    s.delta_pct
FROM dbo.top_riser_snapshot s
JOIN dbo.PLC             p  ON p.PLC_ID       = s.plc_id
JOIN dbo.TEXT_DEFINITION td ON td.TEXT_DEF_ID = s.text_def_id;

-- ----------------------------------------------------------------------------
-- 4. vw_mtbf_snapshot
-- ----------------------------------------------------------------------------
CREATE OR ALTER VIEW vw_mtbf_snapshot AS
SELECT
    s.id,
    s.snapshot_date,
    s.days_window,
    s.plc_id,
    p.PLC_NAME  AS plc_name,
    s.avg_hours,
    s.fault_count
FROM dbo.mtbf_snapshot s
JOIN dbo.PLC p ON p.PLC_ID = s.plc_id;

-- ----------------------------------------------------------------------------
-- 5. vw_repeat_offender_snapshot
-- ----------------------------------------------------------------------------
CREATE OR ALTER VIEW vw_repeat_offender_snapshot AS
SELECT
    s.id,
    s.snapshot_date,
    s.days_window,
    s.plc_id,
    p.PLC_NAME              AS plc_name,
    s.text_def_id,
    td.MNEMONIC             AS mnemonic,
    s.max_per_hour
FROM dbo.repeat_offender_snapshot s
JOIN dbo.PLC             p  ON p.PLC_ID       = s.plc_id
JOIN dbo.TEXT_DEFINITION td ON td.TEXT_DEF_ID = s.text_def_id;

-- ----------------------------------------------------------------------------
-- 6. vw_long_term_trend_snapshot
-- ----------------------------------------------------------------------------
CREATE OR ALTER VIEW vw_long_term_trend_snapshot AS
SELECT
    s.id,
    s.week_start,
    s.plc_id,
    p.PLC_NAME              AS plc_name,
    s.text_def_id,
    td.MNEMONIC             AS mnemonic,
    s.weekly_count
FROM dbo.long_term_trend_snapshot s
JOIN dbo.PLC             p  ON p.PLC_ID       = s.plc_id
JOIN dbo.TEXT_DEFINITION td ON td.TEXT_DEF_ID = s.text_def_id;
/*
================================================================================
MAXDOP 1 TEST — fn_InterlockChain (BSID 1208)
================================================================================

Hypothesis: the ~4.5s is dominated by parallelism overhead on the recursive CTE,
not base-table I/O. Evidence from prior diag runs:

  - FF_INTERLOCK_LOG logical reads  : 7,358      (fine, covered)
  - FF_CONDITION_LOG logical reads  : 4,580      (fine, covered)
  - Worktable logical reads         : 5,173,302  (248 scans)   <-- recursive spool
  - INTERLOCK_DEFINITION reads      : 594,150    (on 925 rows) <-- ~8,457 rebinds
  - Waits                           : CXCONSUMER + EXECSYNC + LATCH_EX
                                      (parallelism sync, not I/O)

If forcing serial execution drops elapsed significantly, the fix is either:
  (a) bake OPTION (MAXDOP 1) into fn_InterlockChain,
  (b) raise cost-threshold-for-parallelism / adjust MAXDOP server-side, or
  (c) restructure the recursive CTE to stop rebinding the downstream Merge Join.

Run blocks 1 -> 2 -> 3 and paste the Messages pane back.
================================================================================
*/

-- ============================================================================
-- 1. BASELINE (parallel, whatever the server picks) — warm cache first
-- ============================================================================
SET STATISTICS IO OFF;
SET STATISTICS TIME OFF;

-- Warm the cache so cold I/O doesn't skew the comparison
IF OBJECT_ID('tempdb..#warm') IS NOT NULL DROP TABLE #warm;
SELECT * INTO #warm FROM dbo.fn_InterlockChain(1208, NULL, NULL, NULL, NULL, NULL);
DROP TABLE #warm;
GO

SET STATISTICS IO ON;
SET STATISTICS TIME ON;

IF OBJECT_ID('tempdb..#diag_parallel') IS NOT NULL DROP TABLE #diag_parallel;

SELECT *
INTO   #diag_parallel
FROM   dbo.fn_InterlockChain(1208, NULL, NULL, NULL, NULL, NULL);
-- no hint -> uses the same plan the app sees today

DROP TABLE #diag_parallel;

SET STATISTICS IO OFF;
SET STATISTICS TIME OFF;
GO


-- ============================================================================
-- 2. SERIAL (MAXDOP 1) — same call, forced single-threaded
-- ============================================================================
-- Note: inline TVFs inherit the outer statement's hints, so OPTION (MAXDOP 1)
-- on the SELECT propagates into the function body. No need to alter the TVF.

SET STATISTICS IO ON;
SET STATISTICS TIME ON;

IF OBJECT_ID('tempdb..#diag_serial') IS NOT NULL DROP TABLE #diag_serial;

SELECT *
INTO   #diag_serial
FROM   dbo.fn_InterlockChain(1208, NULL, NULL, NULL, NULL, NULL)
OPTION (MAXDOP 1);

DROP TABLE #diag_serial;

SET STATISTICS IO OFF;
SET STATISTICS TIME OFF;
GO


-- ============================================================================
-- 3. WAIT-STATS DELTA on the MAXDOP 1 run
-- ============================================================================
-- If parallelism was the culprit, CXCONSUMER / EXECSYNC / LATCH_EX should
-- collapse to ~0 here. What remains is the real work.

IF OBJECT_ID('tempdb..#waits_before_s') IS NOT NULL DROP TABLE #waits_before_s;
SELECT wait_type, waiting_tasks_count, wait_time_ms, signal_wait_time_ms
INTO   #waits_before_s
FROM   sys.dm_os_wait_stats
WHERE  wait_type NOT IN ('SLEEP_TASK','BROKER_TASK_STOP','LAZYWRITER_SLEEP',
                         'XE_TIMER_EVENT','WAITFOR','HADR_FILESTREAM_IOMGR_IOCOMPLETION',
                         'DIRTY_PAGE_POLL','SP_SERVER_DIAGNOSTICS_SLEEP',
                         'SOS_WORK_DISPATCHER');

IF OBJECT_ID('tempdb..#diag_serial2') IS NOT NULL DROP TABLE #diag_serial2;
SELECT * INTO #diag_serial2
FROM dbo.fn_InterlockChain(1208, NULL, NULL, NULL, NULL, NULL)
OPTION (MAXDOP 1);
DROP TABLE #diag_serial2;

SELECT TOP 10
    a.wait_type,
    a.waiting_tasks_count - b.waiting_tasks_count  AS new_tasks,
    a.wait_time_ms        - b.wait_time_ms         AS wait_ms_delta,
    a.signal_wait_time_ms - b.signal_wait_time_ms  AS signal_ms_delta
FROM   sys.dm_os_wait_stats a
JOIN   #waits_before_s b ON a.wait_type = b.wait_type
WHERE  a.wait_time_ms - b.wait_time_ms > 0
ORDER BY wait_ms_delta DESC;

DROP TABLE #waits_before_s;
GO


/*
================================================================================
HOW TO READ THE OUTPUT
================================================================================

Compare section 1 (parallel) vs section 2 (serial) in the Messages pane:

  Elapsed drops a lot (e.g. 4500 ms -> <2000 ms):
      Parallelism overhead confirmed. Next step: add OPTION (MAXDOP 1) inside
      fn_InterlockChain, OR change server MAXDOP / cost threshold.

  Elapsed stays ~same or gets worse:
      Parallelism wasn't the problem. The 8,457 recursive rebinds against
      INTERLOCK_DEFINITION/PLC are the real cost -> the fix is restructuring
      the recursive CTE (materialise the anchor set, replace Merge Join with
      a Nested Loops against a smaller driving set, or flatten one of the
      recursive legs).

Section 3's wait deltas confirm which: CXCONSUMER/EXECSYNC/LATCH_EX should be
absent under MAXDOP 1.
================================================================================
*/
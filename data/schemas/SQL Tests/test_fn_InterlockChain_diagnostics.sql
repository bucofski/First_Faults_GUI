/*
================================================================================
DIAGNOSTICS — fn_InterlockChain (BSID 1208)
================================================================================

Goal: identify where the remaining ~4.5 s is spent.

Run the three sections below IN ORDER. Each targets a different lens:

  1. STATISTICS IO — tells us which TABLE costs the most in reads.
     Look for high "logical reads" — that's pages touched in buffer pool.
     A table with 100k+ reads for a 220-row result is a scan we want to kill.

  2. STATISTICS TIME — CPU vs. elapsed per statement.
     Large CPU/elapsed mismatch = waiting on I/O.

  3. PLAN CACHE — pulls the actual plan XML for fn_InterlockChain so we can
     read operator costs and spot the most expensive node.

  4. OPTIONAL: wait stats snapshot — tells us if we're CPU-bound or waiting.

================================================================================
*/

-- ============================================================================
-- 1 + 2. STATISTICS IO and TIME
-- ============================================================================
-- Run this, then look at the "Messages" tab in SSMS / the output pane.
-- Copy the output back to me — specifically the "Table 'X'. Scan count..."
-- lines and the total elapsed/CPU line.

SET STATISTICS IO ON;
SET STATISTICS TIME ON;

-- Throwaway target so the client doesn't render rows in the timer
IF OBJECT_ID('tempdb..#diag') IS NOT NULL DROP TABLE #diag;

SELECT *
INTO   #diag
FROM   dbo.fn_InterlockChain(1208, NULL, NULL, NULL, NULL, NULL);

DROP TABLE #diag;

SET STATISTICS IO OFF;
SET STATISTICS TIME OFF;
GO


-- ============================================================================
-- 3. PLAN CACHE — grab the actual plan for fn_InterlockChain
-- ============================================================================
-- Click the XML value in the output to open the graphical plan in SSMS.
-- Look for the operator with the highest "Operator Cost" %.

SELECT TOP 5
    qs.execution_count,
    qs.total_worker_time   / 1000.0 / qs.execution_count AS avg_cpu_ms,
    qs.total_elapsed_time  / 1000.0 / qs.execution_count AS avg_elapsed_ms,
    qs.total_logical_reads / qs.execution_count          AS avg_logical_reads,
    SUBSTRING(st.text, (qs.statement_start_offset / 2) + 1,
              ((CASE qs.statement_end_offset
                    WHEN -1 THEN DATALENGTH(st.text)
                    ELSE qs.statement_end_offset END
                - qs.statement_start_offset) / 2) + 1)     AS statement_text,
    qp.query_plan
FROM   sys.dm_exec_query_stats qs
CROSS APPLY sys.dm_exec_sql_text(qs.sql_handle)        st
CROSS APPLY sys.dm_exec_query_plan(qs.plan_handle)     qp
WHERE  st.text LIKE '%fn_InterlockChain%'
  AND  st.text NOT LIKE '%dm_exec_query_stats%'   -- exclude this diagnostic
ORDER BY qs.last_execution_time DESC;
GO


-- ============================================================================
-- 4. OPTIONAL: which waits did the batch hit?
-- ============================================================================
-- Snapshot waits immediately BEFORE and AFTER running fn_InterlockChain once.
-- The delta tells us if we're waiting on I/O (PAGEIOLATCH_*), CPU (SOS_SCHEDULER_YIELD),
-- parallelism (CXPACKET), or something else.

IF OBJECT_ID('tempdb..#waits_before') IS NOT NULL DROP TABLE #waits_before;
SELECT wait_type, waiting_tasks_count, wait_time_ms, signal_wait_time_ms
INTO   #waits_before
FROM   sys.dm_os_wait_stats
WHERE  wait_type NOT IN ('SLEEP_TASK','BROKER_TASK_STOP','LAZYWRITER_SLEEP',
                         'XE_TIMER_EVENT','WAITFOR','HADR_FILESTREAM_IOMGR_IOCOMPLETION',
                         'DIRTY_PAGE_POLL','SP_SERVER_DIAGNOSTICS_SLEEP');

-- run the target once
IF OBJECT_ID('tempdb..#diag2') IS NOT NULL DROP TABLE #diag2;
SELECT * INTO #diag2 FROM dbo.fn_InterlockChain(1208, NULL, NULL, NULL, NULL, NULL);
DROP TABLE #diag2;

SELECT TOP 10
    a.wait_type,
    a.waiting_tasks_count - b.waiting_tasks_count  AS new_tasks,
    a.wait_time_ms        - b.wait_time_ms         AS wait_ms_delta,
    a.signal_wait_time_ms - b.signal_wait_time_ms  AS signal_ms_delta
FROM   sys.dm_os_wait_stats a
JOIN   #waits_before b ON a.wait_type = b.wait_type
WHERE  a.wait_time_ms - b.wait_time_ms > 0
ORDER BY wait_ms_delta DESC;

DROP TABLE #waits_before;
GO
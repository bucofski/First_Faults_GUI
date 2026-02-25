-- ============================================================================
-- MIGRATION VALIDATION: TD2 → First_Fault
-- Run this BEFORE migrating to see how many records would be lost and why
-- Each check maps to a step in Migrate database.sql
-- ============================================================================


-- ============================================================================
-- TOTALS: baseline numbers to compare against
-- ============================================================================
SELECT
    (SELECT COUNT(*) FROM TD2.dbo.FF_INTERLOCK_LOG)  AS Total_Interlock_Log,
    (SELECT COUNT(*) FROM TD2.dbo.FF_CONDITION_LOG)   AS Total_Condition_Log;


-- ============================================================================
-- CHECK 1 (Step 1): Interlock log rows with NULL PLC → PLC won't be created
-- ============================================================================
SELECT
    'NULL PLC'                                            AS Issue,
    COUNT(*)                                              AS Lost_Records
FROM TD2.dbo.FF_INTERLOCK_LOG
WHERE PLC IS NULL;


-- ============================================================================
-- CHECK 2 (Step 2): Interlock log rows missing MNEMONIC or MESSAGE
-- → TEXT_DEFINITION won't be created, row can't be migrated
-- ============================================================================
SELECT
    'NULL MNEMONIC or MESSAGE (interlock log)'            AS Issue,
    COUNT(*)                                              AS Lost_Records
FROM TD2.dbo.FF_INTERLOCK_LOG
WHERE MNEMONIC IS NULL OR MESSAGE IS NULL;


-- ============================================================================
-- CHECK 3 (Step 2): Condition log rows missing MNEMONIC or MESSAGE
-- ============================================================================
SELECT
    'NULL MNEMONIC or MESSAGE (condition log)'            AS Issue,
    COUNT(*)                                              AS Lost_Records
FROM TD2.dbo.FF_CONDITION_LOG
WHERE MNEMONIC IS NULL OR MESSAGE IS NULL;


-- ============================================================================
-- CHECK 4 (Step 3): Duplicate PLC+NUMBER with different text
-- → only most recent text is kept, older versions are lost
-- ============================================================================
SELECT
    'PLC+NUMBER with multiple different texts (only latest kept)'  AS Issue,
    COUNT(*)                                                       AS Lost_Text_Versions
FROM (
    SELECT PLC, NUMBER, COUNT(DISTINCT CAST(MESSAGE AS NVARCHAR(MAX))) AS msg_count
    FROM TD2.dbo.FF_INTERLOCK_LOG
    WHERE PLC IS NOT NULL AND NUMBER IS NOT NULL
      AND MNEMONIC IS NOT NULL AND MESSAGE IS NOT NULL
    GROUP BY PLC, NUMBER
    HAVING COUNT(DISTINCT CAST(MESSAGE AS NVARCHAR(MAX))) > 1
) t;


-- ============================================================================
-- CHECK 5 (Step 5): Interlock log rows with NULL NUMBER → skipped entirely
-- ============================================================================
SELECT
    'NULL NUMBER (interlock skipped)'                     AS Issue,
    COUNT(*)                                              AS Lost_Records
FROM TD2.dbo.FF_INTERLOCK_LOG
WHERE NUMBER IS NULL;


-- ============================================================================
-- CHECK 6 (Step 5): TOP 1000000 limit — how many rows exist total?
-- If > 1000000, the oldest records are cut off
-- ============================================================================
SELECT
    'Records beyond TOP 1000000 limit'                    AS Issue,
    CASE
        WHEN COUNT(*) > 1000000 THEN COUNT(*) - 1000000
        ELSE 0
    END                                                   AS Lost_Records,
    COUNT(*)                                              AS Total_Rows
FROM TD2.dbo.FF_INTERLOCK_LOG
WHERE NUMBER IS NOT NULL;


-- ============================================================================
-- CHECK 7 (Step 7): Condition log rows with NULL TYPE or BIT_INDEX
-- → can't be matched to CONDITION_DEFINITION, skipped
-- ============================================================================
SELECT
    'NULL TYPE or BIT_INDEX (condition log skipped)'      AS Issue,
    COUNT(*)                                              AS Lost_Records
FROM TD2.dbo.FF_CONDITION_LOG
WHERE TYPE IS NULL OR BIT_INDEX IS NULL;


-- ============================================================================
-- CHECK 8 (Step 7): Condition log rows whose parent interlock has NULL NUMBER
-- → parent wasn't migrated, so condition is also lost
-- ============================================================================
SELECT
    'Condition log orphaned (parent interlock has NULL NUMBER)'  AS Issue,
    COUNT(*)                                                     AS Lost_Records
FROM TD2.dbo.FF_CONDITION_LOG cl
INNER JOIN TD2.dbo.FF_INTERLOCK_LOG il ON cl.INTERLOCK_REF = il.ID
WHERE il.NUMBER IS NULL;


-- ============================================================================
-- CHECK 9 (Step 8): UPSTREAM_INTERLOCK_REF pointing to a non-migratable record
-- → upstream chain link will be broken (NULL instead of correct reference)
-- ============================================================================
SELECT
    'Broken upstream references (pointing to non-migrated record)'  AS Issue,
    COUNT(*)                                                        AS Lost_References
FROM TD2.dbo.FF_CONDITION_LOG cl
INNER JOIN TD2.dbo.FF_INTERLOCK_LOG upstream ON cl.UPSTREAM_INTERLOCK_REF = upstream.ID
WHERE upstream.NUMBER IS NULL
   OR upstream.PLC IS NULL
   OR upstream.MNEMONIC IS NULL
   OR upstream.MESSAGE IS NULL;


-- ============================================================================
-- SUMMARY: all issues in one overview (TD2 only)
-- ============================================================================
SELECT Issue, Lost_Records FROM (
    SELECT 'NULL PLC'                                                          AS Issue, COUNT(*) AS Lost_Records FROM TD2.dbo.FF_INTERLOCK_LOG WHERE PLC IS NULL
    UNION ALL
    SELECT 'NULL MNEMONIC/MESSAGE (interlock)',                                          COUNT(*) FROM TD2.dbo.FF_INTERLOCK_LOG WHERE MNEMONIC IS NULL OR MESSAGE IS NULL
    UNION ALL
    SELECT 'NULL MNEMONIC/MESSAGE (condition)',                                          COUNT(*) FROM TD2.dbo.FF_CONDITION_LOG WHERE MNEMONIC IS NULL OR MESSAGE IS NULL
    UNION ALL
    SELECT 'NULL NUMBER (interlock skipped)',                                            COUNT(*) FROM TD2.dbo.FF_INTERLOCK_LOG WHERE NUMBER IS NULL
    UNION ALL
    SELECT 'NULL TYPE or BIT_INDEX (condition skipped)',                                 COUNT(*) FROM TD2.dbo.FF_CONDITION_LOG WHERE TYPE IS NULL OR BIT_INDEX IS NULL
    UNION ALL
    SELECT 'Condition orphaned (parent has NULL NUMBER)',                                COUNT(*) FROM TD2.dbo.FF_CONDITION_LOG cl INNER JOIN TD2.dbo.FF_INTERLOCK_LOG il ON cl.INTERLOCK_REF = il.ID WHERE il.NUMBER IS NULL
    UNION ALL
    SELECT 'Broken upstream refs (target not migratable)',                               COUNT(*) FROM TD2.dbo.FF_CONDITION_LOG cl INNER JOIN TD2.dbo.FF_INTERLOCK_LOG up ON cl.UPSTREAM_INTERLOCK_REF = up.ID WHERE up.NUMBER IS NULL OR up.PLC IS NULL OR up.MNEMONIC IS NULL OR up.MESSAGE IS NULL
) summary
ORDER BY Lost_Records DESC;


-- ============================================================================
-- DEEP DIVE: Why are 3990 upstream references missing?
-- Breaks down the lost upstream refs by root cause
-- ============================================================================
SELECT
    issue,
    COUNT(*) AS lost_upstream_refs
FROM (
    SELECT
        cl.ID,
        CASE
            WHEN up.ID IS NULL
                THEN 'Upstream record does not exist in TD2'
            WHEN up.NUMBER IS NULL
                THEN 'Upstream record has NULL NUMBER (not migrated)'
            WHEN up.PLC IS NULL
                THEN 'Upstream record has NULL PLC (not migrated)'
            WHEN up.MNEMONIC IS NULL OR up.MESSAGE IS NULL
                THEN 'Upstream record has NULL MNEMONIC/MESSAGE (not migrated)'
            ELSE 'Unknown'
        END AS issue
    FROM TD2.dbo.FF_CONDITION_LOG cl
    LEFT JOIN TD2.dbo.FF_INTERLOCK_LOG up ON cl.UPSTREAM_INTERLOCK_REF = up.ID
    WHERE cl.UPSTREAM_INTERLOCK_REF IS NOT NULL
      AND (
          up.ID IS NULL
          OR up.NUMBER IS NULL
          OR up.PLC IS NULL
          OR up.MNEMONIC IS NULL
          OR up.MESSAGE IS NULL
      )
) breakdown
GROUP BY issue
ORDER BY lost_upstream_refs DESC;


-- ============================================================================
-- DEEP DIVE 2: Upstream refs are valid in TD2 but still missing in First_Fault
-- → means the condition row itself was dropped during migration (Step 7 join failed)
-- Checks if the condition log row with the upstream ref could be matched to
-- a CONDITION_DEFINITION — if not, it never made it into #TempConditionLog
-- ============================================================================
SELECT
    'Condition row has valid upstream ref but cannot match CONDITION_DEFINITION'  AS Issue,
    COUNT(*)                                                                      AS Lost_Upstream_Refs
FROM TD2.dbo.FF_CONDITION_LOG cl
INNER JOIN TD2.dbo.FF_INTERLOCK_LOG il  ON cl.INTERLOCK_REF = il.ID
LEFT  JOIN First_Fault.dbo.PLC p        ON il.PLC COLLATE Latin1_General_CI_AS = p.PLC_NAME
LEFT  JOIN First_Fault.dbo.CONDITION_DEFINITION cdef
    ON p.PLC_ID             = cdef.PLC_ID
    AND il.NUMBER           = cdef.INTERLOCK_NUMBER
    AND cl.TYPE             = cdef.TYPE
    AND cl.BIT_INDEX        = cdef.BIT_INDEX
WHERE cl.UPSTREAM_INTERLOCK_REF IS NOT NULL
  AND cdef.CONDITION_DEF_ID IS NULL       -- join failed → row was silently dropped in Step 7
  AND cl.TYPE IS NOT NULL
  AND cl.BIT_INDEX IS NOT NULL
  AND il.NUMBER IS NOT NULL
  AND il.PLC IS NOT NULL;

-- Show which TYPE+BIT_INDEX combinations are failing the join
SELECT
    il.PLC,
    il.NUMBER           AS Interlock_Number,
    cl.TYPE,
    cl.BIT_INDEX,
    COUNT(*)            AS Occurrences
FROM TD2.dbo.FF_CONDITION_LOG cl
INNER JOIN TD2.dbo.FF_INTERLOCK_LOG il  ON cl.INTERLOCK_REF = il.ID
LEFT  JOIN First_Fault.dbo.PLC p        ON il.PLC COLLATE Latin1_General_CI_AS = p.PLC_NAME
LEFT  JOIN First_Fault.dbo.CONDITION_DEFINITION cdef
    ON p.PLC_ID             = cdef.PLC_ID
    AND il.NUMBER           = cdef.INTERLOCK_NUMBER
    AND cl.TYPE             = cdef.TYPE
    AND cl.BIT_INDEX        = cdef.BIT_INDEX
WHERE cl.UPSTREAM_INTERLOCK_REF IS NOT NULL
  AND cdef.CONDITION_DEF_ID IS NULL
  AND cl.TYPE IS NOT NULL
  AND cl.BIT_INDEX IS NOT NULL
  AND il.NUMBER IS NOT NULL
  AND il.PLC IS NOT NULL
GROUP BY il.PLC, il.NUMBER, cl.TYPE, cl.BIT_INDEX
ORDER BY Occurrences DESC;


-- ============================================================================
-- COMPARISON: TD2 vs First_Fault counts side by side
-- Run this after migration to see what made it across
-- ============================================================================
SELECT
    'PLC'                                                        AS Table_Name,
    (SELECT COUNT(DISTINCT PLC) FROM TD2.dbo.FF_INTERLOCK_LOG
     WHERE PLC IS NOT NULL)                                      AS TD2_Count,
    (SELECT COUNT(*) FROM First_Fault.dbo.PLC)                   AS FirstFault_Count

UNION ALL SELECT
    'Interlock Log',
    (SELECT COUNT(*) FROM TD2.dbo.FF_INTERLOCK_LOG
     WHERE NUMBER IS NOT NULL AND PLC IS NOT NULL),
    (SELECT COUNT(*) FROM First_Fault.dbo.FF_INTERLOCK_LOG)

UNION ALL SELECT
    'Condition Log',
    (SELECT COUNT(*) FROM TD2.dbo.FF_CONDITION_LOG
     WHERE TYPE IS NOT NULL AND BIT_INDEX IS NOT NULL),
    (SELECT COUNT(*) FROM First_Fault.dbo.FF_CONDITION_LOG)

UNION ALL SELECT
    'Upstream References',
    -- TD2: count DISTINCT interlocks that have an upstream ref (not condition rows)
    -- one interlock can have multiple conditions all pointing to the same upstream
    (SELECT COUNT(DISTINCT INTERLOCK_REF) FROM TD2.dbo.FF_CONDITION_LOG
     WHERE UPSTREAM_INTERLOCK_REF IS NOT NULL),
    (SELECT COUNT(*) FROM First_Fault.dbo.FF_INTERLOCK_LOG
     WHERE UPSTREAM_INTERLOCK_LOG_ID IS NOT NULL)

UNION ALL SELECT
    'Unique Interlock Definitions (PLC+NUMBER)',
    (SELECT COUNT(DISTINCT CAST(PLC AS NVARCHAR(50)) + '|' + CAST(NUMBER AS NVARCHAR(20)))
     FROM TD2.dbo.FF_INTERLOCK_LOG
     WHERE PLC IS NOT NULL AND NUMBER IS NOT NULL),
    (SELECT COUNT(*) FROM First_Fault.dbo.INTERLOCK_DEFINITION)

UNION ALL SELECT
    'Unique Condition Definitions (PLC+NUMBER+TYPE+BIT_INDEX)',
    (SELECT COUNT(DISTINCT
         CAST(il.PLC AS NVARCHAR(50)) + '|' + CAST(il.NUMBER AS NVARCHAR(20)) + '|' +
         CAST(cl.TYPE AS NVARCHAR(10)) + '|' + CAST(cl.BIT_INDEX AS NVARCHAR(10)))
     FROM TD2.dbo.FF_CONDITION_LOG cl
     INNER JOIN TD2.dbo.FF_INTERLOCK_LOG il ON cl.INTERLOCK_REF = il.ID
     WHERE il.PLC IS NOT NULL AND il.NUMBER IS NOT NULL
       AND cl.TYPE IS NOT NULL AND cl.BIT_INDEX IS NOT NULL),
    (SELECT COUNT(*) FROM First_Fault.dbo.CONDITION_DEFINITION);
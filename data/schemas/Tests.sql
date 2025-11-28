USE First_Fault;
GO

-- Show fault chain with conditions for BSID 11222
;WITH UpstreamChain AS (
    SELECT
        il.ID,
        il.TIMESTAMP,
        il.UPSTREAM_INTERLOCK_LOG_ID,
        p.PLC_CODE,
        idef.NUMBER as BSID,
        td.MNEMONIC,
        td.MESSAGE,
        0 as Level
    FROM FF_INTERLOCK_LOG il
    INNER JOIN INTERLOCK_DEFINITION idef ON il.INTERLOCK_DEF_ID = idef.INTERLOCK_DEF_ID
    INNER JOIN PLC p ON idef.PLC_ID = p.PLC_ID
    INNER JOIN TEXT_DEFINITION td ON idef.TEXT_DEF_ID = td.TEXT_DEF_ID
    WHERE idef.NUMBER = 11222
    ORDER BY il.TIMESTAMP DESC
    OFFSET 0 ROWS FETCH NEXT 1 ROW ONLY

    UNION ALL

    SELECT
        upstream_il.ID,
        upstream_il.TIMESTAMP,
        upstream_il.UPSTREAM_INTERLOCK_LOG_ID,
        p.PLC_CODE,
        idef.NUMBER,
        td.MNEMONIC,
        td.MESSAGE,
        uc.Level + 1
    FROM UpstreamChain uc
    INNER JOIN FF_INTERLOCK_LOG upstream_il ON uc.UPSTREAM_INTERLOCK_LOG_ID = upstream_il.ID
    INNER JOIN INTERLOCK_DEFINITION idef ON upstream_il.INTERLOCK_DEF_ID = idef.INTERLOCK_DEF_ID
    INNER JOIN PLC p ON idef.PLC_ID = p.PLC_ID
    INNER JOIN TEXT_DEFINITION td ON idef.TEXT_DEF_ID = td.TEXT_DEF_ID
)
SELECT
    uc.Level,
    uc.PLC_CODE,
    uc.BSID,
    uc.MESSAGE as Interlock_Message,
    cdef.TYPE as Condition_Type,
    cdef.BIT_INDEX as Condition_Bit,
    td_cond.MESSAGE as Condition_Message,
    CASE WHEN uc.UPSTREAM_INTERLOCK_LOG_ID IS NULL THEN 'ROOT CAUSE' ELSE '' END as Note
FROM UpstreamChain uc
LEFT JOIN FF_CONDITION_LOG cl ON uc.ID = cl.INTERLOCK_LOG_ID
LEFT JOIN CONDITION_DEFINITION cdef ON cl.CONDITION_DEF_ID = cdef.CONDITION_DEF_ID
LEFT JOIN TEXT_DEFINITION td_cond ON cdef.TEXT_DEF_ID = td_cond.TEXT_DEF_ID
ORDER BY uc.Level DESC, cdef.TYPE, cdef.BIT_INDEX;

USE TD2;

-- 1. Check unique combinations in INTERLOCK_LOG
SELECT
    'INTERLOCK_LOG' as Source,
    COUNT(*) as TotalRecords,
    COUNT(DISTINCT MNEMONIC) as UniqueMnemonics,
    COUNT(DISTINCT MESSAGE) as UniqueMessages,
    COUNT(DISTINCT CONCAT(MNEMONIC, '|', MESSAGE)) as UniqueCombinations
FROM FF_INTERLOCK_LOG
WHERE MNEMONIC IS NOT NULL AND MESSAGE IS NOT NULL

UNION ALL

-- 2. Check unique combinations in CONDITION_LOG
SELECT
    'CONDITION_LOG' as Source,
    COUNT(*) as TotalRecords,
    COUNT(DISTINCT MNEMONIC) as UniqueMnemonics,
    COUNT(DISTINCT MESSAGE) as UniqueMessages,
    COUNT(DISTINCT CONCAT(MNEMONIC, '|', MESSAGE)) as UniqueCombinations
FROM FF_CONDITION_LOG
WHERE MNEMONIC IS NOT NULL AND MESSAGE IS NOT NULL;

-- Check if the same interlock NUMBER has different MNEMONIC/MESSAGE over time
SELECT
    PLC,
    NUMBER,
    COUNT(DISTINCT MNEMONIC) as DifferentMnemonics,
    COUNT(DISTINCT MESSAGE) as DifferentMessages,
    COUNT(*) as OccurrenceCount,
    MIN(TIMESTAMP) as FirstSeen,
    MAX(TIMESTAMP) as LastSeen
FROM TD2.dbo.FF_INTERLOCK_LOG
WHERE NUMBER IS NOT NULL
GROUP BY PLC, NUMBER
HAVING COUNT(DISTINCT MNEMONIC) > 1 OR COUNT(DISTINCT MESSAGE) > 1
ORDER BY DifferentMessages DESC, DifferentMnemonics DESC;

-- Check if the same condition (TYPE+BIT_INDEX) has different text over time
SELECT
    il.PLC,
    cl.TYPE,
    cl.BIT_INDEX,
    COUNT(DISTINCT cl.MNEMONIC) as DifferentMnemonics,
    COUNT(DISTINCT cl.MESSAGE) as DifferentMessages,
    COUNT(*) as OccurrenceCount,
    MIN(il.TIMESTAMP) as FirstSeen,
    MAX(il.TIMESTAMP) as LastSeen
FROM TD2.dbo.FF_CONDITION_LOG cl
INNER JOIN TD2.dbo.FF_INTERLOCK_LOG il ON cl.INTERLOCK_REF = il.ID
WHERE cl.TYPE IS NOT NULL AND cl.BIT_INDEX IS NOT NULL
GROUP BY il.PLC, cl.TYPE, cl.BIT_INDEX
HAVING COUNT(DISTINCT cl.MNEMONIC) > 1 OR COUNT(DISTINCT cl.MESSAGE) > 1
ORDER BY DifferentMessages DESC, DifferentMnemonics DESC;


SELECT TOP 20
    PLC,
    NUMBER,
    MNEMONIC,
    MESSAGE,
    TIMESTAMP,
    COUNT(*) OVER (PARTITION BY PLC, NUMBER, MNEMONIC, MESSAGE) as TimesUsed
FROM TD2.dbo.FF_INTERLOCK_LOG
WHERE PLC = 'TDS' AND NUMBER = 1706
ORDER BY TIMESTAMP;


SELECT
    TABLE_NAME,
    COLUMN_NAME,
    DATA_TYPE,
    IS_NULLABLE
FROM First_Fault.INFORMATION_SCHEMA.COLUMNS
WHERE TABLE_NAME IN ('FF_INTERLOCK_LOG', 'FF_CONDITION_LOG', 'INTERLOCK_DEFINITION', 'CONDITION_DEFINITION', 'TEXT_DEFINITION', 'PLC')
ORDER BY TABLE_NAME, ORDINAL_POSITION;

-- Check TD2 for conditions
SELECT TOP 10
    il.ID,
    il.NUMBER as BSID,
    il.MESSAGE as Interlock_Message,
    cl.TYPE,
    cl.BIT_INDEX,
    cl.MESSAGE as Condition_Message
FROM TD2.dbo.FF_INTERLOCK_LOG il
LEFT JOIN TD2.dbo.FF_CONDITION_LOG cl ON il.ID = cl.INTERLOCK_REF
WHERE il.NUMBER = 11222
ORDER BY il.TIMESTAMP DESC;

-- Check First_Fault for conditions
SELECT TOP 10
    il.ID,
    idef.NUMBER as BSID,
    td_interlock.MESSAGE as Interlock_Message,
    cdef.TYPE,
    cdef.BIT_INDEX,
    td_condition.MESSAGE as Condition_Message
FROM First_Fault.dbo.FF_INTERLOCK_LOG il
INNER JOIN First_Fault.dbo.INTERLOCK_DEFINITION idef
    ON il.INTERLOCK_DEF_ID = idef.INTERLOCK_DEF_ID
INNER JOIN First_Fault.dbo.TEXT_DEFINITION td_interlock
    ON idef.TEXT_DEF_ID = td_interlock.TEXT_DEF_ID
LEFT JOIN First_Fault.dbo.FF_CONDITION_LOG cl
    ON il.ID = cl.INTERLOCK_LOG_ID
LEFT JOIN First_Fault.dbo.CONDITION_DEFINITION cdef
    ON cl.CONDITION_DEF_ID = cdef.CONDITION_DEF_ID
LEFT JOIN First_Fault.dbo.TEXT_DEFINITION td_condition
    ON cdef.TEXT_DEF_ID = td_condition.TEXT_DEF_ID
WHERE idef.NUMBER = 11222
ORDER BY il.TIMESTAMP DESC;
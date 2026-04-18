-- ============================================================================
-- 1. NO TIME FILTER — top 20 most recent root cause faults
-- ============================================================================

; WITH ChainData AS (
    SELECT *
    FROM dbo.fn_InterlockChain(NULL, 200, NULL, NULL, NULL, NULL)
),
RootCauses AS (
    SELECT *,
           MAX(Level) OVER (PARTITION BY AnchorReference) AS max_level
    FROM ChainData
),
Deduplicated AS (
    SELECT *,
           ROW_NUMBER() OVER (
               PARTITION BY Condition_Mnemonic, DATEADD(MINUTE, DATEDIFF(MINUTE, 0, TIMESTAMP), 0)
               ORDER BY Interlock_Log_ID DESC
           ) AS dedup_rank
    FROM RootCauses
    WHERE Level = max_level
)
SELECT TOP 20
    Date, TIMESTAMP, Level, max_level,
    Interlock_Log_ID, BSID, PLC,
    Interlock_Message, Condition_Mnemonic, Condition_Message
FROM Deduplicated
WHERE dedup_rank = 1
ORDER BY TIMESTAMP DESC
OPTION (RECOMPILE);


-- ============================================================================
-- 2. LAST 24 HOURS — relative to the most recent record in the database
-- ============================================================================

; WITH LatestTimestamp AS (
    SELECT MAX(TIMESTAMP) AS max_ts
    FROM First_Fault.dbo.FF_INTERLOCK_LOG
),
ChainData AS (
    SELECT *
    FROM dbo.fn_InterlockChain(NULL, 200, (SELECT DATEADD(HOUR, -24, max_ts) FROM LatestTimestamp), NULL, NULL, NULL)
),
RootCauses AS (
    SELECT *,
           MAX(Level) OVER (PARTITION BY AnchorReference) AS max_level
    FROM ChainData
),
Deduplicated AS (
    SELECT *,
           ROW_NUMBER() OVER (
               PARTITION BY Condition_Mnemonic, DATEADD(MINUTE, DATEDIFF(MINUTE, 0, TIMESTAMP), 0)
               ORDER BY Interlock_Log_ID DESC
           ) AS dedup_rank
    FROM RootCauses
    WHERE Level = max_level
)
SELECT TOP 20
    Date, TIMESTAMP, Level, max_level,
    Interlock_Log_ID, BSID, PLC,
    Interlock_Message, Condition_Mnemonic, Condition_Message
FROM Deduplicated
WHERE dedup_rank = 1
ORDER BY TIMESTAMP DESC
OPTION (RECOMPILE);


-- ============================================================================
-- 3. LAST 7 DAYS — relative to the most recent record in the database
-- ============================================================================

; WITH LatestTimestamp AS (
    SELECT MAX(TIMESTAMP) AS max_ts
    FROM First_Fault.dbo.FF_INTERLOCK_LOG
),
ChainData AS (
    SELECT *
    FROM dbo.fn_InterlockChain(NULL, 200, (SELECT DATEADD(DAY, -7, max_ts) FROM LatestTimestamp), NULL, NULL, NULL)
),
RootCauses AS (
    SELECT *,
           MAX(Level) OVER (PARTITION BY AnchorReference) AS max_level
    FROM ChainData
),
Deduplicated AS (
    SELECT *,
           ROW_NUMBER() OVER (
               PARTITION BY Condition_Mnemonic, DATEADD(MINUTE, DATEDIFF(MINUTE, 0, TIMESTAMP), 0)
               ORDER BY Interlock_Log_ID DESC
           ) AS dedup_rank
    FROM RootCauses
    WHERE Level = max_level
)
SELECT TOP 20
    Date, TIMESTAMP, Level, max_level,
    Interlock_Log_ID, BSID, PLC,
    Interlock_Message, Condition_Mnemonic, Condition_Message
FROM Deduplicated
WHERE dedup_rank = 1
ORDER BY TIMESTAMP DESC
OPTION (RECOMPILE);
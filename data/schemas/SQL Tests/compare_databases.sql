-- ============================================================================
-- SIDE BY SIDE COMPARISON: TD2 vs First_Fault
-- Uses TIMESTAMP + PLC + NUMBER as the match key (ID changed during migration)
-- Flags any differences in the data columns
-- ============================================================================

; WITH TD2_Data AS (
    SELECT TOP 200
        il.TIMESTAMP,
        CAST(il.PLC       AS NVARCHAR(50))   AS PLC,
        il.NUMBER                             AS BSID,
        CAST(il.MESSAGE   AS NVARCHAR(500))   AS Interlock_Message,
        cl.TYPE,
        cl.BIT_INDEX,
        CAST(cl.MESSAGE   AS NVARCHAR(500))   AS Condition_Message,
        CAST(cl.MNEMONIC  AS NVARCHAR(255))   AS Condition_Mnemonic,
        cl.UPSTREAM_INTERLOCK_REF
    FROM TD2.dbo.FF_INTERLOCK_LOG il
    LEFT JOIN TD2.dbo.FF_CONDITION_LOG cl ON il.ID = cl.INTERLOCK_REF
    WHERE il.NUMBER IS NOT NULL AND il.PLC IS NOT NULL
    ORDER BY il.TIMESTAMP DESC, il.ORDER_LOG DESC
),

FF_Data AS (
    SELECT TOP 200
        il.TIMESTAMP,
        p.PLC_NAME                            AS PLC,
        idef.NUMBER                           AS BSID,
        itd.MESSAGE                           AS Interlock_Message,
        cdef.TYPE,
        cdef.BIT_INDEX,
        ctd.MESSAGE                           AS Condition_Message,
        ctd.MNEMONIC                          AS Condition_Mnemonic,
        il.UPSTREAM_INTERLOCK_LOG_ID          AS UPSTREAM_INTERLOCK_REF
    FROM First_Fault.dbo.FF_INTERLOCK_LOG il
    INNER JOIN First_Fault.dbo.INTERLOCK_DEFINITION idef ON il.INTERLOCK_DEF_ID  = idef.INTERLOCK_DEF_ID
    INNER JOIN First_Fault.dbo.PLC p                     ON idef.PLC_ID          = p.PLC_ID
    INNER JOIN First_Fault.dbo.TEXT_DEFINITION itd       ON idef.TEXT_DEF_ID     = itd.TEXT_DEF_ID
    LEFT  JOIN First_Fault.dbo.FF_CONDITION_LOG cl       ON il.ID                = cl.INTERLOCK_LOG_ID
    LEFT  JOIN First_Fault.dbo.CONDITION_DEFINITION cdef ON cl.CONDITION_DEF_ID  = cdef.CONDITION_DEF_ID
    LEFT  JOIN First_Fault.dbo.TEXT_DEFINITION ctd       ON cdef.TEXT_DEF_ID     = ctd.TEXT_DEF_ID
    ORDER BY il.TIMESTAMP DESC, il.ORDER_LOG DESC
)

-- ============================================================================
-- RESULT: side by side with difference flags
-- ============================================================================
SELECT
    COALESCE(td2.TIMESTAMP,                                          ff.TIMESTAMP)  AS TIMESTAMP,
    COALESCE(td2.PLC COLLATE Latin1_General_CI_AS,                   ff.PLC)        AS PLC,
    COALESCE(td2.BSID,                                               ff.BSID)       AS BSID,

    -- TD2 values
    td2.Interlock_Message                                    AS TD2_Interlock_Message,
    td2.Condition_Mnemonic                                   AS TD2_Condition_Mnemonic,
    td2.Condition_Message                                    AS TD2_Condition_Message,
    td2.TYPE                                                 AS TD2_TYPE,
    td2.BIT_INDEX                                            AS TD2_BIT_INDEX,

    -- First_Fault values
    ff.Interlock_Message                                     AS FF_Interlock_Message,
    ff.Condition_Mnemonic                                    AS FF_Condition_Mnemonic,
    ff.Condition_Message                                     AS FF_Condition_Message,
    ff.TYPE                                                  AS FF_TYPE,
    ff.BIT_INDEX                                             AS FF_BIT_INDEX,

    -- Difference flags — 'OK' or 'DIFF'
    -- NULL = NULL is handled explicitly: both NULL = OK, one NULL = DIFF
    CASE WHEN ISNULL(td2.Interlock_Message  COLLATE Latin1_General_CI_AS, '') = ISNULL(ff.Interlock_Message,  '') THEN 'OK' ELSE 'DIFF' END  AS Interlock_Message_Match,
    CASE WHEN ISNULL(td2.Condition_Mnemonic COLLATE Latin1_General_CI_AS, '') = ISNULL(ff.Condition_Mnemonic, '') THEN 'OK' ELSE 'DIFF' END  AS Mnemonic_Match,
    CASE WHEN ISNULL(td2.Condition_Message  COLLATE Latin1_General_CI_AS, '') = ISNULL(ff.Condition_Message,  '') THEN 'OK' ELSE 'DIFF' END  AS Condition_Message_Match,
    CASE WHEN ISNULL(CAST(td2.TYPE      AS INT), -1) = ISNULL(CAST(ff.TYPE      AS INT), -1) THEN 'OK' ELSE 'DIFF' END  AS TYPE_Match,
    CASE WHEN ISNULL(CAST(td2.BIT_INDEX AS INT), -1) = ISNULL(CAST(ff.BIT_INDEX AS INT), -1) THEN 'OK' ELSE 'DIFF' END  AS BIT_INDEX_Match,

    -- Source flag: both = matched, TD2 only = missing in FF, FF only = extra in FF
    CASE
        WHEN td2.TIMESTAMP IS NULL THEN 'FF only'
        WHEN ff.TIMESTAMP  IS NULL THEN 'TD2 only'
        ELSE 'Both'
    END                                                      AS Source

FROM TD2_Data td2
FULL OUTER JOIN FF_Data ff
    ON  td2.TIMESTAMP                                   = ff.TIMESTAMP
    AND td2.PLC         COLLATE Latin1_General_CI_AS    = ff.PLC
    AND td2.BSID                                        = ff.BSID
    AND ISNULL(CAST(td2.TYPE      AS INT), -1)           = ISNULL(CAST(ff.TYPE      AS INT), -1)
    AND ISNULL(CAST(td2.BIT_INDEX AS INT), -1)           = ISNULL(CAST(ff.BIT_INDEX AS INT), -1)

ORDER BY TIMESTAMP DESC, BSID, TD2_TYPE, TD2_BIT_INDEX;
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




-- ============================================================================
-- MIGRATION SCRIPT: TD2 to First_Fault (CORRECTED - FIRST 5000 RECORDS)
-- ============================================================================
-- KEY FIX: CONDITION_DEFINITION now includes INTERLOCK_NUMBER
-- This is necessary because TYPE+BIT_INDEX is only unique within each interlock
-- ============================================================================

USE First_Fault;

-- ========================================================================
-- CLEANUP: Close and deallocate any existing cursors from failed runs
-- ========================================================================
IF CURSOR_STATUS('global','interlock_cursor') >= -1
BEGIN
    DEALLOCATE interlock_cursor;
END

-- Clean up any temp tables from previous failed runs
IF OBJECT_ID('tempdb..#TempInterlockLog') IS NOT NULL DROP TABLE #TempInterlockLog;
IF OBJECT_ID('tempdb..#TempConditionLog') IS NOT NULL DROP TABLE #TempConditionLog;
IF OBJECT_ID('tempdb..#IDMapping') IS NOT NULL DROP TABLE #IDMapping;

-- ========================================================================
-- START MIGRATION
-- ========================================================================

BEGIN TRANSACTION;

BEGIN TRY

    -- ========================================================================
    -- STEP 1: Populate PLC Table
    -- ========================================================================
    PRINT 'Step 1: Migrating PLC data from TD2...';

    INSERT INTO First_Fault.dbo.PLC (PLC_NAME)
    SELECT DISTINCT
        PLC COLLATE Latin1_General_CI_AS as PLC_NAME
    FROM TD2.dbo.FF_INTERLOCK_LOG
    WHERE PLC IS NOT NULL
    ORDER BY PLC;

    DECLARE @RowCount INT = @@ROWCOUNT;
    PRINT 'PLCs migrated: ' + CAST(@RowCount AS NVARCHAR(10));

    -- ========================================================================
    -- STEP 2: Populate TEXT_DEFINITION (all unique MNEMONIC+MESSAGE pairs)
    -- ========================================================================
    PRINT 'Step 2: Migrating TEXT_DEFINITION (unique MNEMONIC+MESSAGE pairs)...';

    INSERT INTO First_Fault.dbo.TEXT_DEFINITION (MNEMONIC, MESSAGE)
    SELECT DISTINCT
        MNEMONIC COLLATE Latin1_General_CI_AS as MNEMONIC,
        MESSAGE COLLATE Latin1_General_CI_AS as MESSAGE
    FROM (
        SELECT DISTINCT
            MNEMONIC,
            MESSAGE
        FROM TD2.dbo.FF_INTERLOCK_LOG
        WHERE MNEMONIC IS NOT NULL AND MESSAGE IS NOT NULL

        UNION

        SELECT DISTINCT
            MNEMONIC,
            MESSAGE
        FROM TD2.dbo.FF_CONDITION_LOG
        WHERE MNEMONIC IS NOT NULL AND MESSAGE IS NOT NULL
    ) AS AllTexts
    ORDER BY MNEMONIC, MESSAGE;

    SET @RowCount = @@ROWCOUNT;
    PRINT 'Unique text definitions migrated: ' + CAST(@RowCount AS NVARCHAR(10));

    -- ========================================================================
    -- STEP 3: Populate INTERLOCK_DEFINITION
    -- Uses the MOST RECENT (latest timestamp) text for each PLC+NUMBER
    -- ========================================================================
    PRINT 'Step 3: Migrating Interlock Definitions...';

    -- First, find the most recent text for each PLC+NUMBER combination
    ;WITH LatestInterlockText AS (
        SELECT
            il.PLC,
            il.NUMBER,
            il.MNEMONIC,
            il.MESSAGE,
            il.TIMESTAMP,
            ROW_NUMBER() OVER (
                PARTITION BY il.PLC, il.NUMBER
                ORDER BY il.TIMESTAMP DESC, il.ID DESC
            ) as RowNum
        FROM TD2.dbo.FF_INTERLOCK_LOG il
        WHERE il.NUMBER IS NOT NULL
            AND il.MNEMONIC IS NOT NULL
            AND il.MESSAGE IS NOT NULL
    )
    INSERT INTO First_Fault.dbo.INTERLOCK_DEFINITION (PLC_ID, NUMBER, TEXT_DEF_ID)
    SELECT
        p.PLC_ID,
        lit.NUMBER,
        td.TEXT_DEF_ID
    FROM LatestInterlockText lit
    INNER JOIN First_Fault.dbo.PLC p
        ON lit.PLC COLLATE Latin1_General_CI_AS = p.PLC_NAME
    INNER JOIN First_Fault.dbo.TEXT_DEFINITION td
        ON lit.MNEMONIC COLLATE Latin1_General_CI_AS = td.MNEMONIC
        AND lit.MESSAGE COLLATE Latin1_General_CI_AS = td.MESSAGE
    WHERE lit.RowNum = 1;  -- Only the most recent text

    SET @RowCount = @@ROWCOUNT;
    PRINT 'Interlock Definitions migrated: ' + CAST(@RowCount AS NVARCHAR(10));

    -- ========================================================================
    -- STEP 4: Populate CONDITION_DEFINITION
    -- CRITICAL FIX: Now includes INTERLOCK_NUMBER in the definition
    -- This is because TYPE+BIT_INDEX is only unique WITHIN each interlock
    -- ========================================================================
    PRINT 'Step 4: Migrating Condition Definitions (WITH INTERLOCK_NUMBER)...';

    -- First, find the most recent text for each PLC+INTERLOCK_NUMBER+TYPE+BIT_INDEX combination
    ;WITH LatestConditionText AS (
        SELECT
            il.PLC,
            il.NUMBER as INTERLOCK_NUMBER,
            cl.TYPE,
            cl.BIT_INDEX,
            cl.MNEMONIC,
            cl.MESSAGE,
            il.TIMESTAMP,
            ROW_NUMBER() OVER (
                PARTITION BY il.PLC, il.NUMBER, cl.TYPE, cl.BIT_INDEX
                ORDER BY il.TIMESTAMP DESC, cl.ID DESC
            ) as RowNum
        FROM TD2.dbo.FF_CONDITION_LOG cl
        INNER JOIN TD2.dbo.FF_INTERLOCK_LOG il ON cl.INTERLOCK_REF = il.ID
        WHERE cl.TYPE IS NOT NULL
            AND cl.BIT_INDEX IS NOT NULL
            AND cl.MNEMONIC IS NOT NULL
            AND cl.MESSAGE IS NOT NULL
            AND il.NUMBER IS NOT NULL
    )
    INSERT INTO First_Fault.dbo.CONDITION_DEFINITION (PLC_ID, INTERLOCK_NUMBER, TYPE, BIT_INDEX, TEXT_DEF_ID)
    SELECT
        p.PLC_ID,
        lct.INTERLOCK_NUMBER,
        lct.TYPE,
        lct.BIT_INDEX,
        td.TEXT_DEF_ID
    FROM LatestConditionText lct
    INNER JOIN First_Fault.dbo.PLC p
        ON lct.PLC COLLATE Latin1_General_CI_AS = p.PLC_NAME
    INNER JOIN First_Fault.dbo.TEXT_DEFINITION td
        ON lct.MNEMONIC COLLATE Latin1_General_CI_AS = td.MNEMONIC
        AND lct.MESSAGE COLLATE Latin1_General_CI_AS = td.MESSAGE
    WHERE lct.RowNum = 1;  -- Only the most recent text

    SET @RowCount = @@ROWCOUNT;
    PRINT 'Condition Definitions migrated: ' + CAST(@RowCount AS NVARCHAR(10));

    -- ========================================================================
    -- STEP 5: Prepare Interlock Log data - FIRST 5000 RECORDS ONLY
    -- ========================================================================
    PRINT 'Step 5: Preparing Interlock Log data (TOP 5000)...';

    SELECT TOP 300000
            old_il.ID as Old_ID,
            idef.INTERLOCK_DEF_ID,
            old_il.TIMESTAMP,
            old_il.TIMESTAMP_LOG,
            ISNULL(old_il.ORDER_LOG, 0) as ORDER_LOG
        INTO #TempInterlockLog
        FROM TD2.dbo.FF_INTERLOCK_LOG old_il
        INNER JOIN First_Fault.dbo.PLC p
            ON old_il.PLC COLLATE Latin1_General_CI_AS = p.PLC_NAME
        INNER JOIN First_Fault.dbo.INTERLOCK_DEFINITION idef
            ON p.PLC_ID = idef.PLC_ID
            AND old_il.NUMBER = idef.NUMBER
        WHERE old_il.NUMBER IS NOT NULL  -- Only records with valid NUMBER
        ORDER BY old_il.TIMESTAMP DESC, old_il.ID DESC;

    -- ID mapping with GUID support
    SELECT
        Old_ID,
        CAST(NULL AS INT) as New_ID
    INTO #IDMapping
    FROM #TempInterlockLog;

    DECLARE @TempCount INT;
    SELECT @TempCount = COUNT(*) FROM #TempInterlockLog;
    PRINT 'Temp tables created with ' + CAST(@TempCount AS NVARCHAR(10)) + ' records (LIMITED TO 5000)';

    -- ========================================================================
    -- STEP 6: Insert Interlock Log records and build ID mapping
    -- ========================================================================
    PRINT 'Step 6: Inserting Interlock Log records...';

    DECLARE @Old_ID_GUID UNIQUEIDENTIFIER;
    DECLARE @New_ID INT;
    DECLARE @Counter INT = 0;
    DECLARE @Total INT;
    SELECT @Total = COUNT(*) FROM #TempInterlockLog;

    PRINT 'Total records to migrate: ' + CAST(@Total AS NVARCHAR(10));

    DECLARE interlock_cursor CURSOR LOCAL FAST_FORWARD FOR
        SELECT Old_ID FROM #TempInterlockLog ORDER BY Old_ID;

    OPEN interlock_cursor;
    FETCH NEXT FROM interlock_cursor INTO @Old_ID_GUID;

    WHILE @@FETCH_STATUS = 0
    BEGIN
        INSERT INTO First_Fault.dbo.FF_INTERLOCK_LOG
            (INTERLOCK_DEF_ID, TIMESTAMP, TIMESTAMP_LOG, ORDER_LOG, UPSTREAM_INTERLOCK_LOG_ID)
        SELECT
            INTERLOCK_DEF_ID,
            TIMESTAMP,
            TIMESTAMP_LOG,
            ORDER_LOG,
            NULL  -- Will be updated in STEP 8
        FROM #TempInterlockLog
        WHERE Old_ID = @Old_ID_GUID;

        SET @New_ID = SCOPE_IDENTITY();

        UPDATE #IDMapping
        SET New_ID = @New_ID
        WHERE Old_ID = @Old_ID_GUID;

        SET @Counter = @Counter + 1;

        IF @Counter % 500 = 0
            PRINT '  Progress: ' + CAST(@Counter AS NVARCHAR(10)) + ' / ' + CAST(@Total AS NVARCHAR(10));

        FETCH NEXT FROM interlock_cursor INTO @Old_ID_GUID;
    END

    CLOSE interlock_cursor;
    DEALLOCATE interlock_cursor;

    PRINT 'Interlock Log records migrated: ' + CAST(@Counter AS NVARCHAR(10));

    -- ========================================================================
    -- STEP 7: Populate FF_CONDITION_LOG
    -- CRITICAL FIX: Now joins on INTERLOCK_NUMBER as well
    -- ========================================================================
    PRINT 'Step 7: Migrating Condition Log...';

    SELECT
        old_cl.ID as Old_Condition_ID,
        map.New_ID as New_Interlock_Log_ID,
        cdef.CONDITION_DEF_ID,
        old_cl.UPSTREAM_INTERLOCK_REF,
        CAST(NULL AS INT) as Upstream_New_ID
    INTO #TempConditionLog
    FROM TD2.dbo.FF_CONDITION_LOG old_cl
    INNER JOIN #IDMapping map ON old_cl.INTERLOCK_REF = map.Old_ID
    INNER JOIN TD2.dbo.FF_INTERLOCK_LOG old_il ON old_cl.INTERLOCK_REF = old_il.ID
    INNER JOIN First_Fault.dbo.PLC p
        ON old_il.PLC COLLATE Latin1_General_CI_AS = p.PLC_NAME
    INNER JOIN First_Fault.dbo.CONDITION_DEFINITION cdef
        ON p.PLC_ID = cdef.PLC_ID
        AND old_il.NUMBER = cdef.INTERLOCK_NUMBER  -- FIX: Now includes INTERLOCK_NUMBER
        AND old_cl.TYPE = cdef.TYPE
        AND old_cl.BIT_INDEX = cdef.BIT_INDEX
    WHERE old_cl.INTERLOCK_REF IS NOT NULL
        AND old_cl.TYPE IS NOT NULL
        AND old_cl.BIT_INDEX IS NOT NULL;

    -- Map upstream references
    UPDATE tcl
    SET Upstream_New_ID = map_upstream.New_ID
    FROM #TempConditionLog tcl
    INNER JOIN #IDMapping map_upstream ON tcl.UPSTREAM_INTERLOCK_REF = map_upstream.Old_ID
    WHERE tcl.UPSTREAM_INTERLOCK_REF IS NOT NULL;

    -- Insert condition log entries
    INSERT INTO First_Fault.dbo.FF_CONDITION_LOG (INTERLOCK_LOG_ID, CONDITION_DEF_ID)
    SELECT DISTINCT
        New_Interlock_Log_ID,
        CONDITION_DEF_ID
    FROM #TempConditionLog;

    SET @RowCount = @@ROWCOUNT;
    PRINT 'Condition Log entries migrated: ' + CAST(@RowCount AS NVARCHAR(10));

    -- ========================================================================
    -- STEP 8: Update upstream interlock references
    -- ========================================================================
    PRINT 'Step 8: Updating upstream interlock references...';

    UPDATE il
    SET UPSTREAM_INTERLOCK_LOG_ID = tcl.Upstream_New_ID
    FROM First_Fault.dbo.FF_INTERLOCK_LOG il
    INNER JOIN #TempConditionLog tcl ON il.ID = tcl.New_Interlock_Log_ID
    WHERE tcl.Upstream_New_ID IS NOT NULL;

    SET @RowCount = @@ROWCOUNT;
    PRINT 'Upstream references updated: ' + CAST(@RowCount AS NVARCHAR(10));

    -- ========================================================================
    -- STEP 9: Cleanup
    -- ========================================================================
    PRINT 'Step 9: Cleaning up temporary tables...';
    DROP TABLE #TempInterlockLog;
    DROP TABLE #TempConditionLog;
    DROP TABLE #IDMapping;

    -- ========================================================================
    -- STEP 10: Verification & Summary
    -- ========================================================================
    PRINT '';
    PRINT '========================================================================';
    PRINT 'MIGRATION SUMMARY: TD2 → First_Fault (TEST - 5000 Records)';
    PRINT '========================================================================';
    PRINT '';

    DECLARE @TD2_InterlockCount INT, @TD2_ConditionCount INT;
    DECLARE @FF_PLCCount INT, @FF_TextDefCount INT, @FF_InterlockDefCount INT;
    DECLARE @FF_ConditionDefCount INT, @FF_InterlockLogCount INT;
    DECLARE @FF_ConditionLogCount INT, @FF_UpstreamCount INT;

    SELECT @TD2_InterlockCount = COUNT(*) FROM TD2.dbo.FF_INTERLOCK_LOG;
    SELECT @TD2_ConditionCount = COUNT(*) FROM TD2.dbo.FF_CONDITION_LOG;

    SELECT @FF_PLCCount = COUNT(*) FROM First_Fault.dbo.PLC;
    SELECT @FF_TextDefCount = COUNT(*) FROM First_Fault.dbo.TEXT_DEFINITION;
    SELECT @FF_InterlockDefCount = COUNT(*) FROM First_Fault.dbo.INTERLOCK_DEFINITION;
    SELECT @FF_ConditionDefCount = COUNT(*) FROM First_Fault.dbo.CONDITION_DEFINITION;
    SELECT @FF_InterlockLogCount = COUNT(*) FROM First_Fault.dbo.FF_INTERLOCK_LOG;
    SELECT @FF_ConditionLogCount = COUNT(*) FROM First_Fault.dbo.FF_CONDITION_LOG;
    SELECT @FF_UpstreamCount = COUNT(*) FROM First_Fault.dbo.FF_INTERLOCK_LOG
        WHERE UPSTREAM_INTERLOCK_LOG_ID IS NOT NULL;

    PRINT '--- SOURCE (TD2) ---';
    PRINT 'Total Interlock Log:       ' + CAST(@TD2_InterlockCount AS NVARCHAR(10));
    PRINT 'Total Condition Log:       ' + CAST(@TD2_ConditionCount AS NVARCHAR(10));
    PRINT '';
    PRINT '--- TARGET (First_Fault) - TEST RUN ---';
    PRINT 'PLCs:                      ' + CAST(@FF_PLCCount AS NVARCHAR(10));
    PRINT 'Text Definitions:          ' + CAST(@FF_TextDefCount AS NVARCHAR(10));
    PRINT 'Interlock Definitions:     ' + CAST(@FF_InterlockDefCount AS NVARCHAR(10));
    PRINT 'Condition Definitions:     ' + CAST(@FF_ConditionDefCount AS NVARCHAR(10));
    PRINT 'Interlock Log Entries:     ' + CAST(@FF_InterlockLogCount AS NVARCHAR(10)) + ' (MAX 100000)';
    PRINT 'Condition Log Entries:     ' + CAST(@FF_ConditionLogCount AS NVARCHAR(10));
    PRINT 'Upstream References:       ' + CAST(@FF_UpstreamCount AS NVARCHAR(10));
    PRINT '';
    PRINT '========================================================================';

    COMMIT TRANSACTION;

    PRINT '';
    PRINT '✓ TEST MIGRATION COMPLETED SUCCESSFULLY!';
    PRINT '  (First 5000 interlock records only)';
    PRINT '  KEY FIX APPLIED: CONDITION_DEFINITION now includes INTERLOCK_NUMBER';
    PRINT '';

END TRY
BEGIN CATCH
    IF @@TRANCOUNT > 0
        ROLLBACK TRANSACTION;

    PRINT '';
    PRINT '========================================================================';
    PRINT '✗ ERROR OCCURRED DURING MIGRATION!';
    PRINT '========================================================================';
    PRINT 'Error Message: ' + ERROR_MESSAGE();
    PRINT 'Error Line: ' + CAST(ERROR_LINE() AS NVARCHAR(10));
    PRINT 'Error Number: ' + CAST(ERROR_NUMBER() AS NVARCHAR(10));
    PRINT 'Error Severity: ' + CAST(ERROR_SEVERITY() AS NVARCHAR(10));
    PRINT '';
    PRINT 'All changes have been rolled back.';
    PRINT '========================================================================';

    -- Cleanup cursors
    IF CURSOR_STATUS('local','interlock_cursor') >= -1
    BEGIN
        CLOSE interlock_cursor;
        DEALLOCATE interlock_cursor;
    END

    -- Cleanup temp tables
    IF OBJECT_ID('tempdb..#TempInterlockLog') IS NOT NULL DROP TABLE #TempInterlockLog;
    IF OBJECT_ID('tempdb..#TempConditionLog') IS NOT NULL DROP TABLE #TempConditionLog;
    IF OBJECT_ID('tempdb..#IDMapping') IS NOT NULL DROP TABLE #IDMapping;

END CATCH;
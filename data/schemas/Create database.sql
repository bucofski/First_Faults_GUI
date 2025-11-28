USE First_Fault;

-- ============================================================================
-- DROP AND CREATE SCHEMA (CORRECTED VERSION)
-- ============================================================================
-- KEY FIX: CONDITION_DEFINITION now includes INTERLOCK_NUMBER column
-- This is necessary because TYPE+BIT_INDEX is only unique within each interlock
-- ============================================================================

-- Drop existing tables if they exist
IF OBJECT_ID('First_Fault.dbo.FF_CONDITION_LOG', 'U') IS NOT NULL
    DROP TABLE First_Fault.dbo.FF_CONDITION_LOG;
IF OBJECT_ID('First_Fault.dbo.FF_INTERLOCK_LOG', 'U') IS NOT NULL
    DROP TABLE First_Fault.dbo.FF_INTERLOCK_LOG;
IF OBJECT_ID('First_Fault.dbo.CONDITION_DEFINITION', 'U') IS NOT NULL
    DROP TABLE First_Fault.dbo.CONDITION_DEFINITION;
IF OBJECT_ID('First_Fault.dbo.INTERLOCK_DEFINITION', 'U') IS NOT NULL
    DROP TABLE First_Fault.dbo.INTERLOCK_DEFINITION;
IF OBJECT_ID('First_Fault.dbo.TEXT_DEFINITION', 'U') IS NOT NULL
    DROP TABLE First_Fault.dbo.TEXT_DEFINITION;
IF OBJECT_ID('First_Fault.dbo.PLC', 'U') IS NOT NULL
    DROP TABLE First_Fault.dbo.PLC;
GO

-- ============================================================================
-- Create PLC Table
-- ============================================================================
CREATE TABLE PLC (
    PLC_ID INT PRIMARY KEY IDENTITY(1,1),
    PLC_CODE NVARCHAR(50) NOT NULL UNIQUE,
    PLC_NAME NVARCHAR(100),
    DESCRIPTION NVARCHAR(255)
);

-- ============================================================================
-- Create TEXT_DEFINITION Table
-- Stores all unique MNEMONIC+MESSAGE combinations
-- ============================================================================
CREATE TABLE TEXT_DEFINITION (
    TEXT_DEF_ID INT PRIMARY KEY IDENTITY(1,1),
    MNEMONIC NVARCHAR(255) NOT NULL,
    MESSAGE NVARCHAR(500) NOT NULL,
    CONSTRAINT UQ_Mnemonic_Message UNIQUE (MNEMONIC, MESSAGE)
);

-- ============================================================================
-- Create INTERLOCK_DEFINITION Table
-- Defines interlocks: PLC + NUMBER → Text
-- ============================================================================
CREATE TABLE INTERLOCK_DEFINITION (
    INTERLOCK_DEF_ID INT PRIMARY KEY IDENTITY(1,1),
    PLC_ID INT NOT NULL,
    NUMBER INT NOT NULL,
    TEXT_DEF_ID INT NOT NULL,
    CONSTRAINT FK_InterlockDef_PLC FOREIGN KEY (PLC_ID) REFERENCES PLC(PLC_ID),
    CONSTRAINT FK_InterlockDef_Text FOREIGN KEY (TEXT_DEF_ID) REFERENCES TEXT_DEFINITION(TEXT_DEF_ID),
    CONSTRAINT UQ_PLC_Number UNIQUE (PLC_ID, NUMBER)
);

-- ============================================================================
-- Create CONDITION_DEFINITION Table
-- CORRECTED: Now includes INTERLOCK_NUMBER
-- Defines conditions: PLC + INTERLOCK_NUMBER + TYPE + BIT_INDEX → Text
-- ============================================================================
CREATE TABLE CONDITION_DEFINITION (
    CONDITION_DEF_ID INT PRIMARY KEY IDENTITY(1,1),
    PLC_ID INT NOT NULL,
    INTERLOCK_NUMBER INT NOT NULL,  -- ← KEY FIX: Added this column!
    TYPE INT NOT NULL,
    BIT_INDEX INT NOT NULL,
    TEXT_DEF_ID INT NOT NULL,
    CONSTRAINT FK_ConditionDef_PLC FOREIGN KEY (PLC_ID) REFERENCES PLC(PLC_ID),
    CONSTRAINT FK_ConditionDef_Text FOREIGN KEY (TEXT_DEF_ID) REFERENCES TEXT_DEFINITION(TEXT_DEF_ID),
    CONSTRAINT FK_ConditionDef_Interlock FOREIGN KEY (PLC_ID, INTERLOCK_NUMBER)
        REFERENCES INTERLOCK_DEFINITION(PLC_ID, NUMBER),
    CONSTRAINT UQ_PLC_InterlockNum_Type_BitIndex UNIQUE (PLC_ID, INTERLOCK_NUMBER, TYPE, BIT_INDEX)
);

-- ============================================================================
-- Create FF_INTERLOCK_LOG Table
-- Logs when interlocks occur
-- ============================================================================
CREATE TABLE FF_INTERLOCK_LOG (
    ID INT PRIMARY KEY IDENTITY(1,1),
    INTERLOCK_DEF_ID INT NOT NULL,
    TIMESTAMP DATETIME NOT NULL,
    TIMESTAMP_LOG DATETIME NOT NULL,
    ORDER_LOG INT NOT NULL,
    UPSTREAM_INTERLOCK_LOG_ID INT NULL,
    CONSTRAINT FK_InterlockLog_Definition FOREIGN KEY (INTERLOCK_DEF_ID)
        REFERENCES INTERLOCK_DEFINITION(INTERLOCK_DEF_ID),
    CONSTRAINT FK_InterlockLog_Upstream FOREIGN KEY (UPSTREAM_INTERLOCK_LOG_ID)
        REFERENCES FF_INTERLOCK_LOG(ID)
);

-- ============================================================================
-- Create FF_CONDITION_LOG Table
-- Logs which conditions were active for each interlock
-- ============================================================================
CREATE TABLE FF_CONDITION_LOG (
    ID INT PRIMARY KEY IDENTITY(1,1),
    INTERLOCK_LOG_ID INT NOT NULL,
    CONDITION_DEF_ID INT NOT NULL,
    CONSTRAINT FK_ConditionLog_InterlockLog FOREIGN KEY (INTERLOCK_LOG_ID)
        REFERENCES FF_INTERLOCK_LOG(ID),
    CONSTRAINT FK_ConditionLog_Definition FOREIGN KEY (CONDITION_DEF_ID)
        REFERENCES CONDITION_DEFINITION(CONDITION_DEF_ID)
);

-- ============================================================================
-- Create Indexes for Performance
-- ============================================================================
CREATE INDEX IX_InterlockLog_Timestamp ON FF_INTERLOCK_LOG(TIMESTAMP DESC);
CREATE INDEX IX_InterlockLog_DefID ON FF_INTERLOCK_LOG(INTERLOCK_DEF_ID);
CREATE INDEX IX_InterlockLog_Upstream ON FF_INTERLOCK_LOG(UPSTREAM_INTERLOCK_LOG_ID);
CREATE INDEX IX_ConditionLog_InterlockID ON FF_CONDITION_LOG(INTERLOCK_LOG_ID);
CREATE INDEX IX_InterlockDef_Number ON INTERLOCK_DEFINITION(NUMBER);
CREATE INDEX IX_InterlockDef_PLC ON INTERLOCK_DEFINITION(PLC_ID);
CREATE INDEX IX_InterlockDef_Text ON INTERLOCK_DEFINITION(TEXT_DEF_ID);
CREATE INDEX IX_ConditionDef_PLC_InterlockNum ON CONDITION_DEFINITION(PLC_ID, INTERLOCK_NUMBER);
CREATE INDEX IX_ConditionDef_Text ON CONDITION_DEFINITION(TEXT_DEF_ID);

PRINT '';
PRINT '========================================================================';
PRINT '✓ CORRECTED SCHEMA CREATED SUCCESSFULLY!';
PRINT '========================================================================';
PRINT '';
PRINT 'KEY FIX: CONDITION_DEFINITION now includes INTERLOCK_NUMBER column';
PRINT 'This correctly models that TYPE+BIT_INDEX is unique per interlock';
PRINT '';
PRINT 'You can now run the migration script!';
PRINT '';
PRINT '========================================================================';
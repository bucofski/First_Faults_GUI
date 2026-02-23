``` markdown
# First Fault Database Documentation

## Overview

The First Fault database tracks **interlock events** and their **conditions** in a PLC-based industrial control system. It enables root cause analysis by maintaining hierarchical relationships between interlock events, and supports trend analysis for fault monitoring.

---

## Database Schema

### Entity Relationship Diagram (Logical)
```

┌─────────────────────────────────────┐ │ CONDITION_LOG │ ├─────────────────────────────────────┤ │ - Unique ID │ │ - Interlock_REF ────────────────────┼──┐ │ - Type (BS1, BS2, BS3, ...) │ │ │ - BitIndex (BS-VW Bit) │ │ │ - Message │ │ │ - Mnemonic │ │ └─────────────────────────────────────┘ │ │ ┌───────────────────────────────┘ │ ▼ ┌─────────────────────────────────────┐ │ INTERLOCK_LOG │ ├─────────────────────────────────────┤ │ - Unique ID │ │ - PLC │ │ - Number (BSID) │ │ - Message │ │ - Mnemonic │ │ - Timestamp (TS) │ │ - TS_LOG │ │ - ORDER │ │ - UPSTREAM_INTERLOCK_REF ───────────┼──┐ └─────────────────────────────────────┘ │ ▲ │ └───────────────────────────────┘ (Self-Reference)``` 

---

## Tables

### Core Log Tables

| Table | Description |
|-------|-------------|
| `FF_INTERLOCK_LOG` | Records each interlock event occurrence |
| `FF_CONDITION_LOG` | Records conditions that triggered each interlock |

### Definition Tables (Normalized)

| Table | Description |
|-------|-------------|
| `INTERLOCK_DEFINITION` | Master data for interlock types (BSID numbers) |
| `CONDITION_DEFINITION` | Master data for condition types (Type, BitIndex) |
| `TEXT_DEFINITION` | Shared text storage (Message, Mnemonic) |
| `PLC` | PLC device master data |

### Trend Analysis Tables

| Table | Description |
|-------|-------------|
| `TREND_ANALYSIS_CONFIG` | Configuration for trend comparison periods |
| `FAULT_TREND_SNAPSHOTS` | Periodic snapshots of fault trend analysis |

---

## Table Details

### PLC

Stores PLC device information.

| Field | Type | Description |
|-------|------|-------------|
| `PLC_ID` | INT | Unique identifier (PK, Identity) |
| `PLC_NAME` | NVARCHAR(50) | PLC name (Unique) |
| `DESCRIPTION` | NVARCHAR(255) | Optional description |

---

### TEXT_DEFINITION

Stores all unique MNEMONIC + MESSAGE combinations (shared by interlocks and conditions).

| Field | Type | Description |
|-------|------|-------------|
| `TEXT_DEF_ID` | INT | Unique identifier (PK, Identity) |
| `MNEMONIC` | NVARCHAR(255) | Short code/abbreviation |
| `MESSAGE` | NVARCHAR(500) | Human-readable description |

**Unique Constraint:** `MNEMONIC` + `MESSAGE`

---

### INTERLOCK_DEFINITION

Defines interlock types: PLC + NUMBER → Text

| Field | Type | Description |
|-------|------|-------------|
| `INTERLOCK_DEF_ID` | INT | Unique identifier (PK, Identity) |
| `PLC_ID` | INT | FK to PLC |
| `NUMBER` | INT | BSID number (e.g., 12345) |
| `TEXT_DEF_ID` | INT | FK to text (message/mnemonic) |

**Unique Constraint:** `PLC_ID` + `NUMBER`

---

### CONDITION_DEFINITION

Defines condition types: PLC + INTERLOCK_NUMBER + TYPE + BIT_INDEX → Text

| Field | Type | Description |
|-------|------|-------------|
| `CONDITION_DEF_ID` | INT | Unique identifier (PK, Identity) |
| `PLC_ID` | INT | FK to PLC |
| `INTERLOCK_NUMBER` | INT | Associated interlock BSID |
| `TYPE` | INT | Condition type (BS1, BS2, BS3, etc.) |
| `BIT_INDEX` | INT | Bit position in the PLC word |
| `TEXT_DEF_ID` | INT | FK to text (message/mnemonic) |

**Unique Constraint:** `PLC_ID` + `INTERLOCK_NUMBER` + `TYPE` + `BIT_INDEX`

> **Note:** `INTERLOCK_NUMBER` is required because `TYPE` + `BIT_INDEX` is only unique **within each interlock**, not globally.

---

### FF_INTERLOCK_LOG

Logs when interlocks occur.

| Field | Type | Description |
|-------|------|-------------|
| `ID` | INT | Unique identifier (PK, Identity) |
| `INTERLOCK_DEF_ID` | INT | FK to interlock definition |
| `TIMESTAMP` | DATETIME | When the interlock occurred |
| `TIMESTAMP_LOG` | DATETIME | When the log entry was created |
| `ORDER_LOG` | INT | Sequence order for same-timestamp events |
| `UPSTREAM_INTERLOCK_LOG_ID` | INT | FK to parent interlock (self-reference, nullable) |

---

### FF_CONDITION_LOG

Logs which conditions were active for each interlock.

| Field | Type | Description |
|-------|------|-------------|
| `ID` | INT | Unique identifier (PK, Identity) |
| `INTERLOCK_LOG_ID` | INT | FK to the interlock this condition belongs to |
| `CONDITION_DEF_ID` | INT | FK to condition definition |

---

### TREND_ANALYSIS_CONFIG

Stores configuration parameters for trend analysis comparisons.

| Field | Type | Description |
|-------|------|-------------|
| `CONFIG_ID` | INT | Unique identifier (PK, Identity) |
| `DAYS_RECENT` | INT | Number of days for recent period |
| `DAYS_PREVIOUS` | INT | Number of days for comparison period |

**Unique Constraint:** `DAYS_RECENT` + `DAYS_PREVIOUS`

---

### FAULT_TREND_SNAPSHOTS

Stores periodic snapshots of fault trend analysis for historical tracking.

| Field | Type | Description |
|-------|------|-------------|
| `SNAPSHOT_ID` | INT | Unique identifier (PK, Identity) |
| `SNAPSHOT_DATE` | DATE | Date of the snapshot |
| `CONDITION_DEF_ID` | INT | FK to condition definition |
| `CONFIG_ID` | INT | FK to trend analysis config |
| `RECENT_DAILY_AVG` | DECIMAL(10,2) | Average daily occurrences (recent period) |
| `PREVIOUS_DAILY_AVG` | DECIMAL(10,2) | Average daily occurrences (previous period) |
| `CHANGE_PERCENT` | DECIMAL(10,2) | Percentage change between periods |
| `ABSOLUTE_CHANGE` | DECIMAL(10,2) | Absolute change in occurrences |
| `RECENT_COUNT` | INT | Total count in recent period |
| `PREVIOUS_COUNT` | INT | Total count in previous period |
| `CONFIDENCE_SCORE` | DECIMAL(15,2) | Statistical confidence metric |
| `RANK_POSITION` | INT | Ranking among all conditions |
| `CREATED_AT` | DATETIME | When snapshot was created (default: GETDATE()) |

**Unique Constraint:** `SNAPSHOT_DATE` + `CONDITION_DEF_ID` + `CONFIG_ID`

---

## Key Relationships

### 1. Interlock → Conditions (One-to-Many)

Each interlock event can have **multiple conditions** that caused it.
```

FF_INTERLOCK_LOG.ID ←── FF_CONDITION_LOG.INTERLOCK_LOG_ID``` 

### 2. Interlock → Upstream Interlock (Self-Reference)

An interlock can reference another interlock as its **upstream cause**, forming a chain.
```

FF_INTERLOCK_LOG.UPSTREAM_INTERLOCK_LOG_ID → FF_INTERLOCK_LOG.ID``` 

This enables **root cause analysis** by traversing:
- **Upstream**: Follow `UPSTREAM_INTERLOCK_LOG_ID` to find the original cause
- **Downstream**: Find all interlocks that reference the current one as their upstream

### 3. Definition Lookups
```

FF_INTERLOCK_LOG.INTERLOCK_DEF_ID → INTERLOCK_DEFINITION.INTERLOCK_DEF_ID INTERLOCK_DEFINITION.PLC_ID → PLC.PLC_ID INTERLOCK_DEFINITION.TEXT_DEF_ID → TEXT_DEFINITION.TEXT_DEF_ID
FF_CONDITION_LOG.CONDITION_DEF_ID → CONDITION_DEFINITION.CONDITION_DEF_ID CONDITION_DEFINITION.PLC_ID → PLC.PLC_ID CONDITION_DEFINITION.TEXT_DEF_ID → TEXT_DEFINITION.TEXT_DEF_ID``` 

### 4. Trend Analysis Relationships
```

FAULT_TREND_SNAPSHOTS.CONDITION_DEF_ID → CONDITION_DEFINITION.CONDITION_DEF_ID FAULT_TREND_SNAPSHOTS.CONFIG_ID → TREND_ANALYSIS_CONFIG.CONFIG_ID``` 

---

## Root Cause Analysis

The `UPSTREAM_INTERLOCK_LOG_ID` field enables tracing fault chains:
```

[ROOT CAUSE] [EFFECT 1] [EFFECT 2] │ │ │ ▼ ▼ ▼ ┌─────────┐ ┌─────────┐ ┌─────────┐ │ IL #100 │ ◄─────── │ IL #101 │ ◄─────── │ IL #102 │ └─────────┘ └─────────┘ └─────────┘ (no upstream) (upstream=100) (upstream=101)``` 

- **IL #100**: Root cause (no `UPSTREAM_INTERLOCK_LOG_ID`)
- **IL #101**: Effect of #100
- **IL #102**: Effect of #101

---

## Complete Physical Schema Diagram
```

┌──────────────────┐ ┌──────────────────────┐ ┌─────────────────┐ │ PLC │ │ INTERLOCK_DEFINITION │ │ TEXT_DEFINITION │ ├──────────────────┤ ├──────────────────────┤ ├─────────────────┤ │ PLC_ID (PK) │◄────│ PLC_ID (FK) │ │ TEXT_DEF_ID(PK) │ │ PLC_NAME │ │ INTERLOCK_DEF_ID(PK) │────►│ MESSAGE │ │ DESCRIPTION │ │ NUMBER (BSID) │ │ MNEMONIC │ └──────────────────┘ │ TEXT_DEF_ID (FK) ────┼────►└─────────────────┘ ▲ └──────────────────────┘ ▲ │ ▲ │ │ │ │ │ ┌──────────┴─────────┐ ┌──────────┴───────────┐ │ │ FF_INTERLOCK_LOG │ │ CONDITION_DEFINITION │ │ ├────────────────────┤ ├──────────────────────┤ │ ┌─────────►│ ID (PK) │ │ CONDITION_DEF_ID(PK) │ │ │ │ TIMESTAMP │ │ PLC_ID (FK) ─────────┼──┘ │ │ │ TIMESTAMP_LOG │ │ INTERLOCK_NUMBER │ │ │ │ ORDER_LOG │ │ TYPE │ │ │ │ INTERLOCK_DEF_ID │ │ BIT_INDEX │ │ │ │ UPSTREAM_IL_LOG_ID─┼──┐ │ TEXT_DEF_ID (FK) │ │ │ └────────────────────┘ │ └──────────────────────┘ │ │ ▲ │ ▲ │ └─────────────────────┼────────────┘ │ │ (self-ref) │ │ │ ┌──────────┴─────────┐ │ │ │ FF_CONDITION_LOG │ │ │ ├────────────────────┤ │ │ │ ID (PK) │ │ │ │ INTERLOCK_LOG_ID ──┼────────────────┘ │ │ CONDITION_DEF_ID ──┼────────────────┘ │ └────────────────────┘ │ │ ┌─────────────────────────┐ ┌────────────────────────┐ │ │ TREND_ANALYSIS_CONFIG │ │ FAULT_TREND_SNAPSHOTS │ │ ├─────────────────────────┤ ├────────────────────────┤ │ │ CONFIG_ID (PK) │◄────│ CONFIG_ID (FK) │ │ │ DAYS_RECENT │ │ SNAPSHOT_ID (PK) │ │ │ DAYS_PREVIOUS │ │ SNAPSHOT_DATE │ │ └─────────────────────────┘ │ CONDITION_DEF_ID (FK)──┼───► │ │ RECENT_DAILY_AVG │ └───────────────────────────────────│ CHANGE_PERCENT │ │ RANK_POSITION │ │ ... │ └────────────────────────┘``` 

---

## Query Function

Use `dbo.fn_InterlockChain()` to retrieve full interlock chains with root cause analysis.

### Function Signature
```

sql dbo.fn_InterlockChain ( @TargetBSID INT = NULL, @TopN INT = NULL, @FilterTimestampStart DATETIME = NULL, @FilterTimestampEnd DATETIME = NULL, @FilterConditionMessage NVARCHAR(255) = NULL, @FilterPLC NVARCHAR(50) = NULL )``` 

### Parameters

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `@TargetBSID` | INT | NULL | Filter by specific BSID number |
| `@TopN` | INT | 100 | Limit number of anchor interlocks |
| `@FilterTimestampStart` | DATETIME | NULL | Start of time range (date or datetime) |
| `@FilterTimestampEnd` | DATETIME | NULL | End of time range (date or datetime) |
| `@FilterConditionMessage` | NVARCHAR(255) | NULL | Search in condition message/mnemonic |
| `@FilterPLC` | NVARCHAR(50) | NULL | Filter by PLC name |

> **Smart Date Handling:** If you pass a date without time (e.g., `'2024-12-05'`), the function automatically expands it to cover the full day.

### Usage Examples
```

sql -- Get last 100 interlocks with their full trees SELECT * FROM dbo.fn_InterlockChain(NULL, NULL, NULL, NULL, NULL, NULL) ORDER BY AnchorReference, Level;
-- Get specific BSID with top 10 occurrences SELECT * FROM dbo.fn_InterlockChain(12345, 10, NULL, NULL, NULL, NULL) ORDER BY AnchorReference, Level;
-- Filter by date range SELECT * FROM dbo.fn_InterlockChain(NULL, NULL, '2024-12-01', '2024-12-05', NULL, NULL) ORDER BY AnchorReference, Level;
-- Filter by specific datetime range SELECT * FROM dbo.fn_InterlockChain(NULL, NULL, '2024-12-05 08:00:00', '2024-12-05 16:00:00', NULL, NULL) ORDER BY AnchorReference, Level;
-- Search by condition message and PLC SELECT * FROM dbo.fn_InterlockChain(NULL, NULL, NULL, NULL, 'Emergency', 'PLC_001') ORDER BY AnchorReference, Level;``` 

### Output Columns

| Column | Description |
|--------|-------------|
| `AnchorReference` | ID of the starting interlock for this tree |
| `Date` | Date of the event |
| `Level` | Position in chain (0=anchor, positive=upstream, negative=downstream) |
| `Direction` | ANCHOR, UPSTREAM, or DOWNSTREAM |
| `Interlock_Log_ID` | Unique ID of this interlock event |
| `TIMESTAMP` | When the interlock occurred |
| `PLC` | PLC name |
| `BSID` | Interlock number |
| `Interlock_Message` | Interlock description |
| `TYPE` | Condition type |
| `BIT_INDEX` | Condition bit index |
| `Condition_Mnemonic` | Condition short code |
| `Condition_Message` | Condition description (falls back to mnemonic if empty) |
| `UPSTREAM_INTERLOCK_REF` | Reference to upstream interlock |
| `Status` | `*** ROOT CAUSE ***`, `*** STARTING POINT ***`, or `EFFECT` |

---

## Indexes

### Performance Indexes

| Index | Table | Columns | Purpose |
|-------|-------|---------|---------|
| `IX_InterlockLog_Timestamp` | FF_INTERLOCK_LOG | TIMESTAMP DESC | Fast time-based queries |
| `IX_InterlockLog_DefID` | FF_INTERLOCK_LOG | INTERLOCK_DEF_ID | Definition lookups |
| `IX_InterlockLog_Upstream` | FF_INTERLOCK_LOG | UPSTREAM_INTERLOCK_LOG_ID | Chain traversal |
| `IX_ConditionLog_InterlockID` | FF_CONDITION_LOG | INTERLOCK_LOG_ID | Condition lookups |
| `IX_InterlockDef_Number` | INTERLOCK_DEFINITION | NUMBER | BSID searches |
| `IX_InterlockDef_PLC` | INTERLOCK_DEFINITION | PLC_ID | PLC filtering |
| `IX_InterlockDef_Text` | INTERLOCK_DEFINITION | TEXT_DEF_ID | Text lookups |
| `IX_ConditionDef_PLC_InterlockNum` | CONDITION_DEFINITION | PLC_ID, INTERLOCK_NUMBER | Condition filtering |
| `IX_ConditionDef_Text` | CONDITION_DEFINITION | TEXT_DEF_ID | Text lookups |

### Trend Analysis Indexes

| Index | Table | Columns | Purpose |
|-------|-------|---------|---------|
| `IX_FaultTrendSnapshots_SnapshotDate` | FAULT_TREND_SNAPSHOTS | SNAPSHOT_DATE DESC | Date-based queries |
| `IX_FaultTrendSnapshots_Condition` | FAULT_TREND_SNAPSHOTS | CONDITION_MESSAGE | Condition searches |
| `IX_FaultTrendSnapshots_DateCondition` | FAULT_TREND_SNAPSHOTS | SNAPSHOT_DATE, CONDITION_MESSAGE | Combined filtering |
| `IX_FaultTrendSnapshots_RankPosition` | FAULT_TREND_SNAPSHOTS | SNAPSHOT_DATE, RANK_POSITION | Ranking queries |

### Recommended Additional Index

For optimal chain traversal performance:
```

sql CREATE NONCLUSTERED INDEX IX_FF_INTERLOCK_LOG_UpstreamRef ON First_Fault.dbo.FF_INTERLOCK_LOG (UPSTREAM_INTERLOCK_LOG_ID, TIMESTAMP) INCLUDE (ID, INTERLOCK_DEF_ID, ORDER_LOG);``` 

---

## Schema Notes

### Why INTERLOCK_NUMBER in CONDITION_DEFINITION?

The `TYPE` + `BIT_INDEX` combination is only unique **within each interlock**, not globally across the system. For example:
- Interlock 12345 might have `TYPE=1, BIT_INDEX=0` meaning "Motor Overload"
- Interlock 67890 might have `TYPE=1, BIT_INDEX=0` meaning "Pressure High"

Including `INTERLOCK_NUMBER` ensures proper uniqueness:
```

sql CONSTRAINT UQ_PLC_InterlockNum_Type_BitIndex UNIQUE (PLC_ID, INTERLOCK_NUMBER, TYPE, BIT_INDEX)``` 

### Normalization Benefits

The schema is normalized to:
1. **Reduce redundancy** - Text stored once, referenced many times
2. **Ensure consistency** - Changes to definitions apply everywhere
3. **Improve performance** - Smaller log tables, faster queries
4. **Enable flexibility** - Easy to add new PLCs, interlocks, or conditions
```

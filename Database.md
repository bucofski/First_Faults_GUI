# First Fault Database Documentation

## Overview

The First Fault database (`First_Fault` on SQL Server `192.168.0.30:1433`) tracks **interlock events** and their **conditions** in a PLC-based industrial control system. It enables root cause analysis by maintaining hierarchical relationships between interlock events, and supports trend analysis through pre-computed reporting snapshots.

### Database Statistics

| Table | Rows |
|-------|-----:|
| PLC | 8 |
| TEXT_DEFINITION | 2,786 |
| INTERLOCK_DEFINITION | 925 |
| CONDITION_DEFINITION | 2,439 |
| FF_INTERLOCK_LOG | 817,094 |
| FF_CONDITION_LOG | 1,756,251 |
| daily_hour_snapshot | 2,184 |
| daily_plc_snapshot | 198 |
| top_riser_snapshot | 250 |
| mtbf_snapshot | 16 |
| repeat_offender_snapshot | 20 |
| long_term_trend_snapshot | 2,495 |

---

## Database Schema

### Entity Relationship Diagram (Logical)

```
┌─────────────────────────────┐
│       CONDITION_LOG         │
├─────────────────────────────┤
│ - ID (PK)                  │
│ - INTERLOCK_LOG_ID (FK) ────┼──┐
│ - CONDITION_DEF_ID (FK) ────┼──┼──┐
└─────────────────────────────┘  │  │
                                 │  │
    ┌────────────────────────────┘  │
    │                               │
    ▼                               ▼
┌─────────────────────────────┐  ┌─────────────────────────────────┐
│       INTERLOCK_LOG         │  │     CONDITION_DEFINITION        │
├─────────────────────────────┤  ├─────────────────────────────────┤
│ - ID (PK)                  │  │ - CONDITION_DEF_ID (PK)         │
│ - INTERLOCK_DEF_ID (FK)    │  │ - PLC_ID (FK)                   │
│ - TIMESTAMP                │  │ - INTERLOCK_NUMBER              │
│ - TIMESTAMP_LOG            │  │ - TYPE                          │
│ - ORDER_LOG                │  │ - BIT_INDEX                     │
│ - UPSTREAM_IL_LOG_ID (FK)──┼──┐│ - TEXT_DEF_ID (FK)             │
└─────────────────────────────┘  │└─────────────────────────────────┘
    ▲                            │
    └────────────────────────────┘
           (Self-Reference)
```

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
| `INTERLOCK_DEFINITION` | Master data for interlock types (PLC + BSID number) |
| `CONDITION_DEFINITION` | Master data for condition types (PLC + INTERLOCK_NUMBER + TYPE + BIT_INDEX) |
| `TEXT_DEFINITION` | Shared text storage (Mnemonic + Message). Kolom `REPORTING` (BIT) bepaalt of het mnemonic in rapportage-views wordt opgenomen |
| `PLC` | PLC device master data |

### Reporting Views

| View | Description |
|------|-------------|
| `vw_root_cause_faults` | Root cause faults (interlocks zonder upstream), gefilterd op `TEXT_DEFINITION.REPORTING = 1` |

### Reporting Snapshot Tables

| Table | Description |
|-------|-------------|
| `daily_hour_snapshot` | Fouten per uur (Brussels lokale tijd), per snapshot-datum |
| `daily_plc_snapshot` | Fouten per PLC, per snapshot-datum |
| `top_riser_snapshot` | Top stijgers (recent vs baseline periode), per snapshot-datum |
| `mtbf_snapshot` | Mean Time Between Failures per PLC, per snapshot-datum |
| `repeat_offender_snapshot` | Faults met hoogste herhalingen per uur, per snapshot-datum |
| `long_term_trend_snapshot` | Wekelijkse fouttellingen per fault, voor langetermijntrends |

---

## Table Details

### PLC

Stores PLC device information.

| Field | Type | Description |
|-------|------|-------------|
| `PLC_ID` | INT (PK, Identity) | Unique identifier |
| `PLC_NAME` | NVARCHAR(50) | PLC name (Unique) |
| `DESCRIPTION` | NVARCHAR(255) | Optional description |

**ORM:** `data/orm/reporting_orm.py` → `Plc`

---

### TEXT_DEFINITION

Stores all unique MNEMONIC + MESSAGE combinations (shared by interlocks and conditions).

| Field | Type | Description |
|-------|------|-------------|
| `TEXT_DEF_ID` | INT (PK, Identity) | Unique identifier |
| `MNEMONIC` | NVARCHAR(255) | Short code/abbreviation |
| `MESSAGE` | NVARCHAR(500) | Human-readable description |
| `REPORTING` | BIT NOT NULL (DEFAULT 1) | Of dit mnemonic in rapportage-views wordt opgenomen |

**Unique Constraint:** `MNEMONIC` + `MESSAGE`

**ORM:** `data/orm/reporting_orm.py` → `TextDefinition`

---

### INTERLOCK_DEFINITION

Defines interlock types: PLC + NUMBER → Text

| Field | Type | Description |
|-------|------|-------------|
| `INTERLOCK_DEF_ID` | INT (PK, Identity) | Unique identifier |
| `PLC_ID` | INT (FK → PLC) | PLC device |
| `NUMBER` | INT | BSID number (e.g., 12345) |
| `TEXT_DEF_ID` | INT (FK → TEXT_DEFINITION) | Message/mnemonic |

**Unique Constraint:** `PLC_ID` + `NUMBER`

---

### CONDITION_DEFINITION

Defines condition types: PLC + INTERLOCK_NUMBER + TYPE + BIT_INDEX → Text

| Field | Type | Description |
|-------|------|-------------|
| `CONDITION_DEF_ID` | INT (PK, Identity) | Unique identifier |
| `PLC_ID` | INT (FK → PLC) | PLC device |
| `INTERLOCK_NUMBER` | INT | Associated interlock BSID |
| `TYPE` | INT | Condition type (BS1, BS2, BS3, etc.) |
| `BIT_INDEX` | INT | Bit position in the PLC word |
| `TEXT_DEF_ID` | INT (FK → TEXT_DEFINITION) | Message/mnemonic |

**Foreign Keys:**
- `PLC_ID` → `PLC.PLC_ID`
- `(PLC_ID, INTERLOCK_NUMBER)` → `INTERLOCK_DEFINITION.(PLC_ID, NUMBER)` (composite FK)
- `TEXT_DEF_ID` → `TEXT_DEFINITION.TEXT_DEF_ID`

**Unique Constraint:** `PLC_ID` + `INTERLOCK_NUMBER` + `TYPE` + `BIT_INDEX`

> **Note:** `INTERLOCK_NUMBER` is required because `TYPE` + `BIT_INDEX` is only unique **within each interlock**, not globally.

---

### FF_INTERLOCK_LOG

Logs when interlocks occur.

| Field | Type | Description |
|-------|------|-------------|
| `ID` | INT (PK, Identity) | Unique identifier |
| `INTERLOCK_DEF_ID` | INT (FK → INTERLOCK_DEFINITION) | Interlock type |
| `TIMESTAMP` | DATETIME | When the interlock occurred (UTC) |
| `TIMESTAMP_LOG` | DATETIME | When the log entry was created |
| `ORDER_LOG` | INT | Sequence order for same-timestamp events |
| `UPSTREAM_INTERLOCK_LOG_ID` | INT (FK → self, nullable) | Parent interlock |

---

### FF_CONDITION_LOG

Logs which conditions were active for each interlock.

| Field | Type | Description |
|-------|------|-------------|
| `ID` | INT (PK, Identity) | Unique identifier |
| `INTERLOCK_LOG_ID` | INT (FK → FF_INTERLOCK_LOG) | Parent interlock |
| `CONDITION_DEF_ID` | INT (FK → CONDITION_DEFINITION) | Condition type |

---

## Reporting View

### vw_root_cause_faults

View die root cause faults selecteert — interlocks zonder upstream referentie, gefilterd op mnemonics waar `REPORTING = 1`.

```sql
CREATE OR ALTER VIEW vw_root_cause_faults AS
SELECT
    fil.ID             AS fault_id,
    fil.TIMESTAMP      AS utc_timestamp,
    id_.PLC_ID,
    id_.TEXT_DEF_ID,
    p.PLC_NAME,
    td.MNEMONIC
FROM dbo.FF_INTERLOCK_LOG   fil
JOIN dbo.INTERLOCK_DEFINITION id_ ON id_.INTERLOCK_DEF_ID = fil.INTERLOCK_DEF_ID
JOIN dbo.PLC                p     ON p.PLC_ID              = id_.PLC_ID
JOIN dbo.TEXT_DEFINITION    td    ON td.TEXT_DEF_ID         = id_.TEXT_DEF_ID
WHERE fil.UPSTREAM_INTERLOCK_LOG_ID IS NULL
  AND td.REPORTING = 1;
```

**ORM:** `data/orm/reporting_orm.py` → `RootCauseFault`
**Gebruikt door:** `FaultCountService` voor live fault counting en trend analyse

---

## Reporting Snapshot Tables

Snapshot tabellen worden dagelijks gevuld door `run_daily_snapshot.py` en bevatten vooraf berekende rapportagedata. De `DiagramService` leest deze snapshots; als er geen snapshot beschikbaar is, valt het terug op live queries via `FaultCountService`.

### daily_hour_snapshot

| Field | Type | Description |
|-------|------|-------------|
| `id` | INT (PK, Identity) | Unique identifier |
| `snapshot_date` | DATE | Datum van de snapshot |
| `hour` | TINYINT | Uur (0-23, Brussels lokale tijd) |
| `fault_count` | INT | Aantal fouten in dat uur |

**Unique Constraint:** `snapshot_date` + `hour`

### daily_plc_snapshot

| Field | Type | Description |
|-------|------|-------------|
| `id` | INT (PK, Identity) | Unique identifier |
| `snapshot_date` | DATE | Datum van de snapshot |
| `plc_id` | INT (FK → PLC) | PLC device |
| `fault_count` | INT | Aantal fouten voor die PLC |

**Unique Constraint:** `snapshot_date` + `plc_id`

### top_riser_snapshot

| Field | Type | Description |
|-------|------|-------------|
| `id` | INT (PK, Identity) | Unique identifier |
| `snapshot_date` | DATE | Datum van de snapshot |
| `recent_days` | TINYINT | Lengte recente periode (default 7) |
| `baseline_days` | SMALLINT | Lengte baseline periode (default 30) |
| `plc_id` | INT (FK → PLC) | PLC device |
| `text_def_id` | INT (FK → TEXT_DEFINITION) | Fault mnemonic |
| `recent_count` | INT | Telling in recente periode |
| `baseline_count` | INT | Telling in baseline periode |
| `delta_pct` | FLOAT | Procentuele stijging |

**Unique Constraint:** `snapshot_date` + `recent_days` + `baseline_days` + `plc_id` + `text_def_id`

### mtbf_snapshot

| Field | Type | Description |
|-------|------|-------------|
| `id` | INT (PK, Identity) | Unique identifier |
| `snapshot_date` | DATE | Datum van de snapshot |
| `days_window` | SMALLINT | Aantal dagen in de window (default 30) |
| `plc_id` | INT (FK → PLC) | PLC device |
| `avg_hours` | FLOAT | Gemiddelde uren tussen fouten |
| `fault_count` | INT | Totaal aantal fouten in window |

**Unique Constraint:** `snapshot_date` + `days_window` + `plc_id`

### repeat_offender_snapshot

| Field | Type | Description |
|-------|------|-------------|
| `id` | INT (PK, Identity) | Unique identifier |
| `snapshot_date` | DATE | Datum van de snapshot |
| `days_window` | SMALLINT | Aantal dagen in de window |
| `plc_id` | INT (FK → PLC) | PLC device |
| `text_def_id` | INT (FK → TEXT_DEFINITION) | Fault mnemonic |
| `max_per_hour` | INT | Maximaal aantal keer in een enkel uur |

**Unique Constraint:** `snapshot_date` + `days_window` + `plc_id` + `text_def_id`

### long_term_trend_snapshot

| Field | Type | Description |
|-------|------|-------------|
| `id` | INT (PK, Identity) | Unique identifier |
| `week_start` | DATE | Maandag van de week |
| `plc_id` | INT (FK → PLC) | PLC device |
| `text_def_id` | INT (FK → TEXT_DEFINITION) | Fault mnemonic |
| `weekly_count` | INT | Aantal fouten die week |

**Unique Constraint:** `week_start` + `plc_id` + `text_def_id`

---

## Key Relationships

### 1. Interlock → Conditions (One-to-Many)

Each interlock event can have **multiple conditions** that caused it.

```
FF_INTERLOCK_LOG.ID  ←──  FF_CONDITION_LOG.INTERLOCK_LOG_ID
```

### 2. Interlock → Upstream Interlock (Self-Reference)

An interlock can reference another interlock as its **upstream cause**, forming a chain.

```
FF_INTERLOCK_LOG.UPSTREAM_INTERLOCK_LOG_ID  →  FF_INTERLOCK_LOG.ID
```

This enables **root cause analysis** by traversing:
- **Upstream**: Follow `UPSTREAM_INTERLOCK_LOG_ID` to find the original cause
- **Downstream**: Find all interlocks that reference the current one as their upstream

### 3. Definition Lookups

```
FF_INTERLOCK_LOG.INTERLOCK_DEF_ID  →  INTERLOCK_DEFINITION.INTERLOCK_DEF_ID
  INTERLOCK_DEFINITION.PLC_ID       →  PLC.PLC_ID
  INTERLOCK_DEFINITION.TEXT_DEF_ID  →  TEXT_DEFINITION.TEXT_DEF_ID

FF_CONDITION_LOG.CONDITION_DEF_ID  →  CONDITION_DEFINITION.CONDITION_DEF_ID
  CONDITION_DEFINITION.PLC_ID       →  PLC.PLC_ID
  CONDITION_DEFINITION.TEXT_DEF_ID  →  TEXT_DEFINITION.TEXT_DEF_ID
```

### 4. Snapshot → Definition Lookups

```
daily_plc_snapshot.plc_id           →  PLC.PLC_ID
top_riser_snapshot.plc_id           →  PLC.PLC_ID
top_riser_snapshot.text_def_id      →  TEXT_DEFINITION.TEXT_DEF_ID
mtbf_snapshot.plc_id                →  PLC.PLC_ID
repeat_offender_snapshot.plc_id     →  PLC.PLC_ID
repeat_offender_snapshot.text_def_id→  TEXT_DEFINITION.TEXT_DEF_ID
long_term_trend_snapshot.plc_id     →  PLC.PLC_ID
long_term_trend_snapshot.text_def_id→  TEXT_DEFINITION.TEXT_DEF_ID
```

---

## Root Cause Analysis

The `UPSTREAM_INTERLOCK_LOG_ID` field enables tracing fault chains:

```
[ROOT CAUSE]          [EFFECT 1]          [EFFECT 2]
     │                     │                    │
     ▼                     ▼                    ▼
┌─────────┐          ┌─────────┐          ┌─────────┐
│ IL #100 │  ◄────── │ IL #101 │  ◄────── │ IL #102 │
└─────────┘          └─────────┘          └─────────┘
(no upstream)        (upstream=100)       (upstream=101)
```

- **IL #100**: Root cause (no `UPSTREAM_INTERLOCK_LOG_ID`)
- **IL #101**: Effect of #100
- **IL #102**: Effect of #101

---

## Query Function

Use `dbo.fn_InterlockChain()` (inline table-valued function) to retrieve full interlock chains with root cause analysis.

### Function Signature

```sql
dbo.fn_InterlockChain (
    @TargetBSID              INT           = NULL,
    @TopN                    INT           = NULL,
    @FilterTimestampStart    DATETIME      = NULL,
    @FilterTimestampEnd      DATETIME      = NULL,
    @FilterConditionMessage  NVARCHAR(255) = NULL,
    @FilterPLC               NVARCHAR(50)  = NULL
)
```

### Parameters

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `@TargetBSID` | INT | NULL | Filter by specific BSID number |
| `@TopN` | INT | 100 | Limit number of anchor interlocks |
| `@FilterTimestampStart` | DATETIME | NULL | Start of time range |
| `@FilterTimestampEnd` | DATETIME | NULL | End of time range |
| `@FilterConditionMessage` | NVARCHAR(255) | NULL | Search in condition message/mnemonic |
| `@FilterPLC` | NVARCHAR(50) | NULL | Filter by PLC name |

> **Smart Date Handling:** If you pass a date without time (e.g., `'2024-12-05'`), the function automatically expands it to cover the full day.

### Usage Examples

```sql
-- Get last 100 interlocks with their full trees
SELECT * FROM dbo.fn_InterlockChain(NULL, NULL, NULL, NULL, NULL, NULL)
ORDER BY AnchorReference, Level;

-- Get specific BSID with top 10 occurrences
SELECT * FROM dbo.fn_InterlockChain(12345, 10, NULL, NULL, NULL, NULL)
ORDER BY AnchorReference, Level;

-- Filter by date range
SELECT * FROM dbo.fn_InterlockChain(NULL, NULL, '2024-12-01', '2024-12-05', NULL, NULL)
ORDER BY AnchorReference, Level;

-- Search by condition message and PLC
SELECT * FROM dbo.fn_InterlockChain(NULL, NULL, NULL, NULL, 'Emergency', 'PLC_001')
ORDER BY AnchorReference, Level;
```

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
| `Condition_Message` | Condition description |
| `UPSTREAM_INTERLOCK_REF` | Reference to upstream interlock |
| `Status` | `*** ROOT CAUSE ***`, `*** STARTING POINT ***`, or `EFFECT` |

---

## ORM Mapping

De applicatie gebruikt SQLAlchemy ORM modellen in `data/orm/reporting_orm.py`:

| ORM Class | Table/View |
|-----------|------------|
| `Plc` | `PLC` |
| `TextDefinition` | `TEXT_DEFINITION` |
| `RootCauseFault` | `vw_root_cause_faults` (read-only view) |
| `DailyHourSnapshot` | `daily_hour_snapshot` |
| `DailyPlcSnapshot` | `daily_plc_snapshot` |
| `TopRiserSnapshot` | `top_riser_snapshot` |
| `MtbfSnapshot` | `mtbf_snapshot` |
| `RepeatOffenderSnapshot` | `repeat_offender_snapshot` |
| `LongTermTrendSnapshot` | `long_term_trend_snapshot` |

Domain models in `data/model/models.py`:

| Model | Description |
|-------|-------------|
| `InterlockNode` | Node in de interlock chain tree (met children, conditions) |
| `InterlockCondition` | Enkele conditie binnen een interlock (type, bit_index, message) |

---

## Data Pipeline

### Dagelijkse snapshot (`run_daily_snapshot.py`)

Wordt dagelijks uitgevoerd en vult alle snapshot-tabellen:

```
vw_root_cause_faults  →  FaultCountService  →  SnapshotRepository.save_*()
                             │
                             ├── by_hour          → daily_hour_snapshot
                             ├── by_plc           → daily_plc_snapshot
                             ├── top_risers       → top_riser_snapshot
                             ├── mtbf             → mtbf_snapshot
                             ├── repeat_offenders → repeat_offender_snapshot
                             └── weekly_trend     → long_term_trend_snapshot
```

### Backfill (`scripts/backfill_snapshots.py`)

Vult snapshot-tabellen met historische data voor meerdere databases.

### Cleanup

`SnapshotRepository.cleanup_old_snapshots()` verwijdert snapshots ouder dan 90 dagen (configureerbaar).

---

## Indexes

### All Indexes (from live database)

| Table | Index | Columns | Type |
|-------|-------|---------|------|
| PLC | `UQ__PLC__...` | PLC_NAME | UNIQUE |
| TEXT_DEFINITION | `UQ_Mnemonic_Message` | MNEMONIC, MESSAGE | UNIQUE |
| INTERLOCK_DEFINITION | `IX_InterlockDef_Number` | NUMBER | Index |
| INTERLOCK_DEFINITION | `IX_InterlockDef_PLC` | PLC_ID | Index |
| INTERLOCK_DEFINITION | `IX_InterlockDef_Text` | TEXT_DEF_ID | Index |
| INTERLOCK_DEFINITION | `UQ_PLC_Number` | PLC_ID, NUMBER | UNIQUE |
| CONDITION_DEFINITION | `IX_ConditionDef_PLC_InterlockNum` | PLC_ID, INTERLOCK_NUMBER | Index |
| CONDITION_DEFINITION | `IX_ConditionDef_Text` | TEXT_DEF_ID | Index |
| CONDITION_DEFINITION | `UQ_PLC_InterlockNum_Type_BitIndex` | PLC_ID, INTERLOCK_NUMBER, TYPE, BIT_INDEX | UNIQUE |
| FF_INTERLOCK_LOG | `IX_InterlockLog_Timestamp` | TIMESTAMP | Index |
| FF_INTERLOCK_LOG | `IX_InterlockLog_DefID` | INTERLOCK_DEF_ID | Index |
| FF_INTERLOCK_LOG | `IX_InterlockLog_Upstream` | UPSTREAM_INTERLOCK_LOG_ID | Index |
| FF_CONDITION_LOG | `IX_ConditionLog_InterlockID` | INTERLOCK_LOG_ID | Index |
| daily_hour_snapshot | `UQ_hour_snapshot` | snapshot_date, hour | UNIQUE |
| daily_plc_snapshot | `UQ_plc_snapshot` | snapshot_date, plc_id | UNIQUE |
| top_riser_snapshot | `UQ_top_riser` | snapshot_date, recent_days, baseline_days, plc_id, text_def_id | UNIQUE |
| mtbf_snapshot | `UQ_mtbf` | snapshot_date, days_window, plc_id | UNIQUE |
| repeat_offender_snapshot | `UQ_repeat` | snapshot_date, days_window, plc_id, text_def_id | UNIQUE |
| long_term_trend_snapshot | `UQ_trend` | week_start, plc_id, text_def_id | UNIQUE |

---

## Schema Notes

### Why INTERLOCK_NUMBER in CONDITION_DEFINITION?

The `TYPE` + `BIT_INDEX` combination is only unique **within each interlock**, not globally. For example:
- Interlock 12345 might have `TYPE=1, BIT_INDEX=0` meaning "Motor Overload"
- Interlock 67890 might have `TYPE=1, BIT_INDEX=0` meaning "Pressure High"

Including `INTERLOCK_NUMBER` ensures proper uniqueness:

```sql
CONSTRAINT UQ_PLC_InterlockNum_Type_BitIndex
    UNIQUE (PLC_ID, INTERLOCK_NUMBER, TYPE, BIT_INDEX)
```

### Normalization Benefits

1. **Reduce redundancy** — Text stored once, referenced many times
2. **Ensure consistency** — Changes to definitions apply everywhere
3. **Improve performance** — Smaller log tables, faster queries
4. **Enable flexibility** — Easy to add new PLCs, interlocks, or conditions

### Snapshot vs Live

De applicatie ondersteunt twee datamodi:
- **Snapshot** (default): Leest uit pre-computed snapshot-tabellen via `SnapshotRepository`. Snel, consistent, filtert op `reference_date`.
- **Live fallback**: Als er geen snapshot beschikbaar is, berekent `FaultCountService` de data live uit `vw_root_cause_faults`.

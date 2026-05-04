# Changelog

All notable changes to this project are documented here.  
Format follows [Keep a Changelog](https://keepachangelog.com/en/1.0.0/).

---

## [Unreleased]

---

## [0.6.0] – 2026-04-19

### Added
- Pie chart for PLC fault distribution on the home page
- Database indexes on key columns to speed up queries

### Fixed
- Null-check for `global-loading-overlay` in PDF export flow (button no longer breaks on pages without the overlay)

### Changed
- Subtree toggle logic in `table_tree.html` extracted into a reusable function

---

## [0.5.0] – 2026-04-16 → 2026-04-18

### Added
- Long-term regression reporting: compare fault counts across multiple snapshots over time
- Reference-date support for snapshots: select historical data by week/month on the Diagrams page
- `DiagramPdfService`: renders all six dashboard charts as PNG (Plotly + Kaleido) and bundles them into a landscape PDF via ReportLab — available at `/plc/diagrams-pdf`
- High-level architecture diagram and project structure diagram added to `docs/`
- Enhanced `Database.md`: ER diagrams, ORM mappings, normalized schema explanations, reporting snapshot documentation

### Changed
- `DiagramService` refactored to instance methods with shared repository/service dependencies
- `FaultCountService` relocated to correct module path and imports updated
- Reporting query rewritten to let SQL Server handle aggregation — response time reduced from ~8 s to ~1 s
- Removed obsolete form-related templates, routes, and dead testing code; improved logging throughout

---

## [0.4.0] – 2026-02-23 → 2026-02-25

### Added
- SQL migration validation scripts: interlock chain queries, root fault identification, side-by-side database comparison

### Changed
- `InterlockAnalyzer` renamed to `InterlockService` for consistency with naming conventions
- SQL views enhanced for fault trend analysis
- Repository TVF column handling adjusted

---

## [0.3.0] – 2025-12-19 → 2025-12-20

### Added
- Apache License 2.0 and Contributor Covenant Code of Conduct
- `MailService`: SMTP-based email utility for sending automated reports
- Comprehensive `Database.md` documentation (schema, table definitions, query functions, indexing recommendations)
- Snapshot management: weekly fault snapshots with backfill support across multiple databases

### Changed
- `InterlockRepository` migrated to SQLAlchemy `select` with session context (removed `pandas.read_sql`)
- Snapshot and trend analysis integrated with SQLAlchemy 2.0 patterns

---

## [0.2.0] – 2025-12-04 → 2025-12-15

### Added
- PDF export for the Interlock Tree page using `reportlab`
- `Condition_Mnemonic` field added across interlock processing pipeline (renamed from `Condition_Message`)
- `filter_bit_index` filter in repository, PDF generation, and UI (later removed after evaluation)
- Trend analysis stored procedure and fault snapshot schema
- Weekly snapshot tracking and validation tools
- ML-based fault analysis tools and TensorFlow pipeline (experimental)
- SQL functions for interlock chain traversal with configurable `TopN`

### Fixed
- Date/time filtering failures in interlock queries — `filter_date` parameter removed end-to-end after proving unreliable
- Query failures caused by missing ORDER BY columns in DISTINCT anchor queries
- Repository session handling made consistent with SQLAlchemy 2.0

### Changed
- `InterlockAnalyzer` refactored to follow SOLID principles
- Migrated to SQLAlchemy 2.0 standard (deprecated patterns removed)
- Modularized interlock analysis into separate service, repository, and utility layers
- Default `TopN` tuned; SQL function parameters simplified

---

## [0.1.1] – 2025-11-22 → 2025-11-29

### Added
- Initial normalized database schema and migration script
- `INTERLOCK_NUMBER` column added to `CONDITION_DEFINITION` with unique constraint and index
- `InterlockAnalyzer` class for root cause analysis and interlock chain tracing
- SQLAlchemy ORM models for the normalized schema
- Test scripts for fault-chain SQL functions

### Changed
- Database connection and analysis scripts refactored for modularity

---

## [0.1.0] – 2025-11-05 → 2025-11-16

### Added
- Initial project skeleton with three-tier structure (`presentations/`, `business/`, `data/`)
- Flask application with Blueprint-based routing (`plc` blueprint)
- Plotly-based diagram rendering: bar charts, grouped bar charts, pie charts
- Table visualization page with PLC data, filters, and sorting
- Collapsible table tree view for interlock hierarchies
- PLC repository and service layer
- Session management and user credential handling (IIS user-identification)
- Configuration migrated from YAML to TOML; loaded into Flask app at startup
- Parent-child test data generator for `plc_message` / `plc_message_relation` tables

### Changed
- Blueprint renamed from `audit` to `plc` to match domain language
- Templates and navigation links updated accordingly

---

*Versions before 0.1.0 are not tracked — they correspond to the initial scaffolding commits.*

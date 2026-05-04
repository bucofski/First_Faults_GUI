# First Faults GUI — Project Progress & Goals

**Team:** Benoit Goethals (frontend, visualisations, API) · Tom Van de Vyver (backend, database, queries)  
**Target users:** Control engineers, line supervisors, and maintenance staff at ArcelorMittal who use CIMPLICITY  
**MVP deadline:** 11 June 2026

---

## Vision

Build a Python/Flask web application that turns raw CIMPLICITY (SQL Server) alarm history into actionable insight:
detect recurring faults, surface rising alarms before they become incidents, and produce ready-to-share PDF reports —
all within the IIS security boundary already used on-site.

---

## Timeline

### Phase 1 — Scaffolding & Proof of Concept · Nov 2025

| Date | Milestone |
|------|-----------|
| 2025-11-05 | Repo created, three-tier project structure initialised |
| 2025-11-07 | First Flask app running; Plotly diagrams and table view working |
| 2025-11-08 | Blueprint renamed to `plc`; session management and IIS user-ID in place |
| 2025-11-08 | Collapsible tree table view for interlock hierarchies |
| 2025-11-14 | YAML → TOML config migration; TOML loaded into Flask at startup |
| 2025-11-16 | PLC repository and service layer; parent-child test data generator |

**Outcome:** Working prototype — a Flask app displaying PLC data in a tree table and interactive charts.

---

### Phase 2 — Interlock Engine & DB Schema · Nov – Dec 2025

| Date | Milestone |
|------|-----------|
| 2025-11-22 | Normalised DB schema and migration script |
| 2025-11-28 | SQLAlchemy ORM models; `InterlockAnalyzer` class for root-cause chain tracing |
| 2025-12-04 | Modularised interlock analysis into service/repository/utility layers |
| 2025-12-05 | SQL functions for chain traversal; configurable `TopN`; recursive upstream/downstream logic |
| 2025-12-10 | Experimental ML pipeline for fault prediction (TensorFlow) |
| 2025-12-11 | Trend analysis stored procedure; weekly fault snapshot schema |
| 2025-12-13 | PDF export for Interlock Tree (`reportlab`); `Condition_Mnemonic` field added |
| 2025-12-14 | Migrated repository to SQLAlchemy 2.0; removed deprecated `pandas.read_sql` |
| 2025-12-15 | Fixed date/time and query failures; cleaned up `filter_date` end-to-end |
| 2025-12-19 | `MailService` for SMTP reports; Apache License + Code of Conduct; full DB docs |
| 2025-12-20 | Snapshot management with backfill support across multiple databases |

**Outcome:** Solid backend — interlock chains traced recursively in SQL, snapshots stored and queryable, PDFs generated.

---

### Phase 3 — Stability & Rename · Feb 2026

| Date | Milestone |
|------|-----------|
| 2026-02-23 | `InterlockAnalyzer` → `InterlockService` (naming aligned with service layer convention) |
| 2026-02-23 | SQL views enhanced for fault trend analysis |
| 2026-02-25 | Migration validation scripts; side-by-side DB comparison tooling |

**Outcome:** Codebase cleaned up; interlock service ready for production-style testing.

---

### Phase 4 — Reporting Dashboard & Performance · Apr 2026

| Date | Milestone |
|------|-----------|
| 2026-04-16 | SQL and tree-builder fixes; reporting tools added |
| 2026-04-17 | Long-term regression reporting; snapshot backfill across two production DBs |
| 2026-04-18 | `DiagramService` refactored to instance methods |
| 2026-04-18 | `FaultCountService` relocated and imports fixed |
| 2026-04-18 | Reference-date picker on Diagrams page (select week/month from history) |
| 2026-04-18 | `DiagramPdfService`: six charts → PNG → landscape PDF at `/plc/diagrams-pdf` |
| 2026-04-18 | Architecture and structure diagrams added to `docs/`; `Database.md` fully documented |
| 2026-04-18 | Reporting query rewritten — response time dropped from ~8 s to ~1 s |
| 2026-04-19 | DB indexes added on key columns |
| 2026-04-19 | Pie chart for PLC fault distribution added to home page |
| 2026-04-19 | PDF export null-check bug fixed; subtree toggle refactored |

**Outcome:** Full reporting dashboard live with PDF export, historical snapshots, and sub-second query times.

---

## Current State (May 2026)

| Area | Status |
|------|--------|
| Flask app / routing | Stable |
| Interlock chain analysis | Working, tested |
| Diagrams dashboard (6 charts + heatmap) | Working |
| PDF export — diagrams | Working |
| PDF export — interlock tree | Working |
| Long-term regression reports | Working |
| Snapshot / backfill system | Working |
| MailService | Implemented, not yet wired to scheduled jobs |
| ML fault prediction | Experimental — not in production |
| IIS deployment | Pending final integration test |

---

## Goals

### Achieved
- [x] DB connection via SQLAlchemy + pyodbc (SQL Server)
- [x] Interlock chain tracing with recursive SQL function
- [x] Fault trend and snapshot tracking (weekly)
- [x] Diagrams page: faults/hour, faults/PLC (pie), top risers, MTBF, top climbing faults, repeat offenders, heatmap
- [x] Collapsible interlock tree table with server-side filters
- [x] PDF export for both the tree page and the diagrams page
- [x] Reference-date selection for historical snapshot comparison
- [x] Sub-second reporting queries (index + query rewrite)
- [x] Architecture and database documentation

### In Progress / Short-term (before MVP — 11 June 2026)
- [ ] Schedule daily snapshot runs via Windows Task Scheduler / IIS
- [ ] Wire `MailService` to automated weekly PDF email delivery
- [ ] Final IIS deployment and user-acceptance test with ArcelorMittal engineers
- [ ] About & Contact pages finalised with real contact information
- [ ] End-to-end test pass: all pages, all filters, PDF downloads

### Medium-term (post-MVP)
- [ ] Role-based access: read-only view vs. admin configuration
- [ ] Configurable alert thresholds (e.g., notify when fault count rises > X% week-over-week)
- [ ] Extended heatmap: drill-down from PLC level to individual fault codes
- [ ] Mobile-friendly responsive layout
- [ ] Promote ML fault prediction from experimental to production feature

### Long-term / Nice-to-have
- [ ] Real-time alarm feed (WebSocket or polling) alongside historical view
- [ ] Multi-site support (multiple CIMPLICITY instances / plants)
- [ ] REST API so other tools can query fault trends programmatically
- [ ] Automated regression detection with email/Teams notification

---

## Key Technical Decisions

| Decision | Rationale |
|----------|-----------|
| Flask over FastAPI | Simpler template rendering with Jinja2; team already familiar with it |
| SQL Server TVF for interlock chains | Recursive chain logic is complex — keeping it in SQL lets the DB engine optimise it |
| Plotly server-side → PNG for PDF | Avoids headless-browser dependency; Kaleido is lightweight and reliable |
| ReportLab for PDF | Full control over layout; no external service needed |
| Snapshots stored weekly | Balances storage cost vs. historical resolution for trend analysis |
| IIS authentication | No custom login required; leverages existing site security infrastructure |

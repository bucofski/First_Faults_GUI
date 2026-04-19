# Projectverloop

Overzicht van het verloop van het project, gebaseerd op de Git-geschiedenis. Totaal: ~162 commits door twee teamleden.

## Teamverdeling

| Wie | Rol | Commits |
|-----|-----|---------|
| **Benoit** (benoit / Benoit Goethals) | Frontend, UI, templates, routing, PDF | ~60 |
| **Tom** (TVDV / bucofski) | Backend, database, SQL, analyse, rapportage | ~102 |

## Gantt Chart

![Gantt Chart](gantt.png)

<details>
<summary>Mermaid broncode</summary>

```mermaid
gantt
    title Projectplanning First Faults GUI
    dateFormat  YYYY-MM-DD
    axisFormat  %b %Y

    section Fase 1 – Opzet
    Repo & gitignore (Tom)                :done, t1, 2025-11-05, 2025-11-06
    DB connectie & YAML config (Tom)      :done, t2, 2025-11-07, 2025-11-14
    SQL interlock queries (Tom)           :done, t3, 2025-11-14, 2025-11-30
    ORM modellen (Tom)                    :done, t4, 2025-11-22, 2025-11-30
    3-tier structuur (Benoit)             :done, b1, 2025-11-06, 2025-11-07
    Flask app & routes (Benoit)           :done, b2, 2025-11-07, 2025-11-08
    Plotly diagrammen (Benoit)            :done, b3, 2025-11-07, 2025-11-08
    Table tree view (Benoit)              :done, b4, 2025-11-08, 2025-11-15
    Blueprint refactor (Benoit)           :done, b5, 2025-11-07, 2025-11-08
    Sessie-management (Benoit)            :done, b6, 2025-11-08, 2025-11-09
    PLC service & repo (Benoit)           :done, b7, 2025-11-15, 2025-11-16

    section Fase 2 – Functionaliteit
    Interlock chain analyse (Tom)         :done, t5, 2025-12-04, 2025-12-08
    SOLID refactor analyzer (Tom)         :done, t6, 2025-11-28, 2025-12-07
    SQLAlchemy 2.0 migratie (Tom)         :done, t7, 2025-11-28, 2025-12-05
    DB schema normalisatie (Tom)          :done, t8, 2025-11-22, 2025-12-11
    Condition Mnemonic (Tom)              :done, t9, 2025-12-11, 2025-12-13
    Trend analyse & snapshots (Tom)       :done, t10, 2025-12-10, 2025-12-20
    ML fault prediction (Tom)             :done, t11, 2025-12-10, 2025-12-11
    Table tree filters (Benoit)           :done, b8, 2025-12-07, 2025-12-13
    Loading spinner (Benoit)              :done, b9, 2025-12-07, 2025-12-08
    PDF export (Benoit)                   :done, b10, 2025-12-13, 2025-12-14
    README & docs (Benoit)                :done, b11, 2025-12-08, 2025-12-09
    Licentie & CoC (Benoit)               :done, b12, 2025-12-19, 2025-12-20
    Mail service (Benoit)                 :done, b13, 2025-12-19, 2025-12-20
    Bugfixes SQL & filtering (Tom)        :done, t12, 2025-12-12, 2025-12-20

    section Fase 3 – Validatie
    Migratie validatie scripts (Tom)      :done, t13, 2026-02-23, 2026-02-25
    Root fault identificatie (Tom)        :done, t14, 2026-02-25, 2026-02-26
    Bugfix conditie-zoek (Benoit)         :done, b14, 2026-02-23, 2026-02-24

    section Fase 4 – Rapportage
    Reporting snapshots (Tom)             :done, t15, 2026-04-16, 2026-04-17
    Long-term trends (Tom)                :done, t16, 2026-04-17, 2026-04-18
    Backfill 2 databases (Tom)            :done, t17, 2026-04-17, 2026-04-18
    Dashboard redesign (Benoit)           :done, b15, 2026-04-17, 2026-04-18
    Maand/week selectie (Benoit)          :done, b16, 2026-04-17, 2026-04-18
    reference_date doorvoer (Benoit)      :done, b17, 2026-04-17, 2026-04-18
    Globale spinner (Benoit)              :done, b18, 2026-04-17, 2026-04-18
    Table tree restyle (Benoit)           :done, b19, 2026-04-18, 2026-04-18
    Documentatie (Benoit)                 :done, b20, 2026-04-18, 2026-04-18

    section Milestones
    Vooruitgangsmoment                    :milestone, m1, 2026-02-26, 0d
    MVP deadline                          :milestone, m2, 2026-06-11, 0d
```

</details>

## Tijdlijn per fase

### Fase 1: Opzet & Basis (november 2025)

**59 commits**

#### Tom
- Repository aangemaakt met `.gitignore` en basisconfiguratie
- Eerste databaseconnectie opgezet (YAML config, pyodbc)
- SQL-functies voor interlock fault-chain analyse geschreven
- SQLAlchemy ORM-modellen gedefinieerd
- Testscripts voor DB-connectie

#### Benoit
- 3-tier projectstructuur opgezet (`presentations/`, `business/`, `data/`)
- Flask app geinitialiseerd met routes en templates
- Eerste diagrammen met Plotly (bar chart, pie chart)
- Tabel- en table-tree views aangemaakt
- Blueprint-structuur ingevoerd (rename `audit` -> `plc`)
- Sessie-management en user credentials toegevoegd
- PLC service en repository laag opgezet

**Doelen bereikt:**
- Werkende Flask applicatie met navigatie
- Eerste grafieken zichtbaar
- Databaseconnectie operationeel
- Basisstructuur voor interlock-analyse

---

### Fase 2: Functionaliteit & Bugfixes (december 2025)

**94 commits**

#### Tom
- Interlock chain analyse uitgebreid met recursieve upstream/downstream logica
- `InterlockAnalyzer` gerefactord naar SOLID principes, daarna hernoemd naar `InterlockService`
- SQLAlchemy 2.0 migratie doorgevoerd
- SQL-functies voor filtering (timestamp, PLC, Top N) verbeterd
- Database schema genormaliseerd + migratiescripts
- `Condition_Mnemonic` ondersteuning toegevoegd
- Fault trend analyse en snapshot schema ontwikkeld
- ML-gebaseerde fault prediction tools toegevoegd (TensorFlow)
- Diverse bugfixes op SQL queries en filtering

#### Benoit
- Table tree template volledig opgebouwd met filters (BSID, PLC, tijdsperiode, conditiebericht)
- Loading spinner toegevoegd bij form submits
- PDF-export geimplementeerd met ReportLab
- `filter_bit_index` toegevoegd en later verwijderd (iteratief)
- `filter_date` support verwijderd na herontwerp
- README uitgebreid met architectuurdiagram, rollen, licentie
- Code of Conduct en Apache License toegevoegd
- MailService utility gebouwd
- Diverse template refactors voor consistentie

**Doelen bereikt:**
- Volledige interlock-boomstructuur met in-/uitklappen
- PDF-export werkend
- Filterfunctionaliteit compleet
- Database genormaliseerd
- Trend analyse basis gelegd

---

### Fase 3: Validatie & Migratie (februari 2026)

**3 commits**

#### Tom
- SQL migratie validatie- en testscripts toegevoegd
- Interlock chain queries en root fault identificatie
- Side-by-side database vergelijkingslogica
- Repository TVF kolom handling aangepast

#### Benoit
- Bugfix op conditie-zoekfunctie (PR #45)

**Doelen bereikt:**
- Migratievalidatie naar nieuwe databasestructuur
- Stabielere queries

---

### Fase 4: Rapportage & Dashboard (april 2026)

**8 commits**

#### Tom
- Reporting tools toegevoegd (snapshots, backfill)
- Long-term trend rapportage (regressie-analyse)
- Repeat offenders, MTBF, top risers snapshots
- Twee databases configureerbaar voor backfill

#### Benoit
- Diagrams pagina herontworpen met Bootstrap grid (2-koloms layout)
- Maand/week selectieboxen met berekening eerste maandag
- `reference_date` doorgevoerd door alle lagen (route -> service -> repository)
- Globale loading spinner in `base.html` (werkt bij navigatie + form submits)
- Table Tree pagina gerestyled met Bootstrap (cards, responsive table, dark header)
- Status kolom verwijderd uit table tree
- Documentatie geschreven (diagrams, table tree, README update)

**Doelen bereikt:**
- Dashboard met 6 grafieken + heatmap
- Historische data opvraagbaar via maand/week selectie
- Consistente look & feel over alle pagina's
- Globale spinner voor betere gebruikerservaring
- Projectdocumentatie compleet

---

## Bereikte doelen (samenvatting)

| Doel | Status | Verantwoordelijke |
|------|--------|-------------------|
| Flask applicatie met routing | Bereikt | Benoit |
| 3-tier architectuur | Bereikt | Benoit |
| Database connectie (SQL Server) | Bereikt | Tom |
| Interlock chain analyse | Bereikt | Tom |
| Boomstructuur UI met filters | Bereikt | Benoit + Tom |
| PDF-export | Bereikt | Benoit |
| Diagrammen (Plotly) | Bereikt | Benoit |
| Snapshot/rapportage systeem | Bereikt | Tom |
| Trend analyse (risers, climbers) | Bereikt | Tom |
| MTBF berekening | Bereikt | Tom |
| Heatmap per PLC | Bereikt | Benoit + Tom |
| Historische data filtering (maand/week) | Bereikt | Benoit |
| Globale loading spinner | Bereikt | Benoit |
| Database migratie & validatie | Bereikt | Tom |
| ML fault prediction (experimenteel) | Bereikt | Tom |
| Sessie-management | Bereikt | Benoit |
| Mail service | Bereikt | Benoit |
| Documentatie | Bereikt | Benoit |

## Branches

- `Develop` — hoofdbranch voor development
- `feature/reporting` — rapportage en snapshot functionaliteit
- `feature/DB` — database schema en connectie
- Diverse `Bugfix/*` branches via pull requests
- Feature branches per taak (bv. `feature/benoit/pdf`, `feature/benoit/fiilter_bit`)

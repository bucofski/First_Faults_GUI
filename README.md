# Projectvoorstel: Historische Eerste Foutanalyse UI

## 1. Team

- **Benoit** – Frontend (UI & visualisaties, API)
- **Tom** – Backend (databaseconnectie, queries, rapportage)

**Deadline:** 11 juni (MVP), tussentijdse vooruitgang: 26 februari  
**Rapportage:** Maandelijks

## 2. Projectomschrijving

Ontwikkeling van een Python-gebaseerde webapplicatie die historische alarmen uit CIMPLICITY (SQL Server) visualiseert en analyseert.

**Doel:** Trends detecteren (stijging van dezelfde alarmen binnen een tijdsperiode), top-10 alarmen per PLC, en rapportage per dag/week/maand.

## 3. Marktonderzoek

### Vergelijkbare software:
- **CIMPLICITY Alarm Viewer** – beperkt tot real-time, geen historische analyse
- **Proficy Operations Hub** – dashboarding, maar niet flexibel voor vrije queries
- **Historian Client** – sterk, maar niet gericht op alarmtrends per PLC

### Doelpubliek:
Regeltechniekers, lijnverantwoordelijken, productiebedienden bij Arcelor die CIMPLICITY gebruiken of personeel die het onderhoud op de site doet om vroegtijdige problemen op te sporen.

### Uniek punt:
- Vrije query's op historische alarmdata (in progress)
- Trendanalyse (stijgingen per PLC)
- Automatische PDF-rapporten
- Gebruiksvriendelijke UI binnen IIS met user-identificatie

## 4. Technologiekeuzes

- **Backend:** Flask (Python)
- **Database:** SQL Server via SQLAlchemy + pyodbc
- **Charts:** Plotly (interactieve grafieken, server-side gerenderd als HTML)
- **Rapporten:** PDF-export vanuit de Table Tree pagina
- **Run-omgeving:** IIS (Windows), security via IIS user-identificatie
- **Frontend:** Jinja2 templates, Bootstrap 5

## 5. Architectuur

### High-level Architectuur
![High-level Architecture](docs/HighLevelArchitectureDiagram.png)

### Lagenstructuur

```
presentations/          UI-laag (Flask app, blueprints, templates, services)
  app.py                create_app() — registreert blueprints, config, errorhandlers
  routes/               Blueprints: plc_routes, auth_routes, auth_google, auth_corporate
  templates/            Jinja2 HTML-templates
  services/             View-services (DiagramService, DiagramPdfService, PdfGenerator)
                        + auth-helpers (totp_service, user_store)

business/               Business-laag
  services/             InterlockService (analyzer)
  core/                 FaultCountService, InterlockTreeBuilder

data/                   Data-laag
  repositories/         InterlockRepository, SnapshotRepository, DB_Connection
  model/                Domain models (dataclasses)
  orm/                  SQLAlchemy ORM-modellen (reporting_orm)
```

### Flow:
```
SQL Server -> Repository -> Business Service -> Presentation Service -> Template -> Browser
```

## 6. Pagina's

### Home (`/plc/`)
Landingspagina.

### Diagrams (`/plc/diagrams`)
Dashboard met zes grafieken en een heatmap.

- **Selectie:** Maand + week-van-de-maand dropdowns. De eerste maandag van die week wordt berekend en als `reference_date` doorgegeven om historische snapshots te filteren.
- **Grafieken:** Faults per hour, Faults per PLC (pie), Top risers, MTBF per PLC, Top 10 climbing faults, Repeat offenders.
- **Heatmap:** Per PLC, selecteerbaar via aparte dropdown.
- **PDF export:** Alle grafieken worden via `DiagramPdfService` gerenderd als PNG (Plotly + Kaleido) en samengevoegd in een landscape PDF (ReportLab). Route: `/plc/diagrams-pdf`.
- **Spinner:** Globale loading overlay (uit `base.html`) bij navigatie en form submits.

Zie [docs/diagrams_page.md](docs/diagrams_page.md) voor gedetailleerde documentatie.

### Table Tree (`/plc/table-tree`)
Interlock-boomstructuur met filters en PDF-export.

- **Filters:** Target BSID, Top N, PLC, tijdsperiode, conditiebericht.
- **PRG-patroon:** POST valideert en redirect naar GET met query params.
- **Boom:** Recursieve Jinja2 macro met in-/uitklapbare rijen via JavaScript.
- **PDF:** Async download via `fetch`, zonder paginanavigatie.
- **Validatie:** Server-side parsing van integers en ISO datetimes met flash-meldingen bij fouten.

Zie [docs/table_tree_page.md](docs/table_tree_page.md) voor gedetailleerde documentatie.

### About (`/plc/about`) & Contact (`/plc/contact`)
Informatieve pagina's.

## 7. Globale Spinner

Gedefinieerd in `base.html`, beschikbaar op alle pagina's:
- Wordt geactiveerd bij klik op navigatielinks en bij elke form submit.
- Toont een overlay met draaiende spinner terwijl de server data laadt.

## 8. Key Queries

- Fouten per PLC op 24u
- Alarmen die stijgen per dag/week
- Top-10 alarmen per PLC (week/maand)
- Grafieken per PLC (trend, Pareto)
- MTBF per PLC
- Repeat offenders (max herhalingen per uur)
- Heatmap (uur x datum per PLC)

## 9. Planning

- **Week 1-5:** Analyse + DB-connectie & basisqueries + eerste UI (Tom, Benoit)
- **Week 5-8:** Agile iteratie op andere usecases
- **Week 9-14:** Rapportage & integratie

**Vooruitgangsmoment:** 26 feb -> werkende query + eerste grafiek  
**MVP:** 11 juni -> volledige flow + rapportage

## 10. Authenticatie

De applicatie ondersteunt **twee inlogmethodes** die naast elkaar staan op `/auth/login`:

1. **Continue with Google** — Google OAuth 2.0 (internet) + TOTP 2FA (Google Authenticator / Authy).
2. **Continue with Corporate Account** — in-house OAuth2-server (`auth_server/oauth2_server.py`, lokaal netwerk),
   géén internet, géén TOTP. De lokale server beheert corporate-gebruikers.

Elke provider is een aparte Flask Blueprint (`auth_google.py`, `auth_corporate.py`).
Een derde provider toevoegen = één nieuw bestand + één regel in `app.py` + één knop in `login.html`.

Zie [docs/oauth-totp-flow.md](docs/oauth-totp-flow.md) voor het volledige stroomdiagram, sessiestaten,
en instructies om een extra provider toe te voegen.

## 11. Runnen

### Vereiste omgevingsvariabelen (in `set_env.sh`)

```bash
export GOOGLE_CLIENT_ID="YOUR_REAL_ID.apps.googleusercontent.com"
export GOOGLE_CLIENT_SECRET="YOUR_REAL_SECRET"
export FLASK_SECRET_KEY="any-random-string-is-fine-for-dev"
export FLASK_RUN_HOST="localhost"     # moet matchen met de redirect URI in Google Cloud Console
export FLASK_RUN_PORT="5001"
```

Optioneel voor de corporate flow (defaults werken voor lokale ontwikkeling):
```bash
export CORPORATE_OAUTH2_SERVER="http://localhost:5500"
export CORPORATE_OAUTH2_CLIENT_ID="demo-client"
export CORPORATE_OAUTH2_CLIENT_SECRET="demo-secret-123"
```

### Hoofdapplicatie

```bash
./run.sh
```

Het `run.sh` script sourced `set_env.sh` en start `flask run` op `localhost:5001`,
zodat de Google redirect URI matcht met wat in Google Cloud Console geregistreerd is
(`http://localhost:5001/auth/callback`).

### Corporate authenticatieserver (alleen voor de corporate login)

In een tweede terminal:
```bash
.venv/bin/python auth_server/oauth2_server.py
```

De server draait op `http://localhost:5500`. Als hij niet bereikbaar is en je klikt
op "Continue with Corporate Account", krijg je een nette 503-foutpagina ("OAuth Server
Not Available") in plaats van een browserfout.

Demo-accounts in de corporate server: `benoit` / `tom` (zie `USERS` in `oauth2_server.py`).

## 12. Documentatie

- [Diagrams pagina](docs/diagrams_page.md) – opbouw, spinner, selectieboxen, dataflow
- [Table Tree pagina](docs/table_tree_page.md) – flow, boomstructuur, JavaScript, validatie
- [OAuth + TOTP flow](docs/oauth-totp-flow.md) – Google + corporate login, sessiestaten, hoe een derde provider toevoegen
- [Projectverloop](docs/project_verloop.md) – tijdlijn, Gantt chart, bereikte doelen per fase
- [Presentatie NL](docs/FirstFaults_Presentatie_NL.pptx) – projectpresentatie in het Nederlands
- [Presentation EN](docs/FirstFaults_Presentation.pptx) – project presentation in English

## Structure
![Project Structure](docs/ProjectStructureDiagram.png)

## Licence

Copyright (c) 2025 Tom Van de Vyver / Goethals Benoit

This source code is provided for viewing purposes only.

You may NOT:
- Use this code in any project
- Copy, modify, or distribute this code
- Use this code for commercial or non-commercial purposes

All rights reserved.

# Diagrams Page

## Overzicht

De Diagrams-pagina toont een reeks grafieken over foutmeldingen (faults) van PLC's. De pagina is bereikbaar via het navigatiemenu ("Diagrams") en wordt volledig server-side gerenderd door Flask.

## Loading Spinner

Omdat alle diagramdata server-side wordt opgehaald voordat de HTML naar de browser wordt gestuurd, kan het laden even duren. De spinner lost dit op:

- De spinner zit in `base.html` (globaal voor alle pagina's).
- Bij het klikken op een navigatielink (bv. Home -> Diagrams) wordt de spinner **onmiddellijk** getoond op de huidige pagina, nog voordat de server begint te antwoorden.
- Bij elke form-submit (Apply, Show heatmap, Refresh) wordt de spinner ook getoond.
- Zodra de nieuwe pagina volledig is geladen in de browser, verdwijnt de spinner automatisch (de oude pagina wordt vervangen).

### Technisch

De spinner wordt geactiveerd via JavaScript event listeners in `base.html`:

- `a.nav-link` / `a.navbar-brand` click -> spinner actief
- `form submit` event (globaal) -> spinner actief

## Opbouw van de pagina

### 1. Top bar
- Titel "Diagrams" links
- Refresh-knop rechts (herlaadt de pagina)

### 2. Selectieformulier (maand / week)
Een Bootstrap card met:
- **Month** dropdown: alle 12 maanden (January - December)
- **Week of month** dropdown: Week 1 t/m Week 5
- **Apply** knop: verstuurt het formulier als GET-request
- **Badge**: toont de berekende startdatum (eerste maandag van de geselecteerde week)

### 3. Grafieken (2-koloms grid)
Zes grafieken in drie rijen van twee:

| Links | Rechts |
|-------|--------|
| Faults per hour (staafdiagram) | Faults per PLC (taartdiagram) |
| Top risers (horizontaal staafdiagram) | MTBF per PLC (horizontaal staafdiagram) |
| Top 10 climbing faults (lijndiagram) | Repeat offenders (horizontaal staafdiagram) |

Elke grafiek zit in een Bootstrap `card`.

### 4. Heatmap (volledige breedte)
- Aparte PLC-selector dropdown
- Toont een heatmap (uur x datum) voor de gekozen PLC
- De maand/week selectie wordt meegegeven als hidden fields zodat die niet verloren gaat

## Gebruik van de selectieboxen

### Maand + Week selectie

1. Kies een **maand** en een **week** (1-5) in de dropdowns.
2. Klik op **Apply**.
3. De route `/plc/diagrams?month=4&week=2` wordt aangeroepen.
4. In de Flask route (`plc.py`) wordt de **eerste maandag** van die week in die maand berekend via `_first_monday_of_month_week()`.
5. Deze datum wordt als `reference_date` doorgegeven aan alle diagram-service methodes.
6. De service geeft het door aan de repository, die snapshots filtert op `snapshot_date <= reference_date`.
7. Zo kun je historische data bekijken door een maand/week in het verleden te selecteren.

### PLC selectie (heatmap)

1. Kies een PLC in de dropdown onderaan de pagina.
2. Klik op **Show heatmap**.
3. De geselecteerde maand/week wordt via hidden fields meegestuurd, zodat die niet reset.
4. De heatmap wordt getoond voor de gekozen PLC.

## Dataflow

```
Browser (GET /plc/diagrams?month=X&week=Y&plc=Z)
    |
    v
Flask route (plc.py: diagrams)
    |-- berekent selected_date = eerste maandag van maand/week
    |-- roept DiagramService methodes aan met reference_date
    |       |
    |       v
    |   DiagramService (diagram_service_view.py)
    |       |-- roept SnapshotRepository aan met reference_date
    |       |       |
    |       |       v
    |       |   SnapshotRepository: WHERE snapshot_date <= reference_date
    |       |
    |       |-- genereert Plotly HTML
    |
    v
Template (diagrams.html) rendert alle chart HTML
    |
    v
Browser toont pagina, spinner verdwijnt
```

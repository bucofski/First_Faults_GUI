# Table Tree Page

## Overzicht

De Table Tree pagina toont interlock-gegevens in een boomstructuur. Gebruikers kunnen filteren op BSID, PLC, tijdsperiode en conditieberichten. Resultaten worden weergegeven als een uitklapbare boom in een tabel.

## Flow

### POST-GET-Redirect patroon

De pagina gebruikt het PRG (Post-Redirect-Get) patroon om dubbele submits te voorkomen:

```
1. Gebruiker vult filters in en klikt "Load"
2. Browser stuurt POST naar /plc/table-tree
3. Flask valideert de filters (plc.py: _parse_table_tree_filters_or_redirect)
   - Bij validatiefout: flash error + redirect terug
   - Bij succes: redirect naar GET /plc/table-tree?target_bsid=X&top_n=Y&...
4. GET-request leest parameters uit query string
5. Flask roept service_interlock.analyze_interlock() aan
6. Template rendert de boomtabel
```

### Dataflow

```
Browser (POST form data)
    |
    v
Flask route: table_tree (POST)
    |-- _read_table_tree_form_params()    -> leest form fields
    |-- _parse_table_tree_filters_or_redirect()
    |       |-- _parse_optional_int()     -> target_bsid, top_n
    |       |-- _parse_iso_datetime()     -> start/end timestamps
    |       |-- Bij fout: flash() + redirect
    |
    v
Redirect naar GET met query params
    |
    v
Flask route: table_tree (GET)
    |-- Leest query params
    |-- InterlockService.analyze_interlock(**kwargs)
    |       |
    |       v
    |   Database query -> boomstructuur opgebouwd
    |
    v
Template: table_tree.html rendert boom
```

## Opbouw van de pagina

### 1. Top bar
- Titel "Interlock Tree" links
- Refresh-knop rechts

### 2. Foutmeldingen
- Bootstrap `alert-danger` blokken met dismiss-knop
- Worden getoond bij validatiefouten (bv. ongeldige BSID, fout datumformaat)

### 3. Filterformulier
Een Bootstrap card met twee rijen:

**Rij 1** (5 kolommen):
| Veld | Type | Beschrijving |
|------|------|-------------|
| Target BSID | number | Specifieke BSID opzoeken |
| Top N | number | Maximum aantal resultaten |
| PLC Filter | text | Filteren op PLC-naam |
| Start Time | datetime-local | Begin van tijdsperiode |
| End Time | datetime-local | Einde van tijdsperiode |

**Rij 2**:
| Veld | Type | Beschrijving |
|------|------|-------------|
| Condition Message | text | Zoeken in conditieberichten |

**Knoppen**:
- **Load** (primary): Verstuurt het formulier (POST)
- **PDF** (outline-success): Exporteert resultaten als PDF via fetch
- **Reset** (outline-secondary): Wist alle velden en herlaadt de pagina zonder params

### 4. Resultaattabel
- Omhuld door een Bootstrap `card` met `table-responsive`
- Donkere header (`table-dark`)
- Kolommen: Interlock Message, BSID, PLC, Direction, Timestamp, Conditions
- Boomstructuur met inspringende niveaus (padding-left per depth)

### 5. Lege states
- Als BSID ingevuld maar geen resultaten: "No interlock data found for BSID X."
- Als niets ingevuld: "Enter a Target BSID to load interlock data."

## Boomstructuur (Tree)

### Jinja2 macro: `render_node`

De boom wordt recursief gerenderd met een Jinja2 macro:

```
render_node(node, parent_id, depth=0)
```

- **depth=0**: Root-rij, altijd zichtbaar, CSS-klasse `parent`
- **depth>0**: Kind-rij, begint verborgen (`display:none`), klasse `child child-of-{parent_id}`
- Elke rij krijgt een `data-node-id` attribuut (combinatie van `interlock_log_id` en `depth`)
- Inspringen: `padding-left: 20 + depth * 30 px`
- Nodes met kinderen tonen een toggle-pijltje (▶), bladeren tonen een bolletje (•)
- Kind-rijen krijgen een blauwe linkerborder (`border-left: 3px solid #007bff`)

### Conditions kolom
- Lijst van condities per node
- Elke conditie toont: **bit {index}:** {message}
- Zonder condities wordt een streepje (-) getoond

## JavaScript

### 1. Form submit (Load-knop)
```javascript
form.addEventListener("submit", function () {
    loadBtn.disabled = true;
});
```
- Schakelt de Load-knop uit om dubbele submits te voorkomen
- De globale spinner uit `base.html` wordt automatisch geactiveerd (via het globale `submit` event)

### 2. PDF export
```javascript
pdfBtn.addEventListener("click", async function () { ... });
```
- Toont de globale spinner
- Stuurt de formulierdata via `fetch` als POST naar `/plc/pdf-table_tree_export-tree`
- Bij succes: maakt een Blob URL, creëert een onzichtbare `<a>` tag, klikt erop om de download te starten
- Probeert de bestandsnaam uit de `Content-Disposition` header te lezen
- Bij fout: toont een alert
- In `finally`: verbergt spinner, schakelt knoppen weer in

### 3. Reset-knop
```javascript
document.getElementById("reset-btn").addEventListener("click", function () {
    form.reset();
    window.location.href = "{{ url_for('plc.table_tree') }}";
});
```
- Reset het formulier
- Navigeert naar de pagina zonder query parameters (schone staat)

### 4. Toggle boom (in-/uitklappen)
```javascript
document.addEventListener("click", function (e) {
    const t = e.target.closest(".toggle");
    if (!t) return;
    ...
});
```
- Gebruikt event delegation op `document` (werkt voor alle toggle-pijltjes)
- Zoekt alle kind-rijen via CSS selector `.child-of-{id}`
- Klapt in: verbergt kind-rijen + klapt geneste kinderen recursief in
- Klapt uit: toont directe kind-rijen
- Wisselt het pijltje: ▶ (dicht) ↔ ▼ (open)

## Validatie

### Server-side (Flask route)

**`_parse_optional_int(value, field_label, error_message)`**
- Probeert een string naar int te converteren
- Lege string → `None` (optioneel veld)
- Ongeldige waarde → `flash(error_message, "error")`

**`_parse_iso_datetime(value, field_name)`**
- Parst ISO datetime strings (bv. `2026-04-17T08:30`)
- Lege/None → `None`
- Ongeldig formaat → `ValueError` exception, gevangen in de route en geflashed als error

**Validatieflow:**
1. `_read_table_tree_form_params()` leest alle velden uit `request.form`
2. `_parse_table_tree_filters_or_redirect()` valideert elk veld
3. Bij fout: `flash()` + `redirect()` terug naar de pagina (errors worden getoond als alerts)
4. Bij succes: redirect naar GET met geparste parameters in de query string

### Client-side
- HTML `type="number"` voorkomt niet-numerieke invoer voor BSID en Top N
- HTML `type="datetime-local"` biedt een browser-native datumkiezer
- Geen verdere JavaScript-validatie; de server is de bron van waarheid

## Spinner

De pagina gebruikt de **globale spinner** uit `base.html`:
- Automatisch bij form submit (Load-knop) via het globale submit event
- Handmatig geactiveerd bij PDF export (`overlay.classList.add("active")`)
- Handmatig gedeactiveerd na PDF download/fout in het `finally` blok

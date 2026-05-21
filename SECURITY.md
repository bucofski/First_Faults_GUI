# Security

Dit document beschrijft hoe authenticatie en autorisatie werken in de First Faults GUI applicatie.

## Overzicht

De applicatie gebruikt een op Flask gebaseerde sessie-authenticatie met de volgende lagen:

```
Browser → Login (CSRF check) → Wachtwoordverificatie → Sessie → Beschermde routes
```

## Authenticatie (A01 / A07)

### Gebruikersopslag

Gebruikers worden geladen vanuit een van deze twee bronnen (in volgorde van prioriteit):

| Omgevingsvariabele | Beschrijving |
|--------------------|--------------|
| `AUTH_USERS_JSON`  | JSON-blob direct als string |
| `AUTH_USERS_FILE`  | Pad naar een JSON-bestand |

Als geen van beide is ingesteld, kan **niemand inloggen**. Elke beschermde route stuurt dan door naar `/auth/login`.

### Wachtwoordhashing

Wachtwoorden worden nooit in plaintext opgeslagen. De applicatie gebruikt **Werkzeug's `scrypt`-gebaseerde hashing** (`scrypt:32768:8:1$...`). Verificatie gebeurt via `check_password_hash()`.

Nieuw wachtwoordhash genereren:

```bash
python scripts/hash_password.py <gebruikersnaam> <rol>
```

Het script vraagt het wachtwoord interactief (niet zichtbaar), vraagt om bevestiging, en drukt een JSON-fragment af dat je in `users.json` kunt plakken.

### Rollen

Gedefinieerd in `presentations/services/creadential.py`:

| Rol       | Beschrijving                     |
|-----------|----------------------------------|
| `admin`   | Volledige toegang                |
| `user`    | Standaard gebruiker              |
| `guest`   | Beperkte toegang                 |
| `anonymous` | Niet ingelogd (intern gebruik) |

### Login-flow

1. `GET /auth/login` — toont het loginformulier met een CSRF-token in de sessie.
2. `POST /auth/login` — controleert CSRF-token, daarna gebruikersnaam/wachtwoord.
3. Bij succes: `session.clear()` gevolgd door een nieuwe sessie met `username` en `role`.
4. Redirect naar de oorspronkelijk opgevraagde URL (of `plc.home`).

### Logout

`POST /auth/logout` — wist de volledige sessie en stuurt terug naar de loginpagina. Logout via GET is niet mogelijk (bescherming tegen CSRF-aanvallen via links).

## Autorisatie

### `login_required`

Toegepast op het volledige `plc.*` blueprint via `before_request` in `presentations/app.py`:

```python
plc_routes.bp.before_request(login_required(lambda: None))
```

Niet-ingelogde gebruikers worden doorgestuurd naar `/auth/login?next=<pad>`.

### `role_required(*rollen)`

Decorator voor routes die een specifieke rol vereisen:

```python
@role_required(Role.ADMIN)
def admin_only_view():
    ...
```

Bij onvoldoende rechten: HTTP 403.

## CSRF-bescherming

Het CSRF-token wordt aangemaakt via `os.urandom(32).hex()` en opgeslagen in de Flask-sessie. Bij elk POST-verzoek op de loginroute wordt het token uit het formulier vergeleken met het token in de sessie.

Het token is beschikbaar in Jinja2-templates via:

```html
<input type="hidden" name="csrf_token" value="{{ csrf_token() }}">
```

**Alle nieuwe POST-formulieren moeten dit veld bevatten.**

## Sessie-instellingen

- `app.secret_key` staat momenteel op `"dev"` (hardcoded). **Vervang dit in productie:**

```python
app.secret_key = os.environ["FLASK_SECRET_KEY"]
```

Genereer een sterke sleutel:

```bash
python -c "import secrets; print(secrets.token_hex(32))"
```

## Logging

Beveiligingsgebeurtenissen worden gelogd via twee aparte loggers:

| Logger     | Gebeurtenissen                                              |
|------------|-------------------------------------------------------------|
| `auth`     | `LOGIN_SUCCESS`, `LOGIN_FAILURE`, `LOGOUT`                  |
| `security` | `AUTHZ_DENY reason=anonymous`, `AUTHZ_DENY reason=role`     |

Elke log-entry bevat het IP-adres, gebruikersnaam, en het gevraagde pad.

## Wat NIET doen

- `users.json` met echte wachtwoordhashes **niet committen** in git. Gebruik `AUTH_USERS_FILE` buiten de repo of een secret manager.
- De `"dev"` secret key **niet gebruiken** in productie of gedeelde omgevingen.
- CSRF-token **niet weglaten** bij nieuwe POST-formulieren.

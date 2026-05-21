# Authenticatie: Google OAuth 2.0 + TOTP (2FA)

## Overzicht

De applicatie gebruikt een **twee-staps authenticatie**:

1. **Google OAuth 2.0** – de gebruiker logt in via zijn Google-account (SSO).
2. **TOTP (Time-based One-Time Password)** – na Google wordt een 6-cijferige code gevraagd uit een authenticator-app (bijv. Google Authenticator of Authy).

Beide stappen moeten slagen voordat een sessie volledig geactiveerd wordt.

---

## Stroomdiagram

```
┌─────────────────────────────────────────────────────────────────────┐
│                         BROWSER / GEBRUIKER                         │
└───────────────────────────┬─────────────────────────────────────────┘
                            │
                   Bezoekt /auth/login
                            │
                            ▼
┌─────────────────────────────────────────────────────────────────────┐
│                        STAP 1 – GOOGLE OAUTH                        │
└─────────────────────────────────────────────────────────────────────┘
                            │
            Klikt "Continue with Google"
                            │
                            ▼
                     GET /auth/google
                 (genereert state + nonce)
                            │
                            │  redirect
                            ▼
               ┌────────────────────────┐
               │  Google OAuth consent  │
               │  accounts.google.com   │
               └────────────┬───────────┘
                            │
               Gebruiker geeft toestemming
                            │
                            ▼
              GET /auth/callback?code=...
                            │
                 Authlib wisselt code in
                  voor ID-token bij Google
                            │
                  Extraheert uit token:
                  • sub (Google user-ID)
                  • email
                  • name
                            │
                  upsert_user() → data/users.json
                  (aanmaken of bijwerken)
                            │
              session["oauth_pending"] = google_sub
                            │
              ┌─────────────┴──────────────┐
              │                            │
     totp_enrolled == True        totp_enrolled == False
              │                            │
              ▼                            ▼
┌─────────────────────────────────────────────────────────────────────┐
│                        STAP 2A – TOTP VERIFY                        │
│                   (bestaande gebruiker, al ingeschreven)            │
└─────────────────────────────────────────────────────────────────────┘
        GET /auth/totp-verify
        Toont invoerveld voor 6-cijferige code
                 │
         Gebruiker vult code in
                 │
        POST /auth/totp-verify
        verify_totp(secret, code)  ← uit users.json
                 │
        ┌────────┴────────┐
        │                 │
      Fout             Correct
        │                 │
    Toon fout      _complete_login()
                         │
                         ▼
              ┌──────────────────────┐
              │  Sessie geactiveerd  │  ◄───────────────────┐
              │  session["username"] │                      │
              │  session["role"]     │                      │
              │  session["google_sub"]                      │
              └──────────┬───────────┘                      │
                         │                                  │
                  Redirect naar home                        │
                                                            │
┌─────────────────────────────────────────────────────────────────────┐
│                        STAP 2B – TOTP SETUP                         │
│                   (nieuwe gebruiker, eerste keer)                   │
└─────────────────────────────────────────────────────────────────────┘
        GET /auth/totp-setup
        • Genereert willekeurige TOTP-geheime sleutel
        • Toont QR-code om in te scannen
        • Toont ook handmatige invoersleutel
                 │
         Gebruiker scant QR-code
         en vult 6-cijferige code in
                 │
        POST /auth/totp-setup
        verify_totp(generated_secret, code)
                 │
        ┌────────┴────────┐
        │                 │
      Fout             Correct
        │                 │
    Toon fout    enroll_totp() → slaat secret op in users.json
                 totp_enrolled = True
                         │
                  _complete_login() ─────────────────────────►
```

---

## Componentbeschrijving

### Routes (`presentations/routes/auth_routes.py`)

| Route | Methode | Beschrijving |
|---|---|---|
| `/auth/login` | GET | Loginpagina; redirect naar home als al ingelogd |
| `/auth/google` | GET | Start Google OAuth-stroom |
| `/auth/callback` | GET | Ontvangt code van Google, verwerkt token |
| `/auth/totp-setup` | GET / POST | Eerste TOTP-inschrijving met QR-code |
| `/auth/totp-verify` | GET / POST | TOTP-verificatie voor bestaande gebruikers |
| `/auth/logout` | GET | Wist sessie |

### Service (`presentations/services/auth_service.py`)

| Functie | Beschrijving |
|---|---|
| `init_oauth(app)` | Configureert Authlib met Google OpenID Connect |
| `generate_totp_secret()` | Genereert willekeurige base32-sleutel via `pyotp` |
| `get_totp_uri(secret, email)` | Maakt provisioning-URI voor QR-code |
| `verify_totp(secret, code)` | Valideert 6-cijferige code (±30 s tolerantie) |
| `qr_code_base64(uri)` | Geeft base64-gecodeerde PNG terug voor weergave in HTML |

### Gebruikersopslag (`presentations/services/user_store.py`)

Sla op in `data/users.json` (thread-safe met `threading.Lock`):

```json
{
  "google_sub": "1234567890",
  "email": "gebruiker@gmail.com",
  "name": "Jan Jansen",
  "role": "user",
  "totp_secret": "BASE32SECRET...",
  "totp_enrolled": true
}
```

---

## Sessiestaten

```
[Niet ingelogd]
      │
      │  Na succesvolle OAuth-callback
      ▼
[oauth_pending]  →  session["oauth_pending"] = google_sub
      │
      │  Na succesvolle TOTP-verificatie / setup
      ▼
[Volledig ingelogd]  →  session["username"], session["role"], session["google_sub"]
```

De decorator `@login_required` controleert of `session["username"]` aanwezig is. Ontbreekt dat, dan redirect naar `/auth/login`.

---

## Benodigde omgevingsvariabelen

| Variabele | Beschrijving |
|---|---|
| `GOOGLE_CLIENT_ID` | OAuth client-ID van Google Cloud Console |
| `GOOGLE_CLIENT_SECRET` | OAuth client-secret van Google Cloud Console |
| `FLASK_SECRET_KEY` | Geheime sleutel voor Flask-sessies (verplicht in productie) |

De geautoriseerde redirect-URI in Google Cloud Console moet zijn:
```
http://localhost:5001/auth/callback
```

---

## Afhankelijkheden

| Pakket | Gebruik |
|---|---|
| `authlib` | OAuth 2.0 / OpenID Connect client |
| `pyotp` | TOTP-generatie en -verificatie |
| `qrcode[pil]` | QR-code generatie als PNG |
| `flask` | Sessie- en routebeheer |

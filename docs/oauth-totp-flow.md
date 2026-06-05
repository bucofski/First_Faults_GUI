# Authentication — Google OAuth + Corporate OAuth + TOTP

## Overview

The application supports **two independent login paths**, both reachable from `/auth/login`:

1. **Google OAuth 2.0** (over the internet) + **TOTP (2FA)** — Google identity provider.
2. **Corporate OAuth 2.0** (local network only) — talks to the in-house OAuth2 server
   bundled in `auth_server/oauth2_server.py`. No internet, no TOTP — the local server owns
   user management.

Each path is implemented by its own Flask Blueprint; the two never share OAuth code.
A **third provider** can be added by dropping in a new `auth_xxx.py` blueprint —
no changes to existing files are required apart from registering the new blueprint in `app.py`.

---

## File layout

```
presentations/
  routes/
    auth_routes.py        shared: /auth/login, /auth/logout, /auth/totp-*
    auth_google.py        Google OAuth — self-contained: init + /auth/google + /auth/callback
    auth_corporate.py     Corporate OAuth — self-contained: /auth/corporate/start + /auth/corporate/callback
  services/
    totp_service.py       TOTP helpers (secret, QR, verify)
    user_store.py         user persistence (data/users.json)
  templates/auth/
    login.html            two-button login page
    totp_setup.html       QR-code enrollment screen
    totp_verify.html      6-digit code prompt

auth_server/
  oauth2_server.py        the in-house OAuth2 Authorization Server (localhost:5500)
```

---

## URL map

| URL | Owner | Purpose |
|---|---|---|
| `/auth/login` | `auth_routes` | login page with both provider buttons |
| `/auth/logout` | `auth_routes` | clears the session |
| `/auth/google` | `auth_google` | starts Google OAuth flow |
| `/auth/callback` | `auth_google` | receives Google's redirect (URL fixed so Google Console doesn't need re-registering) |
| `/auth/totp-setup` | `auth_routes` | first-time TOTP enrollment (Google flow only) |
| `/auth/totp-verify` | `auth_routes` | TOTP code prompt on subsequent logins (Google flow only) |
| `/auth/corporate/start` | `auth_corporate` | starts Corporate OAuth flow |
| `/auth/corporate/callback` | `auth_corporate` | receives the local OAuth server's redirect |

---

## Flow 1 — Google OAuth + TOTP

```
┌─────────────────────────────────────────────────────────────────────┐
│                        BROWSER / USER                               │
└───────────────────────────┬─────────────────────────────────────────┘
                            │
                  Visits /auth/login,
              clicks "Continue with Google"
                            │
                            ▼
                    GET /auth/google
              (authlib generates state + nonce)
                            │
                            │  302 → accounts.google.com
                            ▼
                ┌────────────────────────┐
                │  Google OAuth consent  │
                │  accounts.google.com   │
                └────────────┬───────────┘
                             │
                  User grants access
                             │
                             ▼
              GET /auth/callback?code=…&state=…
                             │
                  authlib exchanges code for ID-token,
                  extracts: sub, email, name
                             │
                  upsert_user() → data/users.json
                             │
                session["oauth_pending"] = google_sub
                             │
              ┌──────────────┴───────────────┐
              │                              │
     totp_enrolled == True            totp_enrolled == False
              │                              │
              ▼                              ▼
   GET /auth/totp-verify           GET /auth/totp-setup
   prompts 6-digit code            generates secret + QR
              │                              │
   POST /auth/totp-verify          POST /auth/totp-setup
   totp_service.verify(…)          totp_service.verify(…),
              │                    enroll_totp() saves to users.json
              │                              │
              └──────────────┬───────────────┘
                             ▼
                    _complete_login(user)
                  session["username"], …, ["google_sub"]
                             │
                             ▼
                   Redirect → home page
```

---

## Flow 2 — Corporate OAuth (no TOTP)

```
┌─────────────────────────────────────────────────────────────────────┐
│                        BROWSER / USER                               │
└───────────────────────────┬─────────────────────────────────────────┘
                            │
                Clicks "Continue with Corporate Account"
                            │
                            ▼
                GET /auth/corporate/start
                            │
            ┌───────────────┴────────────────┐
            │                                │
   local server reachable?         not reachable
            │                                │
            ▼                                ▼
  session["corporate_oauth_state"]    render error.html
       = random uuid                  HTTP 503 "OAuth Server Not Available"
            │
            │  302 → http://localhost:5500/oauth/authorize?…
            ▼
   ┌────────────────────────────┐
   │  oauth2_server.py login    │
   │  (in-house, local network) │
   └────────────┬───────────────┘
                │
       User enters credentials
                │
                ▼
   GET /auth/corporate/callback?code=…&state=…
                │
       state matches session?            no → flash "CSRF" → /auth/login
                │ yes
                ▼
       POST /oauth/token   (client_id + client_secret via HTTP Basic)
                │
       GET  /api/userinfo  (Bearer token)
                │
       any HTTP error?                   yes → error.html (503)
                │ no
                ▼
   session.clear()
   session["username"]    = username
   session["role"]        = "user"
   session["auth_source"] = "corporate"
                │
                ▼
        Redirect → home page
```

The corporate flow performs **no internet calls**. The local `oauth2_server.py`
owns corporate users — the app keeps **no record** of them in `users.json`.

---

## Session states

```
[Not logged in]
      │
      │  (Google flow)
      ▼
[oauth_pending]   session["oauth_pending"] = google_sub
      │
      │  TOTP succeeded
      ▼
[Logged in]       session["username"], ["role"], ["google_sub"]


[Not logged in]
      │
      │  (Corporate flow) — single step, no intermediate state
      ▼
[Logged in]       session["username"], ["role"], ["auth_source"] = "corporate"
```

The `@login_required` decorator in `auth_routes.py` simply checks
`"username" in session` — both providers populate that field, so downstream
routes treat both kinds of users identically.

---

## Components

### `presentations/routes/auth_routes.py` — shared

| Route | Method | Purpose |
|---|---|---|
| `/auth/login` | GET | Login page; redirects to home if already logged in |
| `/auth/logout` | GET | `session.clear()`, redirect to login |
| `/auth/totp-setup` | GET / POST | First-time TOTP enrollment (QR code) |
| `/auth/totp-verify` | GET / POST | TOTP code prompt for existing users |

Also defines `@login_required` and the `_complete_login()` helper.

### `presentations/routes/auth_google.py` — Google only

| Route | Method | Purpose |
|---|---|---|
| `/auth/google` | GET | Redirect to Google's consent screen via authlib |
| `/auth/callback` | GET | Receive Google's redirect, look up user, route to TOTP |

Also exposes `init(app)`, called once from `create_app()` to register the
authlib Google client with `GOOGLE_CLIENT_ID` / `GOOGLE_CLIENT_SECRET`.

### `presentations/routes/auth_corporate.py` — Corporate only

| Route | Method | Purpose |
|---|---|---|
| `/auth/corporate/start` | GET | Probe server reachability, then 302 to `oauth2_server` |
| `/auth/corporate/callback` | GET | Validate state, exchange code, fetch userinfo, log user in |

Includes `_is_server_reachable()` and `_server_unavailable_page()` for the
503 error page when `oauth2_server.py` is not running.

### `presentations/services/totp_service.py`

| Function | Purpose |
|---|---|
| `generate_secret()` | Random base32 secret via `pyotp` |
| `provisioning_uri(secret, email)` | Provisioning URI for QR-code rendering |
| `verify(secret, code)` | Validates 6-digit code (±30 s tolerance) |
| `qr_code_base64(uri)` | PNG QR code as base64 for inline HTML |

### `presentations/services/user_store.py`

Thread-safe JSON-backed store in `data/users.json`. Keys are Google `sub`
identifiers. **Corporate users are not stored here** — the local OAuth
server owns them.

```json
{
  "google_sub": "117657466979844859441",
  "email": "user@example.com",
  "name": "Some User",
  "role": "user",
  "totp_secret": "BASE32SECRET…",
  "totp_enrolled": true
}
```

---

## Environment variables

| Variable | Used by | Default |
|---|---|---|
| `GOOGLE_CLIENT_ID` | `auth_google.init()` | *required* |
| `GOOGLE_CLIENT_SECRET` | `auth_google.init()` | *required* |
| `FLASK_SECRET_KEY` | Flask session signing | `"dev-only-change-in-prod"` |
| `FLASK_RUN_HOST` | Flask CLI | `127.0.0.1` (set to `localhost` for Google) |
| `FLASK_RUN_PORT` | Flask CLI | `5000` (set to `5001` to match Google Console) |
| `CORPORATE_OAUTH2_SERVER` | `auth_corporate` | `http://localhost:5500` |
| `CORPORATE_OAUTH2_CLIENT_ID` | `auth_corporate` | `demo-client` |
| `CORPORATE_OAUTH2_CLIENT_SECRET` | `auth_corporate` | `demo-secret-123` |

`set_env.sh` exports the Google credentials and Flask host/port for local dev;
`./run.sh` sources it and launches Flask.

The authorized redirect URI registered in Google Cloud Console must be:
```
http://localhost:5001/auth/callback
```

---

## Adding a third OAuth provider

1. Create `presentations/routes/auth_xxx.py` with a `Blueprint("xxx_auth", …)`
   defining `/start` and `/callback`.
2. Register it in `presentations/app.py`:
   ```python
   from presentations.routes import auth_xxx
   app.register_blueprint(auth_xxx.bp)
   ```
3. Add a button to `presentations/templates/auth/login.html`:
   ```html
   <a class="btn btn-outline-…" href="{{ url_for('xxx_auth.start') }}">
     Continue with XXX
   </a>
   ```

Existing providers stay untouched.

---

## Dependencies

| Package | Used by |
|---|---|
| `authlib` | Google OAuth client |
| `requests` | Corporate OAuth client |
| `pyotp` | TOTP secret + verification |
| `qrcode[pil]` | QR code rendering |
| `flask` | sessions, routes, blueprints |

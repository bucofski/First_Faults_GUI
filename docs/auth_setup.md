# Enabling the auth scaffold from `security/a01-a07-auth-scaffold`

This branch replaces the stub `CredentialService` (which always returned an
admin user) with a real authentication layer: hashed passwords, a login/logout
flow, CSRF on the login form, and `login_required` / `role_required`
decorators. To use it on your machine you need to do a few things beyond just
checking out the branch.

## 1. Check out the branch and sync dependencies

```bash
git fetch origin
git checkout security/a01-a07-auth-scaffold
git pull
```

A new runtime dependency was added in `pyproject.toml`
(`werkzeug` is pulled in via `flask[all]`, but the password hashing helper is
new code that uses it). Re-sync your environment:

```bash
uv sync          # or: pip install -e .
```

## 2. Provide a user store

The app loads users from one of two environment variables (checked in order):

1. `AUTH_USERS_JSON` — a JSON blob inline.
2. `AUTH_USERS_FILE` — path to a JSON file.

If neither is set, **no user can log in** and every protected page will
redirect to `/auth/login`.

The repo ships an example file at `users.json` in the project root. To use it:

```bash
export AUTH_USERS_FILE="$PWD/users.json"
```

The shipped `users.json` contains demo accounts (`benoit`, `tom`). Treat those
hashes as throwaway — generate your own before doing anything real.

## 3. Generate your own password hashes

```bash
python scripts/hash_password.py <username> <role>
```

The script prompts for a password (not echoed), confirms it, and prints a
JSON fragment like:

```json
{
  "alice": {
    "password_hash": "scrypt:32768:8:1$...",
    "role": "admin"
  }
}
```

Drop the fragment(s) into your `users.json` (or concatenate into
`AUTH_USERS_JSON`). Valid roles come from `presentations/services/creadential.py`
(`Role` enum) — typically `admin`, `user`, `guest`.

## 4. Set a real `SECRET_KEY` (recommended)

`presentations/app.py` currently hardcodes `app.secret_key = "dev"`. Sessions
and the CSRF token both depend on this key, so for any non-local use replace
it with an env-driven value, e.g.:

```python
app.secret_key = os.environ["FLASK_SECRET_KEY"]
```

and export `FLASK_SECRET_KEY` to a long random string. For local dev the
default `"dev"` works but invalidates sessions on restart.

## 5. Run the app

```bash
export AUTH_USERS_FILE="$PWD/users.json"
# optional: export FLASK_SECRET_KEY="$(python -c 'import secrets; print(secrets.token_hex(32))')"
python -m presentations.app    # or however you normally launch it
```

Then visit `http://127.0.0.1:5000/`. You will be redirected to
`/auth/login`. After a successful login you land on `plc.home`. The navbar
gains a logout link (POST to `/auth/logout`).

## 6. Verifying it works

- Hitting any `plc.*` route while logged out → 302 to `/auth/login`.
- Wrong password → flash message + HTTP 401, with a `LOGIN_FAILURE` line in
  the `auth` logger.
- Successful login → `LOGIN_SUCCESS` log line, session cookie set.
- Logout → session cleared, `LOGOUT` log line.
- Denied access on a `role_required` route → `AUTHZ_DENY reason=role ...`
  in the `security` logger and HTTP 403.

## 7. Things to watch out for when merging downstream

- Any code still importing `CredentialService` for its old "always admin"
  behaviour will break — that shortcut is gone. Use `current_user()` from
  `presentations.services.auth` instead.
- The login form submits a `csrf_token` field; if you build other POST
  forms, include `{{ csrf_token() }}` and validate it the same way
  `auth_routes.login` does.
- `users.json` is committed for demo purposes. Do **not** commit a file
  containing real credentials — point `AUTH_USERS_FILE` at something
  outside the repo, or use `AUTH_USERS_JSON` from your secret manager.

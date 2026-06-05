import json
import threading
from pathlib import Path

_STORE_PATH = Path(__file__).resolve().parent.parent.parent / "data" / "users.json"
_lock = threading.Lock()


def _load() -> dict:
    if not _STORE_PATH.exists():
        return {"users": {}}
    with open(_STORE_PATH) as f:
        return json.load(f)


def _save(data: dict) -> None:
    _STORE_PATH.parent.mkdir(parents=True, exist_ok=True)
    with open(_STORE_PATH, "w") as f:
        json.dump(data, f, indent=2)


def get_user(google_sub: str) -> dict | None:
    with _lock:
        data = _load()
        return data["users"].get(google_sub)


def upsert_user(google_sub: str, email: str, name: str) -> dict:
    with _lock:
        data = _load()
        existing = data["users"].get(google_sub)
        if existing is None:
            existing = {
                "google_sub": google_sub,
                "email": email,
                "name": name,
                "role": "user",
                "totp_secret": None,
                "totp_enrolled": False,
            }
        else:
            existing["email"] = email
            existing["name"] = name
        data["users"][google_sub] = existing
        _save(data)
        return dict(existing)


def enroll_totp(google_sub: str, secret: str) -> None:
    with _lock:
        data = _load()
        user = data["users"].get(google_sub)
        if user is None:
            raise KeyError(f"User {google_sub} not found")
        user["totp_secret"] = secret
        user["totp_enrolled"] = True
        data["users"][google_sub] = user
        _save(data)

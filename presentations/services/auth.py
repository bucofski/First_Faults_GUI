"""Authentication and authorization helpers.

Replaces the previous stub `CredentialService` that returned an admin user
unconditionally. This module:

  * loads a user store from env vars (`AUTH_USERS_JSON`) or a JSON file
    pointed to by `AUTH_USERS_FILE`
  * verifies passwords with werkzeug.security
  * exposes `login_required` and `role_required` decorators that emit
    structured `AUTHZ_DENY` security events when access is refused
"""
from __future__ import annotations

import json
import logging
import os
from dataclasses import dataclass
from functools import wraps
from pathlib import Path
from typing import Iterable

from flask import abort, redirect, request, session, url_for
from werkzeug.security import check_password_hash

from presentations.services.creadential import Role

_auth_log = logging.getLogger("auth")
_sec_log = logging.getLogger("security")


@dataclass(frozen=True)
class StoredUser:
    username: str
    password_hash: str
    role: Role


def _parse_users(raw: dict) -> dict[str, StoredUser]:
    out: dict[str, StoredUser] = {}
    for username, entry in raw.items():
        try:
            role = Role(entry.get("role", "user"))
        except ValueError:
            role = Role.GUEST
        out[username] = StoredUser(
            username=username,
            password_hash=entry["password_hash"],
            role=role,
        )
    return out


def _load_user_store() -> dict[str, StoredUser]:
    raw_json = os.environ.get("AUTH_USERS_JSON")
    if raw_json:
        return _parse_users(json.loads(raw_json))

    file_path = os.environ.get("AUTH_USERS_FILE")
    if file_path and Path(file_path).exists():
        with open(file_path, "r", encoding="utf-8") as f:
            return _parse_users(json.load(f))

    return {}


_USERS: dict[str, StoredUser] | None = None


def _users() -> dict[str, StoredUser]:
    global _USERS
    if _USERS is None:
        _USERS = _load_user_store()
    return _USERS


def authenticate(username: str, password: str) -> StoredUser | None:
    user = _users().get(username)
    if user is None:
        return None
    if not check_password_hash(user.password_hash, password):
        return None
    return user


def current_user() -> dict | None:
    if "username" not in session:
        return None
    return {"username": session["username"], "role": session.get("role", Role.GUEST.value)}


def login_required(view):
    @wraps(view)
    def wrapper(*args, **kwargs):
        if "username" not in session:
            _sec_log.info(
                "AUTHZ_DENY reason=anonymous ip=%s path=%s method=%s",
                request.remote_addr or "-", request.path, request.method,
            )
            return redirect(url_for("auth.login", next=request.full_path))
        return view(*args, **kwargs)

    return wrapper


def role_required(*allowed: Role | str):
    allowed_values = {r.value if isinstance(r, Role) else r for r in allowed}

    def decorator(view):
        @wraps(view)
        def wrapper(*args, **kwargs):
            user = current_user()
            if user is None:
                _sec_log.info(
                    "AUTHZ_DENY reason=anonymous ip=%s path=%s method=%s",
                    request.remote_addr or "-", request.path, request.method,
                )
                return redirect(url_for("auth.login", next=request.full_path))
            if user["role"] not in allowed_values:
                _sec_log.warning(
                    "AUTHZ_DENY reason=role ip=%s user=%s role=%s required=%s path=%s",
                    request.remote_addr or "-", user["username"], user["role"],
                    sorted(allowed_values), request.path,
                )
                abort(403)
            return view(*args, **kwargs)

        return wrapper

    return decorator


def login_user(user: StoredUser) -> None:
    session.clear()
    session["username"] = user.username
    session["role"] = user.role.value
    session.permanent = True
    _auth_log.info("LOGIN_SUCCESS user=%s role=%s ip=%s",
                   user.username, user.role.value, request.remote_addr or "-")


def logout_user() -> None:
    name = session.get("username", "-")
    session.clear()
    _auth_log.info("LOGOUT user=%s ip=%s", name, request.remote_addr or "-")


def log_failed_login(username: str) -> None:
    _auth_log.warning(
        "LOGIN_FAILURE user=%s ip=%s",
        username or "-", request.remote_addr or "-",
    )


__all__: Iterable[str] = (
    "StoredUser",
    "authenticate",
    "current_user",
    "login_required",
    "role_required",
    "login_user",
    "logout_user",
    "log_failed_login",
)

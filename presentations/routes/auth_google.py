"""
Google OAuth blueprint — fully self-contained.

Talks ONLY to accounts.google.com via authlib. After authentication, hands
the user off to the shared TOTP routes for 2-factor enrollment / verification.

URLs are intentionally kept at /auth/google and /auth/callback so the existing
Google Cloud Console "Authorized redirect URI" (http://localhost:5001/auth/callback)
does not need to be changed.
"""

import logging
import os

from authlib.integrations.flask_client import OAuth
from flask import Blueprint, flash, redirect, session, url_for

from presentations.services import user_store

_log = logging.getLogger("auth.google")
_oauth = OAuth()

bp = Blueprint("google_auth", __name__)


def init(app) -> None:
    """
    Initializes authentication configuration for Google OAuth.

    This function sets up the required environment and OAuth registration
    necessary for enabling authentication with Google. It retrieves the
    Google Client ID and Client Secret from the environment variables,
    ensures their presence, and configures the OAuth library.
    Additionally, it allows insecure transport during local development.

    Raises:
        RuntimeError: If GOOGLE_CLIENT_ID or GOOGLE_CLIENT_SECRET is not
            set in the environment variables.

    Args:
        app: The application instance to be configured for OAuth.

    Returns:
        None
    """
    client_id = os.environ.get("GOOGLE_CLIENT_ID")
    client_secret = os.environ.get("GOOGLE_CLIENT_SECRET")
    if not client_id or not client_secret:
        raise RuntimeError(
            "GOOGLE_CLIENT_ID and GOOGLE_CLIENT_SECRET must be set. "
            "See https://console.cloud.google.com/apis/credentials"
        )

    # Allow http://localhost during local development.
    os.environ.setdefault("AUTHLIB_INSECURE_TRANSPORT", "1")

    _oauth.init_app(app)
    _oauth.register(
        name="google",
        client_id=client_id,
        client_secret=client_secret,
        server_metadata_url="https://accounts.google.com/.well-known/openid-configuration",
        client_kwargs={"scope": "openid email profile"},
    )


@bp.route("/auth/google")
def start():
    """
    Start the Google OAuth authorization process.

    This function handles the initiation of the Google OAuth 2.0 flow by creating
    a redirect to Google's authorization endpoint.

    Returns:
        flask.wrappers.Response: A redirect response that points to the Google
        authorization endpoint.
    """
    redirect_uri = url_for("google_auth.callback", _external=True)
    return _oauth.google.authorize_redirect(redirect_uri)


@bp.route("/auth/callback")
def callback():
    """
    Handles the OAuth 2.0 callback from Google after user authentication and authorization.
    This route processes the access token, retrieves user information, and initiates partial or full
    authentication flow based on whether Time-based OTP (TOTP) is already enrolled.

    Raises:
        Exception: If the token exchange with Google fails.

    Parameters:
        None

    Returns:
        Response: A redirect response to the appropriate page based on the authentication state.
    """
    try:
        token = _oauth.google.authorize_access_token()
    except Exception as exc:
        _log.warning("Google OAuth token exchange failed: %s", exc)
        flash("Google login failed — please try again.", "error")
        return redirect(url_for("auth.login"))

    user_info = token.get("userinfo")
    if not user_info:
        flash("Google login failed — no user info returned.", "error")
        return redirect(url_for("auth.login"))

    google_sub = user_info["sub"]
    email = user_info.get("email", "")
    name = user_info.get("name", email)

    user = user_store.upsert_user(google_sub, email, name)
    _log.info(
        "Google OAuth callback: sub=%s email=%s totp_enrolled=%s",
        google_sub, email, user["totp_enrolled"],
    )

    # Partial-auth state — full session is set only after TOTP succeeds.
    session["oauth_pending"] = google_sub

    if user["totp_enrolled"]:
        return redirect(url_for("auth.totp_verify"))
    return redirect(url_for("auth.totp_setup"))

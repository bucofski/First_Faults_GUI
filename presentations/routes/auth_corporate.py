"""
Corporate OAuth blueprint — fully self-contained.

Talks ONLY to the in-house OAuth2 Authorization Server
(auth_server/oauth2_server.py, default http://localhost:5500).

No internet calls. No TOTP. No shared user_store with Google.
The local oauth_server owns corporate user management; once it confirms
the user, we put them straight into the Flask session.
"""

import logging
import os
import uuid
from urllib.parse import urlencode

import requests
from flask import (
    Blueprint,
    flash,
    redirect,
    render_template,
    request,
    session,
    url_for,
)

_log = logging.getLogger("auth.corporate")

bp = Blueprint("corporate_auth", __name__, url_prefix="/auth/corporate")

_STATE_KEY = "corporate_oauth_state"

# Configuration is read at request time, so env vars set via set_env.sh /
# system shell still apply even if the module was imported before they were set.

def _config() -> tuple[str, str, str]:
    """
    Retrieves OAuth2 configuration values from environment variables.

    This function fetches the `CORPORATE_OAUTH2_SERVER`, `CORPORATE_OAUTH2_CLIENT_ID`,
    and `CORPORATE_OAUTH2_CLIENT_SECRET` environment variables. If these variables are
    not set, it falls back to default values. The function returns a tuple containing
    the processed server URL, client ID, and client secret.

    Returns:
        tuple[str, str, str]: A tuple containing the OAuth2 server URL, client ID,
        and client secret.
    """
    server = os.environ.get("CORPORATE_OAUTH2_SERVER", "http://localhost:5500")
    client_id = os.environ.get("CORPORATE_OAUTH2_CLIENT_ID", "demo-client")
    client_secret = os.environ.get("CORPORATE_OAUTH2_CLIENT_SECRET", "demo-secret-123")
    return server.rstrip("/"), client_id, client_secret


def _is_server_reachable(server: str) -> bool:
    """
    Checks if a given server is reachable within a timeout of 3 seconds.

    This function attempts to send a GET request to the specified server. If the
    request is successful within the timeout, the server is considered reachable.
    If a RequestException occurs, it logs a warning and indicates that the server
    is unreachable.

    Parameters:
    server (str): The URL of the server to check for reachability.

    Returns:
    bool: True if the server is reachable, False otherwise.
    """
    try:
        requests.get(server, timeout=3)
        return True
    except requests.RequestException as exc:
        _log.warning("Corporate OAuth server unreachable at %s: %s", server, exc)
        return False


def _server_unavailable_page(server: str):
    """
    Renders an error page for unavailable OAuth server.

    This function is used to display a custom error page when the corporate OAuth
    authentication server is not available. It provides a detailed error message
    and a suggestion for the user to try again later or use an alternative sign-in
    method.

    Parameters:
    server (str): The URL or identifier of the corporate authentication server
                  that is unavailable.

    Returns:
    tuple: A rendered HTML page using the "error.html" template along with an
           HTTP status code 503.
    """
    return render_template(
        "error.html",
        title="Corporate Login Unavailable",
        error_code=503,
        error_title="OAuth Server Not Available",
        error_message=(
            f"The corporate authentication server at {server} is offline. "
            "Please try again later or sign in with Google."
        ),
    ), 503


@bp.route("/start")
def start():
    """
    Handles the start of the OAuth authorization process by initiating the
    redirection to the authorization server with required parameters.

    Raises an error if the authorization server is unavailable.

    Arguments:
        None

    Returns:
        Werkzeug Response: Redirects the user to the authorization server's
        OAuth endpoint with the appropriate query parameters.
    """
    server, client_id, _ = _config()
    if not _is_server_reachable(server):
        return _server_unavailable_page(server)

    redirect_uri = url_for("corporate_auth.callback", _external=True)

    state = str(uuid.uuid4())
    session[_STATE_KEY] = state

    params = {
        "response_type": "code",
        "client_id": client_id,
        "redirect_uri": redirect_uri,
        "state": state,
        "scope": "openid profile",
    }
    return redirect(f"{server}/oauth/authorize?{urlencode(params)}")


@bp.route("/callback")
def callback():
    """
    Handles the callback for corporate authentication.

    This function processes the OAuth callback after the user attempts to log in
    using corporate authentication. It handles authorization errors, validates
    state to prevent potential CSRF attacks, exchanges the authorization code
    for an access token, fetches user information from the OAuth server, and
    logs the user in if all steps are successful. In the event of failures at any
    step, it redirects to the login page with appropriate error messages.

    Raises
    ------
    Redirect : Redirects to the login page in case of authentication errors,
               state mismatches, missing authorization code, unsuccessful
               login, or server unavailability.

    Parameters
    ----------
    None

    Returns
    -------
    Redirect
        Redirects the user to the appropriate page depending on the authentication
        outcome.
    """
    server, client_id, client_secret = _config()
    redirect_uri = url_for("corporate_auth.callback", _external=True)

    error = request.args.get("error")
    if error:
        flash(f"Corporate login error: {error}", "error")
        return redirect(url_for("auth.login"))

    received_state = request.args.get("state")
    expected_state = session.pop(_STATE_KEY, None)
    if not expected_state or received_state != expected_state:
        flash("State mismatch — possible CSRF attack.", "error")
        return redirect(url_for("auth.login"))

    code = request.args.get("code")
    if not code:
        flash("Corporate login failed — no authorization code returned.", "error")
        return redirect(url_for("auth.login"))

    try:
        token_resp = requests.post(
            f"{server}/oauth/token",
            data={
                "grant_type": "authorization_code",
                "code": code,
                "redirect_uri": redirect_uri,
            },
            auth=(client_id, client_secret),
            timeout=10,
        )
        token_resp.raise_for_status()
        access_token = token_resp.json()["access_token"]

        userinfo_resp = requests.get(
            f"{server}/api/userinfo",
            headers={"Authorization": f"Bearer {access_token}"},
            timeout=10,
        )
        userinfo_resp.raise_for_status()
        info = userinfo_resp.json()
    except requests.RequestException as exc:
        _log.warning("Corporate OAuth HTTP error: %s", exc)
        return _server_unavailable_page(server)

    username = info.get("username") or ""
    if not username:
        flash("Corporate login failed — no username returned.", "error")
        return redirect(url_for("auth.login"))

    # The local oauth_server has already authenticated the user — log them in
    # directly. No TOTP, no extra factor.
    session.clear()
    session["username"] = username
    session["role"] = "user"
    session["auth_source"] = "corporate"

    _log.info("Corporate login OK: username=%s", username)
    return redirect(session.pop("next", url_for("plc.home")))

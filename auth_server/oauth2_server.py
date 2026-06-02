"""
Eenvoudige OAuth2 Authorization Server implementatie met Flask.
Demonstreert de Authorization Code grant flow.
"""

import uuid
import base64
import hashlib
from datetime import UTC, datetime, timedelta
from functools import wraps
from urllib.parse import urlencode

from flask import Flask, request, redirect, jsonify, render_template_string
from jinja2 import ChoiceLoader, DictLoader

app = Flask(__name__)


def _now() -> datetime:
    """Timezone-aware UTC 'now'. Replaces the deprecated datetime.utcnow()."""
    return datetime.now(UTC)

# In-memory "database" voor demonstratie doeleinden
CLIENTS = {
    "demo-client": {
        "client_secret": "demo-secret-123",
        "redirect_uris": [
            "http://localhost:5001/callback",                      # standalone oauth2_client.py demo
            "http://localhost:5000/auth/corporate/callback",       # First Faults GUI (flask run default)
            "http://localhost:5001/auth/corporate/callback",       # First Faults GUI (config.toml port)
            "http://127.0.0.1:5000/auth/corporate/callback",       # First Faults GUI (127.0.0.1 variant)
            "http://127.0.0.1:5001/auth/corporate/callback",       # First Faults GUI (127.0.0.1 variant)
        ],
        "name": "First Faults GUI"
    }
}

USERS = {
    "benoit": {"password": "benoitke1401", "name": "Alice"},
    "tom": {"password": "geheim456", "name": "Bob"}
}

# Tokens en codes opslag
authorization_codes = {}
access_tokens = {}
refresh_tokens = {}


# --- Hulpfuncties ---

def generate_token():
    """Genereer een willekeurige token string."""
    return base64.urlsafe_b64encode(
        hashlib.sha256(uuid.uuid4().bytes).digest()
    ).rstrip(b'=').decode('ascii')


def require_client_auth(f):
    """Decorator die client authenticatie vereist voor token endpoint."""
    @wraps(f)
    def decorated(*args, **kwargs):
        auth = request.authorization
        if not auth:
            return jsonify({"error": "invalid_client"}), 401
        
        client = CLIENTS.get(auth.username)
        if not client or client["client_secret"] != auth.password:
            return jsonify({"error": "invalid_client"}), 401
        
        request.client = client
        return f(*args, **kwargs)
    return decorated


# --- HTML Templates (eenvoudig inline) ---

_BASE_TEMPLATE = """
<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta content="width=device-width, initial-scale=1" name="viewport">
    <title>{% block title %}First Faults — Authorization Server{% endblock %}</title>
    <link
        crossorigin="anonymous"
        href="https://cdn.jsdelivr.net/npm/bootstrap@5.3.2/dist/css/bootstrap.min.css"
        rel="stylesheet">
    <style>
        body { background-color: #f8f9fa; font-family: Arial, sans-serif; }
    </style>
</head>
<body>
<nav class="navbar navbar-expand-lg navbar-dark bg-dark">
    <div class="container-fluid">
        <span class="navbar-brand">First Faults — Authorization Server</span>
    </div>
</nav>

<div class="container my-4">
    {% block content %}{% endblock %}
</div>

<script
    crossorigin="anonymous"
    src="https://cdn.jsdelivr.net/npm/bootstrap@5.3.2/dist/js/bootstrap.bundle.min.js">
</script>
</body>
</html>
"""

# Register the base template so `{% extends "base.html" %}` resolves below.
_dict_loader = DictLoader({"base.html": _BASE_TEMPLATE})
_default_loader = app.jinja_loader
app.jinja_loader = (
    ChoiceLoader([_default_loader, _dict_loader]) if _default_loader else _dict_loader
)

LOGIN_TEMPLATE = """
{% extends "base.html" %}
{% block title %}Sign in — First Faults Auth Server{% endblock %}
{% block content %}
<div class="row justify-content-center mt-5">
  <div class="col-md-5 col-lg-4">
    <div class="card shadow-sm">
      <div class="card-body p-4">
        <h3 class="card-title text-center mb-4">Sign in</h3>

        <div class="alert alert-secondary py-2 mb-3">
          <strong>{{ client_name }}</strong> is requesting access to your account.
        </div>

        {% if error %}
          <div class="alert alert-danger">{{ error }}</div>
        {% endif %}

        <form method="POST">
          <input type="hidden" name="client_id" value="{{ client_id }}">
          <input type="hidden" name="redirect_uri" value="{{ redirect_uri }}">
          <input type="hidden" name="state" value="{{ state }}">

          <div class="mb-3">
            <label for="username" class="form-label">Username</label>
            <input type="text" class="form-control" id="username" name="username"
                   autocomplete="username" required autofocus>
          </div>

          <div class="mb-3">
            <label for="password" class="form-label">Password</label>
            <input type="password" class="form-control" id="password" name="password"
                   autocomplete="current-password" required>
          </div>

          <div class="d-grid">
            <button type="submit" class="btn btn-primary btn-lg">
              Sign in & Authorize
            </button>
          </div>
        </form>
      </div>
    </div>
  </div>
</div>
{% endblock %}
"""

SUCCESS_TEMPLATE = """
{% extends "base.html" %}
{% block title %}Authorization Successful — First Faults Auth Server{% endblock %}
{% block content %}
<div class="row justify-content-center mt-5">
  <div class="col-md-6">
    <div class="card shadow-sm">
      <div class="card-body p-4 text-center">
        <h2 class="text-success mb-3">✓ Authorization Successful</h2>
        <p class="text-muted">The client application has received an authorization code.</p>
        <div class="bg-light border rounded p-3 my-3 text-break">
          <small class="text-muted d-block mb-1">Authorization Code</small>
          <code>{{ code }}</code>
        </div>
        <p class="text-muted mb-0">The client is now exchanging this code for an access token.</p>
      </div>
    </div>
  </div>
</div>
{% endblock %}
"""


# --- OAuth2 Endpoints ---

@app.route("/oauth/authorize", methods=["GET", "POST"])
def authorize():
    """
    Authorization Endpoint.
    Stap 1: Gebruiker wordt doorgestuurd hierheen met client_id, redirect_uri, response_type, state.
    Stap 2: Gebruiker logt in en geeft toestemming.
    Stap 3: Server stuurt gebruiker terug naar client met autorisatie code.
    """
    if request.method == "GET":
        # Stap 1: Toon login formulier
        client_id = request.args.get("client_id")
        redirect_uri = request.args.get("redirect_uri")
        response_type = request.args.get("response_type")
        state = request.args.get("state", "")
        scope = request.args.get("scope", "")
        
        # Validatie
        client = CLIENTS.get(client_id)
        if not client:
            return jsonify({"error": "invalid_client"}), 400
        if redirect_uri not in client["redirect_uris"]:
            return jsonify({"error": "invalid_redirect_uri"}), 400
        if response_type != "code":
            return redirect(f"{redirect_uri}?error=unsupported_response_type&state={state}")
        
        return render_template_string(LOGIN_TEMPLATE,
            client_name=client["name"],
            client_id=client_id,
            redirect_uri=redirect_uri,
            state=state,
            error=None
        )
    
    else:  # POST
        # Stap 2: Verwerk login
        client_id = request.form.get("client_id")
        redirect_uri = request.form.get("redirect_uri")
        state = request.form.get("state", "")
        username = request.form.get("username")
        password = request.form.get("password")
        
        client = CLIENTS.get(client_id)
        if not client:
            return jsonify({"error": "invalid_client"}), 400
        
        # Authenticatie gebruiker
        user = USERS.get(username)
        if not user or user["password"] != password:
            return render_template_string(LOGIN_TEMPLATE,
                client_name=client["name"],
                client_id=client_id,
                redirect_uri=redirect_uri,
                state=state,
                error="Ongeldige gebruikersnaam of wachtwoord"
            ), 401
        
        # Stap 3: Genereer autorisatie code
        code = generate_token()
        authorization_codes[code] = {
            "client_id": client_id,
            "redirect_uri": redirect_uri,
            "username": username,
            "expires": _now() + timedelta(minutes=10)
        }
        
        # Redirect terug naar client met code
        params = {"code": code}
        if state:
            params["state"] = state
        
        return redirect(f"{redirect_uri}?{urlencode(params)}")


@app.route("/oauth/token", methods=["POST"])
@require_client_auth
def token():
    """
    Token Endpoint.
    Client ruilt autorisatie code in voor access token (en optioneel refresh token).
    Vereist client authenticatie (Basic Auth).
    """
    grant_type = request.form.get("grant_type")
    
    if grant_type == "authorization_code":
        code = request.form.get("code")
        redirect_uri = request.form.get("redirect_uri")
        
        auth_code = authorization_codes.get(code)
        if not auth_code:
            return jsonify({"error": "invalid_grant"}), 400
        
        # Verwijder code (mag maar 1 keer gebruikt worden)
        del authorization_codes[code]
        
        # Controleer vervaldatum
        if _now() > auth_code["expires"]:
            return jsonify({"error": "invalid_grant", "error_description": "Code expired"}), 400
        
        # Controleer client_id en redirect_uri
        if auth_code["client_id"] != request.authorization.username:
            return jsonify({"error": "invalid_grant"}), 400
        if auth_code["redirect_uri"] != redirect_uri:
            return jsonify({"error": "invalid_grant"}), 400
        
        # Genereer tokens
        access_token = generate_token()
        refresh_token = generate_token()
        expires_in = 3600  # 1 uur
        
        access_tokens[access_token] = {
            "username": auth_code["username"],
            "client_id": auth_code["client_id"],
            "expires": _now() + timedelta(seconds=expires_in)
        }
        
        refresh_tokens[refresh_token] = {
            "username": auth_code["username"],
            "client_id": auth_code["client_id"]
        }
        
        return jsonify({
            "access_token": access_token,
            "token_type": "Bearer",
            "expires_in": expires_in,
            "refresh_token": refresh_token
        })
    
    elif grant_type == "refresh_token":
        refresh_token_value = request.form.get("refresh_token")
        refresh_data = refresh_tokens.get(refresh_token_value)
        
        if not refresh_data:
            return jsonify({"error": "invalid_grant"}), 400
        
        # Genereer nieuw access token
        access_token = generate_token()
        expires_in = 3600
        
        access_tokens[access_token] = {
            "username": refresh_data["username"],
            "client_id": refresh_data["client_id"],
            "expires": _now() + timedelta(seconds=expires_in)
        }
        
        return jsonify({
            "access_token": access_token,
            "token_type": "Bearer",
            "expires_in": expires_in
        })
    
    else:
        return jsonify({"error": "unsupported_grant_type"}), 400


@app.route("/api/userinfo")
def userinfo():
    """
    Beschermde resource endpoint.
    Vereist een geldig access token in de Authorization header.
    """
    auth_header = request.headers.get("Authorization", "")
    if not auth_header.startswith("Bearer "):
        return jsonify({"error": "invalid_token"}), 401
    
    token = auth_header[7:]
    token_data = access_tokens.get(token)
    
    if not token_data:
        return jsonify({"error": "invalid_token"}), 401
    
    if _now() > token_data["expires"]:
        return jsonify({"error": "invalid_token", "error_description": "Token expired"}), 401
    
    user = USERS.get(token_data["username"])
    return jsonify({
        "username": token_data["username"],
        "name": user["name"],
        "client_id": token_data["client_id"]
    })


@app.route("/")
def index():
    """Info pagina voor de OAuth2 server."""
    return jsonify({
        "service": "OAuth2 Authorization Server",
        "endpoints": {
            "authorization": "/oauth/authorize",
            "token": "/oauth/token",
            "userinfo": "/api/userinfo"
        },
        "grant_types_supported": ["authorization_code", "refresh_token"],
        "token_endpoint_auth_methods_supported": ["client_secret_basic"]
    })


if __name__ == "__main__":
    print("=" * 60)
    print("OAuth2 Authorization Server")
    print("Draait op http://localhost:5500")
    print("=" * 60)
    app.run(host="0.0.0.0", port=5500, debug=True)

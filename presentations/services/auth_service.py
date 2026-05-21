import base64
import io
import os

import pyotp
import qrcode
from authlib.integrations.flask_client import OAuth

_oauth = OAuth()
_APP_NAME = "First Faults GUI"


def init_oauth(app):
    client_id = os.environ.get("GOOGLE_CLIENT_ID")
    client_secret = os.environ.get("GOOGLE_CLIENT_SECRET")
    if not client_id or not client_secret:
        raise RuntimeError(
            "GOOGLE_CLIENT_ID and GOOGLE_CLIENT_SECRET must be set. "
            "See https://console.cloud.google.com/apis/credentials"
        )

    # Allow http://localhost during local development.
    # Remove this line (or unset the env var) in production.
    os.environ.setdefault("AUTHLIB_INSECURE_TRANSPORT", "1")

    _oauth.init_app(app)
    _oauth.register(
        name="google",
        client_id=client_id,
        client_secret=client_secret,
        server_metadata_url="https://accounts.google.com/.well-known/openid-configuration",
        client_kwargs={"scope": "openid email profile"},
    )


def google_client():
    return _oauth.google


def generate_totp_secret() -> str:
    return pyotp.random_base32()


def get_totp_uri(secret: str, email: str) -> str:
    return pyotp.totp.TOTP(secret).provisioning_uri(name=email, issuer_name=_APP_NAME)


def verify_totp(secret: str, code: str) -> bool:
    totp = pyotp.TOTP(secret)
    return totp.verify(code.strip(), valid_window=1)


def qr_code_base64(uri: str) -> str:
    img = qrcode.make(uri)
    buf = io.BytesIO()
    img.save(buf, format="PNG")
    return base64.b64encode(buf.getvalue()).decode()

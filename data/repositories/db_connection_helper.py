from __future__ import annotations
from functools import lru_cache
from pathlib import Path
from typing import Any, Dict
import urllib.parse
import yaml
from sqlalchemy import create_engine
from sqlalchemy.engine import Engine


_CONFIG_PATH = Path(__file__).resolve().parents[2] / "config" / "Connection.yaml"
_ENGINE: Engine | None = None


def _load_db_config() -> Dict[str, Any]:
    with _CONFIG_PATH.open("r", encoding="utf-8") as f:
        raw = yaml.safe_load(f) or {}

    if "DBconnection" not in raw:
        raise RuntimeError("Missing 'DBconnection' section in Connection.yaml")
    cfg = raw["DBconnection"]
    required_keys = [
        "host",
        "port",
        "database",
        "username",
        "password",
        "odbc_driver",
    ]
    missing = [k for k in required_keys if k not in cfg]
    if missing:
        raise RuntimeError(f"Missing keys in DBconnection: {', '.join(missing)}")

    # Provide sensible defaults for optional keys
    cfg.setdefault("encrypt", True)
    cfg.setdefault("trust_server_certificate", False)

    return cfg


def _build_odbc_connect_string(cfg: Dict[str, Any]) -> str:
    host = cfg["host"]
    port = cfg["port"]
    database = cfg["database"]
    username = cfg["username"]
    password = cfg["password"]
    odbc_driver = cfg["odbc_driver"]
    encrypt = bool(cfg.get("encrypt", True))
    trust_server_certificate = bool(cfg.get("trust_server_certificate", False))
    encrypt_str = "yes" if encrypt else "no"
    trust_str = "yes" if trust_server_certificate else "no"
    odbc_parts = [
        f"DRIVER={odbc_driver}",
        f"SERVER={host},{port}",
        f"DATABASE={database}",
        f"UID={username}",
        f"PWD={password}",
        f"Encrypt={encrypt_str}",
        f"TrustServerCertificate={trust_str}",
    ]

    odbc_conn_str = ";".join(odbc_parts)
    return urllib.parse.quote_plus(odbc_conn_str)

@lru_cache(maxsize=1)
def get_engine() -> Engine:
    cfg = _load_db_config()
    odbc_connect = _build_odbc_connect_string(cfg)
    url = f"mssql+pyodbc:///?odbc_connect={odbc_connect}"
    engine = create_engine(url, fast_executemany=True)
    return engine
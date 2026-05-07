import logging
import os
from pathlib import Path
from contextlib import contextmanager
from urllib.parse import quote_plus

import yaml
from sqlalchemy import create_engine, Engine
from sqlalchemy.orm import Session, DeclarativeBase

logger = logging.getLogger(__name__)

# ============================================================================
# Constants
# ============================================================================
LOCAL_DIRECTORY = Path(__file__).parent
CONFIG_PATH = LOCAL_DIRECTORY.parent.parent / "config" / "Connection.yaml"

# ============================================================================
# ORM Base
# ============================================================================
class Base(DeclarativeBase):
    pass


# ============================================================================
# Configuration Loader
# ============================================================================
_ENV_OVERRIDES = {
    "server": "DB_SERVER",
    "port": "DB_PORT",
    "database": "DB_NAME",
    "username": "DB_USERNAME",
    "password": "DB_PASSWORD",
    "driver": "DB_DRIVER",
    "encrypt": "DB_ENCRYPT",
    "trust_server_certificate": "DB_TRUST_SERVER_CERT",
}


def _coerce(key: str, value: str):
    if key == "port":
        return int(value)
    if key in ("encrypt", "trust_server_certificate"):
        return value.strip().lower() in ("1", "true", "yes", "on")
    return value


def load_db_config(config_path: Path | None = None) -> dict:
    """Load database configuration.

    Resolution order per field: environment variable → YAML file → KeyError.
    Credentials should be supplied via env vars in production; the YAML file
    is convenient for local development only.
    """
    path = config_path or CONFIG_PATH
    config: dict = {}
    if path.exists():
        try:
            with open(path, "r") as file:
                loaded = yaml.safe_load(file) or {}
            config = loaded.get("DBconnection", {}) or {}
        except (yaml.YAMLError, TypeError) as e:
            logger.error("Invalid database config in %s: %s", path, e)
            raise

    for key, env_name in _ENV_OVERRIDES.items():
        env_val = os.environ.get(env_name)
        if env_val is not None and env_val != "":
            config[key] = _coerce(key, env_val)

    missing = [k for k in ("server", "database", "username", "password", "driver") if not config.get(k)]
    if missing:
        raise KeyError(
            f"Missing required DB config keys: {missing}. "
            f"Set via env ({', '.join(_ENV_OVERRIDES[k] for k in missing)}) "
            f"or {path}."
        )
    return config


# ============================================================================
# ODBC Connection String Builder
# ============================================================================
def _build_odbc_connect_string(conn_config: dict) -> str:
    """Build ODBC connection string from configuration."""
    parts = [
        f"DRIVER={{{conn_config['driver']}}}",
        f"SERVER={conn_config['server']},{conn_config.get('port', 1433)}",
        f"DATABASE={conn_config['database']}",
        f"UID={conn_config['username']}",
        f"PWD={conn_config['password']}",
        f"Encrypt={'yes' if conn_config.get('encrypt', True) else 'no'}",
        f"TrustServerCertificate={'yes' if conn_config.get('trust_server_certificate', False) else 'no'}",
        "MARS_Connection=yes",
    ]
    return ";".join(parts)


# ============================================================================
# Global Engine (Lazy Initialization)
# ============================================================================
_engine: Engine | None = None


def get_engine() -> Engine:
    """Get or create the global engine instance (singleton)."""
    global _engine
    if _engine is None:
        cfg = load_db_config()
        odbc_connect = _build_odbc_connect_string(cfg)
        url = f"mssql+pyodbc:///?odbc_connect={quote_plus(odbc_connect)}"
        _engine = create_engine(
            url,
            fast_executemany=True,
            pool_pre_ping=True,
            pool_size=5,
            pool_recycle=3600,
        )
    return _engine


# ============================================================================
# Session Context Manager
# ============================================================================
@contextmanager
def get_session():
    """
    Context manager for database sessions.

    Usage:
        with get_session() as session:
            result = session.execute(query)
    """
    engine = get_engine()
    session = Session(engine)
    try:
        yield session
        session.commit()
    except Exception:
        session.rollback()
        logger.exception("Database session error")
        raise
    finally:
        session.close()


# ============================================================================
# Database Initialization
# ============================================================================
def init_db() -> None:
    """Initialize database by creating all tables."""
    engine = get_engine()
    Base.metadata.drop_all(engine)
    Base.metadata.create_all(engine)


# ============================================================================
# Main Entry Point
# ============================================================================
if __name__ == "__main__":
    init_db()
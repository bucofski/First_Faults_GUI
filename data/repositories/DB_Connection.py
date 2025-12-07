from pathlib import Path
from contextlib import contextmanager
from urllib.parse import quote_plus

import yaml
from sqlalchemy import create_engine, Engine
from sqlalchemy.orm import Session, DeclarativeBase

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
def load_db_config(config_path: Path | None = None) -> dict:
    """Load database configuration from YAML file."""
    path = config_path or CONFIG_PATH
    with open(path, "r") as file:
        config = yaml.safe_load(file)
    return config["DBconnection"]


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
        f"Encrypt={'yes' if conn_config.get('encrypt', False) else 'no'}",
        f"TrustServerCertificate={'yes' if conn_config.get('trust_server_certificate', True) else 'no'}",
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
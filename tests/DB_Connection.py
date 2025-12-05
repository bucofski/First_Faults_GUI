
import os
from pathlib import Path
from collections.abc import Callable

import yaml
from sqlalchemy import create_engine, URL, Engine
from sqlalchemy.orm import Session, scoped_session, sessionmaker, DeclarativeBase

# ============================================================================
# Constants
# ============================================================================
LOCAL_DIRECTORY = Path(__file__).parent
CONFIG_PATH = LOCAL_DIRECTORY.parent / "config" / "Connection.yaml"


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
# Database URL Builder
# ============================================================================
def build_database_url(conn_config: dict) -> URL:
    """Build SQLAlchemy connection URL from configuration dictionary."""
    return URL.create(
        "mssql+pyodbc",
        username=conn_config["username"],
        password=conn_config["password"],
        host=conn_config["server"],
        port=conn_config.get("port", 1433),
        database=conn_config["database"],
        query={
            "driver": conn_config["driver"],
            "Encrypt": "yes" if conn_config.get("encrypt", False) else "no",
            "TrustServerCertificate": "yes" if conn_config.get("trust_server_certificate", True) else "no",
            "MARS_Connection": "yes",
        },
    )


# ============================================================================
# Session Factory
# ============================================================================
def create_db_session(
    database_url: URL | None = None,
) -> tuple[scoped_session[Session], Callable[..., None]]:
    """
    Create a scoped database session and cleanup function.

    Args:
        database_url: Optional SQLAlchemy URL. If not provided, loads from config.

    Returns:
        Tuple of (scoped_session, remove_session_function)
    """
    if database_url is None:
        conn_config = load_db_config()
        database_url = build_database_url(conn_config)

    engine = create_engine(
        database_url,
        echo=False,
        pool_pre_ping=True,
        pool_size=5,
        pool_recycle=3600,
        fast_executemany=True,
    )

    db_session = scoped_session(
        sessionmaker(autocommit=False, autoflush=False, bind=engine)
    )

    def remove_session(_exc: BaseException | None = None) -> None:
        db_session.remove()

    return db_session, remove_session


# ============================================================================
# Engine Factory
# ============================================================================
def create_db_engine(database_url: URL | None = None, echo: bool = False) -> Engine:
    """
    Create a SQLAlchemy engine.

    Args:
        database_url: Optional SQLAlchemy URL. If not provided, loads from config.
        echo: Whether to echo SQL statements.

    Returns:
        SQLAlchemy Engine object
    """
    if database_url is None:
        conn_config = load_db_config()
        database_url = build_database_url(conn_config)

    engine = create_engine(
        database_url,
        echo=echo,
        pool_pre_ping=True,
        pool_size=5,
        pool_recycle=3600,
        fast_executemany=True,
    )

    return engine


# ============================================================================
# Database Initialization
# ============================================================================
def init_db(database_url: URL | None = None) -> None:
    """Initialize database by creating all tables."""
    engine = create_db_engine(database_url, echo=True)
    Base.metadata.drop_all(engine)
    Base.metadata.create_all(engine)


# ============================================================================
# Global Engine (Lazy Initialization)
# ============================================================================
_engine: Engine | None = None


def get_engine() -> Engine:
    """Get or create the global engine instance."""
    global _engine
    if _engine is None:
        _engine = create_db_engine()
    return _engine


# ============================================================================
# Main Entry Point
# ============================================================================
if __name__ == "__main__":
    import sys

    if len(sys.argv) > 1:
        # If a config path is provided, use it
        config_path = Path(sys.argv[1])
        conn_config = load_db_config(config_path)
        url = build_database_url(conn_config)
        init_db(url)
    else:
        init_db()
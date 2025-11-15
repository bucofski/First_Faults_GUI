from __future__ import annotations

from functools import lru_cache
from pathlib import Path

import yaml
from sqlalchemy import create_engine
from sqlalchemy.engine import Engine


CONFIG_PATH = Path(__file__).resolve().parent.parent / "config" / "Connection.yaml"


def _load_db_config() -> dict:
    with CONFIG_PATH.open("r", encoding="utf-8") as f:
        raw = yaml.safe_load(f) or {}
    cfg = raw.get("database", {})

    required_keys = ["driver", "host", "port", "database", "username", "password", "odbc_driver"]
    missing = [k for k in required_keys if k not in cfg]
    if missing:
        raise RuntimeError(f"Missing database config keys in Connection.yaml: {', '.join(missing)}")

    return cfg


@lru_cache(maxsize=1)
def get_engine() -> Engine:
    """
    Maak een gedeelde SQLAlchemy Engine op basis van Connection.yaml.
    SQLAlchemy Core, geen ORM.
    """
    cfg = _load_db_config()

    driver = cfg["driver"]  # bv. "mssql+pyodbc"
    host = cfg["host"]
    port = cfg["port"]
    db = cfg["database"]
    user = cfg["username"]
    password = cfg["password"]
    odbc_driver = cfg["odbc_driver"]

    # pyodbc-style connection string
    connection_string = (
        f"{driver}://{user}:{password}"
        f"@{host}:{port}/{db}"
        f"?driver={odbc_driver.replace(' ', '+')}"
    )

    # echo=True als je SQL logging wilt
    engine = create_engine(connection_string, echo=False, future=True)
    return engine
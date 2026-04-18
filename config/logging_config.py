"""Centralised logging configuration.

Four rotating log files:
  logs/app.log         — general application errors (config, startup, etc.)
  logs/repository.log  — errors from data.repositories.*
  logs/flask.log       — errors from Flask / werkzeug / presentation layer
  logs/auth.log        — credential / login events
"""

import logging
from logging.handlers import RotatingFileHandler
from pathlib import Path

LOG_DIR = Path(__file__).resolve().parent.parent / "logs"
LOG_DIR.mkdir(exist_ok=True)

_MAX_BYTES = 5 * 1024 * 1024  # 5 MB per file
_BACKUP_COUNT = 3
_FORMAT = "%(asctime)s  %(levelname)-8s  %(name)s  %(message)s"
_DATE_FMT = "%Y-%m-%d %H:%M:%S"


def _make_handler(filename: str, level: int = logging.ERROR) -> RotatingFileHandler:
    handler = RotatingFileHandler(
        LOG_DIR / filename,
        maxBytes=_MAX_BYTES,
        backupCount=_BACKUP_COUNT,
        encoding="utf-8",
    )
    handler.setLevel(level)
    handler.setFormatter(logging.Formatter(_FORMAT, datefmt=_DATE_FMT))
    return handler


def setup_logging() -> None:
    """Call once at application startup."""

    # 0) General app errors → logs/app.log (root-level catch-all)
    root_logger = logging.getLogger()
    root_logger.setLevel(logging.WARNING)
    root_logger.addHandler(_make_handler("app.log", logging.WARNING))

    # 1) Repository errors → logs/repository.log
    repo_logger = logging.getLogger("data.repositories")
    repo_logger.setLevel(logging.WARNING)
    repo_logger.addHandler(_make_handler("repository.log", logging.WARNING))

    # 2) Flask / presentation errors → logs/flask.log
    flask_handler = _make_handler("flask.log", logging.WARNING)
    for name in ("flask.app", "werkzeug", "presentations"):
        logger = logging.getLogger(name)
        logger.setLevel(logging.WARNING)
        logger.addHandler(flask_handler)

    # 3) Auth / credential events → logs/auth.log  (INFO level to capture logins)
    auth_logger = logging.getLogger("auth")
    auth_logger.setLevel(logging.INFO)
    auth_logger.addHandler(_make_handler("auth.log", logging.INFO))

"""Logging utilities."""

from __future__ import annotations

import logging
from pathlib import Path
from typing import Optional


LOG_FORMAT = "%(asctime)s | %(levelname)-8s | %(name)s | %(message)s"
DEFAULT_LEVEL = logging.INFO


def setup_logging(level: int = DEFAULT_LEVEL, log_file: Optional[Path] = None) -> None:
    """Configure root logging with optional file handler."""

    logging.basicConfig(level=level, format=LOG_FORMAT)
    if log_file:
        log_file.parent.mkdir(parents=True, exist_ok=True)
        file_handler = logging.FileHandler(log_file, encoding="utf-8")
        file_handler.setLevel(level)
        file_handler.setFormatter(logging.Formatter(LOG_FORMAT))
        logging.getLogger().addHandler(file_handler)


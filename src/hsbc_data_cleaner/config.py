"""Configuration helpers for the HSBC PDF cleaning toolkit."""

from __future__ import annotations

import os
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, Optional

try:  # Python 3.11+
    import tomllib  # type: ignore[attr-defined]
except ModuleNotFoundError:  # pragma: no cover - Python <3.11 fallback
    import tomli as tomllib  # type: ignore[no-redef]


_QUARTER_PATTERN = re.compile(r"^(?P<year>\d{4})[- ]?Q(?P<q>[1-4])$")
_CONFIG_ENV_VAR = "HSBC_DATA_CLEANER_CONFIG"


@dataclass
class AppConfig:
    """Resolved configuration for the cleaning pipeline."""

    raw_dir: Path = Path("raw") / "pdf"
    clean_pdf_dir: Path = Path("clean") / "pdf"
    clean_chunks_dir: Path = Path("clean") / "chunks"
    structured_dir: Path = Path("outputs") / "structured"
    state_dir: Path = Path("state")
    log_dir: Path = Path("logs")
    drive_folder_id: Optional[str] = None

    def __post_init__(self) -> None:
        self.raw_dir = Path(self.raw_dir)
        self.clean_pdf_dir = Path(self.clean_pdf_dir)
        self.clean_chunks_dir = Path(self.clean_chunks_dir)
        self.structured_dir = Path(self.structured_dir)
        self.state_dir = Path(self.state_dir)
        self.log_dir = Path(self.log_dir)
        if self.drive_folder_id:
            self.drive_folder_id = self.drive_folder_id.strip() or None

    @staticmethod
    def normalize_quarter(quarter: str) -> str:
        match = _QUARTER_PATTERN.match(quarter.strip())
        if not match:
            raise ValueError(
                "Quarter must be in the format YYYYQ#, e.g. 2025Q2 (case-insensitive)."
            )
        year = match.group("year")
        q = match.group("q")
        return f"{year}Q{q}"

    @staticmethod
    def quarter_folder_name(quarter: str) -> str:
        norm = AppConfig.normalize_quarter(quarter)
        return f"{norm[:4]}-Q{norm[-1]}"

    def resolve_input_dir(self, quarter: str, override: Optional[Path] = None) -> Path:
        base = Path(override) if override else self.raw_dir
        return base / self.quarter_folder_name(quarter)

    def resolve_clean_chunks_dir(self, quarter: str, override: Optional[Path] = None) -> Path:
        base = Path(override) if override else self.clean_chunks_dir
        return base / self.quarter_folder_name(quarter)

    def resolve_clean_pdf_dir(self, quarter: str, override: Optional[Path] = None) -> Path:
        base = Path(override) if override else self.clean_pdf_dir
        return base / self.quarter_folder_name(quarter)

    def resolve_structured_dir(self, quarter: str, override: Optional[Path] = None) -> Path:
        base = Path(override) if override else self.structured_dir
        return base / self.quarter_folder_name(quarter)


def load_app_config(config_path: Optional[Path] = None) -> AppConfig:
    """Load configuration from defaults, optional TOML, and environment variables."""

    config_candidates = []
    if config_path:
        config_candidates.append(Path(config_path))
    elif env_config := os.getenv(_CONFIG_ENV_VAR):
        config_candidates.append(Path(env_config))
    else:
        config_candidates.extend(
            [
                Path("hsbc_data_cleaner.toml"),
                Path("config") / "hsbc_data_cleaner.toml",
            ]
        )

    data: Dict[str, Any] = {}

    for candidate in config_candidates:
        if candidate.is_file():
            data.update(_load_toml(candidate))
            break

    data.update(_env_overrides())

    return AppConfig(**data)


def _env_overrides() -> Dict[str, Any]:
    mapping = {
        "raw_dir": os.getenv("HSBC_RAW_DIR"),
        "clean_pdf_dir": os.getenv("HSBC_CLEAN_PDF_DIR"),
        "clean_chunks_dir": os.getenv("HSBC_CLEAN_CHUNKS_DIR"),
        "structured_dir": os.getenv("HSBC_STRUCTURED_DIR"),
        "state_dir": os.getenv("HSBC_STATE_DIR"),
        "log_dir": os.getenv("HSBC_LOG_DIR"),
        "drive_folder_id": os.getenv("HSBC_DRIVE_FOLDER_ID"),
    }
    return {k: v for k, v in mapping.items() if v}


def _load_toml(path: Path) -> Dict[str, Any]:
    with path.open("rb") as handle:
        content = tomllib.load(handle)
    section = content.get("hsbc_data_cleaner", content)
    return {k: v for k, v in section.items() if v is not None}

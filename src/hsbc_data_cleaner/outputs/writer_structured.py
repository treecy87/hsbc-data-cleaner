"""Writers for structured outputs (CSV/JSON)."""

from __future__ import annotations

import csv
from pathlib import Path
from typing import Dict, Iterable


def append_top_holdings_companies(
    companies: Iterable[str],
    quarter: str,
    base_dir: Path,
) -> None:
    """Maintain a global unique list of top-holding companies."""

    base_dir = Path(base_dir)
    base_dir.mkdir(parents=True, exist_ok=True)
    csv_path = base_dir / "top_holdings_companies.csv"
    _append_unique_strings(csv_path, "company_name", companies)


def append_top_holdings_fixed_income(
    holdings: Iterable[str],
    quarter: str,
    base_dir: Path,
) -> None:
    """Maintain a global unique list of fixed-income holdings."""

    base_dir = Path(base_dir)
    base_dir.mkdir(parents=True, exist_ok=True)
    csv_path = base_dir / "top_holdings_bonds.csv"
    _append_unique_strings(csv_path, "security_name", holdings)


def _append_unique_strings(csv_path: Path, header: str, values: Iterable[str]) -> None:
    existing: Dict[str, str] = {}
    if csv_path.exists():
        with csv_path.open("r", newline="", encoding="utf-8") as handle:
            reader = csv.DictReader(handle)
            for row in reader:
                value = (row.get(header) or "").strip()
                if value:
                    existing[value.lower()] = value

    updated = False
    for value in values:
        normalized = value.strip()
        if not normalized:
            continue
        key = normalized.lower()
        if key in existing:
            continue
        existing[key] = normalized
        updated = True

    if not updated:
        return

    sorted_names = sorted(existing.values(), key=lambda x: x.lower())
    with csv_path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.writer(handle)
        writer.writerow([header])
        for item in sorted_names:
            writer.writerow([item])

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

    existing: Dict[str, str] = {}
    if csv_path.exists():
        with csv_path.open("r", newline="", encoding="utf-8") as handle:
            reader = csv.DictReader(handle)
            for row in reader:
                name = (row.get("company_name") or "").strip()
                if name:
                    existing[name.lower()] = name

    updated = False
    for company in companies:
        normalized = company.strip()
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
        writer.writerow(["company_name"])
        for name in sorted_names:
            writer.writerow([name])

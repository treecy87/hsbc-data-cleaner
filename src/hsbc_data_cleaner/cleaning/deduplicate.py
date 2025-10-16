"""Section fingerprinting and deduplication helpers."""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Tuple

from ..parsers.pdf_parser import PdfSection


_DEFAULT_INDEX_PATH = Path("state") / "chunk_index.json"


@dataclass
class SectionHashResult:
    key: str
    name: str
    current_hash: str
    status: str  # new | updated | reuse
    previous_hash: Optional[str]


def compute_hash(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def load_index(path: Path) -> Dict[str, Dict[str, Dict[str, Dict[str, str]]]]:
    if not path.exists():
        return {}
    with path.open("r", encoding="utf-8") as handle:
        return json.load(handle)


def save_index(data: Dict, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as handle:
        json.dump(data, handle, ensure_ascii=False, indent=2)


def evaluate_sections(
    fund_id: str,
    quarter: str,
    sections: Iterable[PdfSection],
    index_path: Path = _DEFAULT_INDEX_PATH,
) -> List[SectionHashResult]:
    index = load_index(index_path)
    fund_entry = index.setdefault(fund_id, {})

    existing_current_map = fund_entry.get(quarter)
    previous_quarter, previous_map = _find_previous_quarter(fund_entry, quarter)

    results: List[SectionHashResult] = []
    new_current_map: Dict[str, Dict[str, str]] = {}

    for idx, section in enumerate(sections):
        key = f"{section.name}:{idx}"
        current_hash = compute_hash(section.text)
        previous_hash = None
        status = "new"

        # Prefer comparing with existing data of the same quarter (re-run scenario)
        if existing_current_map and key in existing_current_map:
            previous_hash = existing_current_map[key].get("hash")
            if previous_hash == current_hash:
                status = "reuse"
            else:
                status = "updated"
        elif previous_map:
            prev_entry = previous_map.get(key)
            if prev_entry:
                previous_hash = prev_entry.get("hash")
                if previous_hash == current_hash:
                    status = "reuse"
                else:
                    status = "updated"

        new_current_map[key] = {"hash": current_hash, "section": section.name}
        results.append(
            SectionHashResult(
                key=key,
                name=section.name,
                current_hash=current_hash,
                status=status,
                previous_hash=previous_hash,
            )
        )

    fund_entry[quarter] = new_current_map
    save_index(index, index_path)
    return results


def _find_previous_quarter(
    fund_entry: Dict[str, Dict[str, Dict[str, str]]],
    quarter: str,
) -> Tuple[Optional[str], Optional[Dict[str, Dict[str, str]]]]:
    available = [q for q in fund_entry.keys() if q != quarter]
    if not available:
        return None, None
    try:
        sorted_quarters = sorted(available)
    except TypeError:
        sorted_quarters = available
    prev_quarter = sorted_quarters[-1]
    return prev_quarter, fund_entry.get(prev_quarter)

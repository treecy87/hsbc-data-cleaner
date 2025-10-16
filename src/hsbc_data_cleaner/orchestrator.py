"""High-level orchestration entry points."""

from __future__ import annotations

import json
import logging
import re
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List, Optional, Sequence

from . import __version__
from .chunking.chunker import chunk_section_text, generate_change_summary
from .cleaning.deduplicate import SectionHashResult, compute_hash, evaluate_sections
from .config import AppConfig
from .outputs.writer_structured import (
    append_top_holdings_companies,
    append_top_holdings_fixed_income,
)
from .parsers.pdf_parser import (
    PdfSection,
    extract_top_holdings_entries,
    parse_pdf_sections,
)
from .preprocessing.english_filter import remove_english_pages

LOGGER = logging.getLogger(__name__)


@dataclass(frozen=True)
class FundMetadata:
    code: str
    name: str


def run_cleaning(
    settings: AppConfig,
    quarter: str,
    fund_code: Optional[str] = None,
    input_dir: Optional[Path] = None,
    chunks_dir: Optional[Path] = None,
    incremental: bool = True,
    upload: bool = False,
) -> None:
    """Placeholder cleaning pipeline for a given quarter.

    The real implementation will plug in preprocessing, parsing, and output modules.
    """

    resolved_input = settings.resolve_input_dir(quarter, input_dir)
    resolved_chunks = settings.resolve_clean_chunks_dir(quarter, chunks_dir)

    LOGGER.info(
        "Starting cleaning run | quarter=%s fund=%s incremental=%s",
        quarter,
        fund_code or "<all>",
        incremental,
    )
    LOGGER.debug("Using input directory: %s", resolved_input)
    LOGGER.debug("Using chunk output directory: %s", resolved_chunks)

    # Placeholder: ensure directories exist for future stages.
    resolved_chunks.mkdir(parents=True, exist_ok=True)
    settings.state_dir.mkdir(parents=True, exist_ok=True)
    settings.log_dir.mkdir(parents=True, exist_ok=True)

    # Step 1: remove English pages (output currently written back to clean/pdf dir)
    clean_pdf_dir = settings.resolve_clean_pdf_dir(quarter)
    clean_pdf_dir.mkdir(parents=True, exist_ok=True)

    if not resolved_input.exists():
        LOGGER.warning("Input directory %s does not exist; nothing to process.", resolved_input)
    else:
        equity_companies: List[str] = []
        fixed_income_holdings: List[str] = []
        for input_pdf in sorted(resolved_input.glob("*.pdf")):
            output_pdf = clean_pdf_dir / input_pdf.name
            filter_result = remove_english_pages(input_pdf, output_pdf)
            LOGGER.info(
                "Filtered %s: kept %s/%s page(s) (removed %s)",
                input_pdf.name,
                filter_result.kept_count,
                filter_result.total_pages,
                filter_result.removed_count,
            )

            cleaned_pdf = output_pdf if output_pdf.exists() else input_pdf
            sections = parse_pdf_sections(cleaned_pdf)
            LOGGER.info(
                "Parsed %s into %s section(s): %s",
                input_pdf.name,
                len(sections.sections),
                [section.name for section in sections.sections],
            )

            fund_meta = _derive_fund_metadata(input_pdf, fund_code)
            chunk_index_path = settings.state_dir / "chunk_index.json"
            dedupe_results = evaluate_sections(
                fund_id=fund_meta.code,
                quarter=quarter,
                sections=sections.sections,
                index_path=chunk_index_path,
            )

            status_counts = {"new": 0, "updated": 0, "reuse": 0}
            for item in dedupe_results:
                status_counts[item.status] = status_counts.get(item.status, 0) + 1
            LOGGER.info("Section dedupe for %s: %s", fund_meta.code, status_counts)

            _emit_chunks(
                sections=sections.sections,
                dedupe_results=dedupe_results,
                quarter=quarter,
                chunks_dir=settings.resolve_clean_chunks_dir(quarter, chunks_dir),
                fund_metadata=fund_meta,
                file_timestamp=_format_file_timestamp(input_pdf),
                data_date=None,
                language=_infer_language(sections.sections),
            )

            for section in sections.sections:
                if section.name == "top_holdings":
                    entries = extract_top_holdings_entries(section)
                    for entry in entries:
                        if entry.instrument_type == "fixed_income":
                            fixed_income_holdings.append(entry.name)
                        else:
                            equity_companies.append(entry.name)

        if equity_companies:
            append_top_holdings_companies(
                companies=equity_companies,
                quarter=quarter,
                base_dir=settings.structured_dir,
            )
            LOGGER.info(
                "Recorded %s unique equity top-holding companies for %s",
                len(set(equity_companies)),
                quarter,
            )
        if fixed_income_holdings:
            append_top_holdings_fixed_income(
                holdings=fixed_income_holdings,
                quarter=quarter,
                base_dir=settings.structured_dir,
            )
            LOGGER.info(
                "Recorded %s unique fixed-income holdings for %s",
                len(set(fixed_income_holdings)),
                quarter,
            )

    LOGGER.warning("Remaining pipeline modules not yet implemented beyond English-page filtering.")

    if upload:
        LOGGER.info(
            "Upload flag set; would trigger upload after cleaning in future iterations."
        )


def _emit_chunks(
    *,
    sections: Sequence[PdfSection],
    dedupe_results: List[SectionHashResult],
    quarter: str,
    chunks_dir: Path,
    fund_metadata: FundMetadata,
    file_timestamp: str,
    data_date: Optional[str],
    language: str,
    chunk_size: int = 600,
    overlap: int = 80,
) -> None:
    chunks_dir.mkdir(parents=True, exist_ok=True)
    timestamp = datetime.now().strftime("%Y%m%dT%H%M%S%f")
    filename = f"{fund_metadata.name}_{fund_metadata.code}_{quarter}_{timestamp}.json"
    output_path = chunks_dir / filename

    status_map = {result.key: result for result in dedupe_results}

    entries = []

    for idx, section in enumerate(sections):
        key = f"{section.name}:{idx}"
        result = status_map.get(key)
        summary = None
        if result:
            summary = generate_change_summary(
                section_name=section.name,
                status=result.status,
                previous_hash=result.previous_hash,
                current_hash=result.current_hash,
            )

        page_range = _format_page_range(section.pages)
        summary_entry_base = _base_metadata_entry(
            fund_metadata=fund_metadata,
            quarter=quarter,
            file_timestamp=file_timestamp,
            data_date=data_date,
            language=language,
            section_name=section.name,
            page_range=page_range,
            previous_hash=result.previous_hash if result else None,
            section_hash=result.current_hash if result else None,
        )

        chunks = chunk_section_text(
            section.name,
            section.text,
            chunk_size=chunk_size,
            overlap=overlap,
        )

        if summary:
            summary_entry = {
                **summary_entry_base,
                "type": "summary",
                "text": summary,
                "change_type": result.status if result else "unknown",
                "chunk_index": None,
                "chunk_hash": compute_hash(summary),
                "start_offset": None,
                "end_offset": None,
                "structured_refs": [],
            }
            entries.append(
                summary_entry
            )

        for chunk in chunks:
            chunk_entry = {
                **summary_entry_base,
                "type": "chunk",
                "chunk_index": chunk.index,
                "chunk_hash": compute_hash(chunk.text),
                "change_type": result.status if result else "unknown",
                "text": chunk.text,
                "start_offset": chunk.start_offset,
                "end_offset": chunk.end_offset,
                "structured_refs": [],
            }
            entries.append(
                chunk_entry
            )

    with output_path.open("w", encoding="utf-8") as handle:
        json.dump(entries, handle, ensure_ascii=False, indent=2)

    LOGGER.info("Wrote %s chunk entries to %s", len(entries), output_path.name)



def upload_chunks(
    settings: AppConfig,
    quarter: str,
    chunks_dir: Optional[Path] = None,
    drive_folder_id: Optional[str] = None,
) -> None:
    """Placeholder upload operation."""

    resolved_chunks = settings.resolve_clean_chunks_dir(quarter, chunks_dir)
    target_folder = drive_folder_id or settings.drive_folder_id

    LOGGER.info("Preparing to upload chunks for %s from %s", quarter, resolved_chunks)
    if not target_folder:
        LOGGER.warning("No Drive folder ID configured; skipping upload.")
        return

    if not resolved_chunks.exists():
        LOGGER.warning("Chunk directory %s does not exist; nothing to upload.", resolved_chunks)
        return

    LOGGER.warning(
        "Upload logic not yet implemented. Would upload files in %s to Drive folder %s.",
        resolved_chunks,
        target_folder,
    )


def _derive_fund_metadata(pdf_path: Path, override_code: Optional[str]) -> FundMetadata:
    stem = pdf_path.stem
    name_candidate, code_candidate = _split_name_code(stem)
    code = (override_code or code_candidate or stem).strip()
    name = name_candidate.strip() or stem
    return FundMetadata(code=code, name=name)


def _split_name_code(stem: str) -> tuple[str, str]:
    parts = stem.rsplit("_", 1)
    if len(parts) == 2 and re.fullmatch(r"[A-Za-z0-9\-]+", parts[1]):
        return parts[0], parts[1]
    return stem, stem


def _format_file_timestamp(pdf_path: Path) -> str:
    try:
        mtime = pdf_path.stat().st_mtime
    except OSError:
        return datetime.now(timezone.utc).isoformat()
    return datetime.fromtimestamp(mtime, timezone.utc).isoformat()


def _format_page_range(pages: Sequence[int]) -> Optional[str]:
    if not pages:
        return None
    start = min(pages)
    end = max(pages)
    if start == end:
        return str(start)
    return f"{start}-{end}"


def _base_metadata_entry(
    *,
    fund_metadata: FundMetadata,
    quarter: str,
    file_timestamp: str,
    data_date: Optional[str],
    language: str,
    section_name: str,
    page_range: Optional[str],
    previous_hash: Optional[str],
    section_hash: Optional[str],
) -> Dict[str, Optional[str]]:
    return {
        "fund_code": fund_metadata.code,
        "fund_name": fund_metadata.name,
        "section": section_name,
        "page_range": page_range,
        "quarter": quarter,
        "data_date": data_date,
        "file_timestamp": file_timestamp,
        "language": language,
        "source_type": "pdf",
        "version": __version__,
        "previous_chunk_hash": previous_hash,
        "section_hash": section_hash,
    }


def _infer_language(sections: Sequence[PdfSection]) -> str:
    total_chars = 0
    ascii_chars = 0
    for section in sections:
        text = section.text
        total_chars += len(text)
        ascii_chars += sum(1 for ch in text if ch.isascii())
    if not total_chars:
        return "unknown"
    ratio = ascii_chars / total_chars
    if ratio > 0.3:
        return "mix"
    return "zh"

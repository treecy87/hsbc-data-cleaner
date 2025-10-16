"""High-level orchestration entry points."""

from __future__ import annotations

import logging
import json
from pathlib import Path
from typing import List, Optional

from .config import AppConfig
from .preprocessing.english_filter import remove_english_pages
from .parsers.pdf_parser import extract_top_holdings_companies, parse_pdf_sections
from .outputs.writer_structured import append_top_holdings_companies
from .cleaning.deduplicate import SectionHashResult, evaluate_sections
from datetime import datetime
from .chunking.chunker import chunk_section_text, generate_change_summary

LOGGER = logging.getLogger(__name__)


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
        all_companies: List[str] = []
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

            fund_id = fund_code or input_pdf.stem
            chunk_index_path = settings.state_dir / "chunk_index.json"
            dedupe_results = evaluate_sections(
                fund_id=fund_id,
                quarter=quarter,
                sections=sections.sections,
                index_path=chunk_index_path,
            )

            status_counts = {"new": 0, "updated": 0, "reuse": 0}
            for item in dedupe_results:
                status_counts[item.status] = status_counts.get(item.status, 0) + 1
            LOGGER.info("Section dedupe for %s: %s", fund_id, status_counts)

            _emit_chunks(
                sections=sections.sections,
                dedupe_results=dedupe_results,
                quarter=quarter,
                chunks_dir=settings.resolve_clean_chunks_dir(quarter, chunks_dir),
                fund_id=fund_id,
            )

            for section in sections.sections:
                if section.name == "top_holdings":
                    companies = extract_top_holdings_companies(section)
                    all_companies.extend(companies)

        if all_companies:
            append_top_holdings_companies(
                companies=all_companies,
                quarter=quarter,
                base_dir=settings.structured_dir,
            )
            LOGGER.info(
                "Recorded %s unique top-holding companies for %s", len(set(all_companies)), quarter
            )

    LOGGER.warning("Remaining pipeline modules not yet implemented beyond English-page filtering.")

    if upload:
        LOGGER.info(
            "Upload flag set; would trigger upload after cleaning in future iterations."
        )


def _emit_chunks(
    *,
    sections: List,
    dedupe_results: List[SectionHashResult],
    quarter: str,
    chunks_dir: Path,
    fund_id: str,
    chunk_size: int = 600,
    overlap: int = 80,
) -> None:
    chunks_dir.mkdir(parents=True, exist_ok=True)
    timestamp = datetime.now().strftime("%Y%m%dT%H%M%S")
    filename = f"{fund_id}_{quarter}_{timestamp}.json"
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

        chunks = chunk_section_text(
            section.name,
            section.text,
            chunk_size=chunk_size,
            overlap=overlap,
        )

        if summary:
            entries.append(
                {
                    "type": "summary",
                    "section": section.name,
                    "summary": summary,
                    "status": result.status if result else "unknown",
                }
            )

        for chunk in chunks:
            entries.append(
                {
                    "type": "chunk",
                    "section": chunk.section,
                    "index": chunk.index,
                    "text": chunk.text,
                    "start_offset": chunk.start_offset,
                    "end_offset": chunk.end_offset,
                }
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

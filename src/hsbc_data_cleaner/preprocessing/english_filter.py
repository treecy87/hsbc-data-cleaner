"""Utilities for removing primarily English pages from PDFs."""

from __future__ import annotations

import logging
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable, List, Optional, Sequence

from pypdf import PdfReader, PdfWriter

LOGGER = logging.getLogger(__name__)


@dataclass
class EnglishFilterResult:
    """Summary information from an English-page filtering run."""

    input_path: Path
    output_path: Optional[Path]
    kept_pages: Sequence[int]
    removed_pages: Sequence[int]
    total_pages: int

    @property
    def removed_count(self) -> int:
        return len(self.removed_pages)

    @property
    def kept_count(self) -> int:
        return len(self.kept_pages)


def remove_english_pages(
    pdf_path: Path,
    output_path: Optional[Path] = None,
    *,
    chinese_threshold: int = 10,
    ascii_ratio_threshold: float = 0.8,
) -> EnglishFilterResult:
    """Remove pages that appear to be primarily English text.

    Args:
        pdf_path: Source PDF file.
        output_path: Destination for filtered PDF. If None, file is not written.
        chinese_threshold: Minimum number of CJK characters required to consider
            a page as Chinese-dominant.
        ascii_ratio_threshold: When Chinese characters are below threshold,
            classify as English if ASCII letters make up at least this ratio of
            total letters. Value should be between 0 and 1.
    """

    pdf_path = Path(pdf_path)
    reader = PdfReader(str(pdf_path))

    kept_pages: List[int] = []
    removed_pages: List[int] = []

    for page_index, page in enumerate(reader.pages, start=1):
        try:
            text = page.extract_text() or ""
        except Exception:  # pragma: no cover - pypdf best-effort extraction
            text = ""

        if _is_chinese_dominant(text, chinese_threshold, ascii_ratio_threshold):
            kept_pages.append(page_index)
        else:
            removed_pages.append(page_index)

    if output_path and kept_pages:
        writer = PdfWriter()
        for index in kept_pages:
            writer.add_page(reader.pages[index - 1])

        output_path = Path(output_path)
        output_path.parent.mkdir(parents=True, exist_ok=True)
        with output_path.open("wb") as handle:
            writer.write(handle)
        LOGGER.info(
            "Wrote filtered PDF without %s English page(s) to %s",
            len(removed_pages),
            output_path,
        )
    elif output_path:
        LOGGER.warning(
            "All pages classified as English in %s; no file written.", pdf_path.name
        )

    if removed_pages:
        LOGGER.debug(
            "Removed English pages %s from %s", list(removed_pages), pdf_path.name
        )

    return EnglishFilterResult(
        input_path=pdf_path,
        output_path=Path(output_path) if output_path else None,
        kept_pages=kept_pages,
        removed_pages=removed_pages,
        total_pages=len(reader.pages),
    )


def _is_chinese_dominant(
    text: str,
    chinese_threshold: int,
    ascii_ratio_threshold: float,
) -> bool:
    chinese = sum(1 for ch in text if _is_cjk(ch))
    ascii_letters = sum(1 for ch in text if ch.isascii() and ch.isalpha())

    if chinese >= chinese_threshold:
        return True

    total_letters = chinese + ascii_letters
    if total_letters == 0:
        # Empty or symbols-only page; treat as English to allow removal.
        return False

    ascii_ratio = ascii_letters / total_letters
    return ascii_ratio < ascii_ratio_threshold


def _is_cjk(char: str) -> bool:
    return "\u4e00" <= char <= "\u9fff"


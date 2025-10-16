"""Section parsing utilities for cleaned HSBC PDF documents."""

from __future__ import annotations

import logging
import re
from dataclasses import dataclass, field
from pathlib import Path
from typing import Iterable, List, Optional, Sequence

from pypdf import PdfReader

LOGGER = logging.getLogger(__name__)

from ..cleaning.normalizers import normalize_lines


@dataclass(frozen=True)
class SectionDefinition:
    """Definition of a section heading (supports multilingual patterns)."""

    name: str
    patterns: Sequence[re.Pattern[str]]


DEFAULT_SECTION_DEFINITIONS: Sequence[SectionDefinition] = [
    SectionDefinition(
        name="important_information",
        patterns=[
            re.compile(r"important\s+information", re.IGNORECASE),
            re.compile(r"重要事項"),
        ],
    ),
    SectionDefinition(
        name="top_holdings",
        patterns=[
            re.compile(r"top\s+10\s+holdings", re.IGNORECASE),
            re.compile(r"十大持股"),
            re.compile(r"十大投資項目"),
        ],
    ),
    SectionDefinition(
        name="performance",
        patterns=[
            re.compile(r"calendar\s+year\s+returns", re.IGNORECASE),
            re.compile(r"年度回報"),
            re.compile(r"累積回報"),
        ],
    ),
    SectionDefinition(
        name="product_summary",
        patterns=[
            re.compile(r"product\s+key\s+facts", re.IGNORECASE),
            re.compile(r"產品資料概要"),
        ],
    ),
    SectionDefinition(
        name="objective_strategy",
        patterns=[
            re.compile(r"objective[s]?\s+and\s+investment\s+strategy", re.IGNORECASE),
            re.compile(r"目標及投資策略"),
        ],
    ),
    SectionDefinition(
        name="fees_charges",
        patterns=[
            re.compile(r"fees?\s+and\s+charges", re.IGNORECASE),
            re.compile(r"費用"),
            re.compile(r"費用及開支"),
        ],
    ),
    SectionDefinition(
        name="other_information",
        patterns=[
            re.compile(r"other\s+information", re.IGNORECASE),
            re.compile(r"其他資料"),
        ],
    ),
]

SECTOR_NAMES = [
    "Information Technology",
    "Communication Services",
    "Consumer Discretionary",
    "Consumer Staples",
    "Health Care",
    "Financials",
    "Industrials",
    "Materials",
    "Utilities",
    "Real Estate",
    "Energy",
    "Other",
    "Cash",
    "Cash & Derivatives",
]

_SECTOR_PATTERN = re.compile(
    r"^(?P<name>[A-Za-z0-9.,'&()/ -]+?)\s+(?P<sector>" + "|".join(re.escape(s) for s in SECTOR_NAMES) + r")\b",
    re.IGNORECASE,
)

_CHINESE_HOLDING_PATTERN = re.compile(
    r"^(?P<name>[A-Za-z0-9.,'&()/\- ]+?)\s+\S+\s+\S+\s+[0-9]+(?:\.[0-9]+)?$"
)


@dataclass
class PdfSection:
    """Parsed section content with metadata."""

    name: str
    title: str
    pages: List[int] = field(default_factory=list)
    lines: List[str] = field(default_factory=list)

    @property
    def text(self) -> str:
        return "\n".join(self.lines).strip()


@dataclass
class ParseResult:
    input_path: Path
    sections: List[PdfSection]
    total_pages: int

    def as_dict(self) -> dict:
        return {
            "input_path": str(self.input_path),
            "total_pages": self.total_pages,
            "sections": [
                {
                    "name": section.name,
                    "title": section.title,
                    "pages": section.pages,
                    "text": section.text,
                }
                for section in self.sections
            ],
        }


def parse_pdf_sections(
    pdf_path: Path,
    section_definitions: Sequence[SectionDefinition] = DEFAULT_SECTION_DEFINITIONS,
) -> ParseResult:
    """Parse a cleaned PDF into coarse sections."""

    pdf_path = Path(pdf_path)
    reader = PdfReader(str(pdf_path))

    sections: List[PdfSection] = []
    current_section = PdfSection(name="document_intro", title="Document Introduction")

    for page_index, page in enumerate(reader.pages, start=1):
        try:
            page_text = page.extract_text() or ""
        except Exception:  # pragma: no cover - best-effort extraction
            page_text = ""

        lines = normalize_lines(page_text.splitlines())
        for line in lines:
            matched_def = _match_section(line, section_definitions)
            if matched_def:
                if current_section.lines:
                    sections.append(current_section)
                current_section = PdfSection(
                    name=matched_def.name,
                    title=line.strip(),
                    pages=[page_index],
                    lines=[],
                )
                continue

            current_section.lines.append(line)
            if page_index not in current_section.pages:
                current_section.pages.append(page_index)

    if current_section.lines or current_section.name != "document_intro":
        sections.append(current_section)

    LOGGER.debug(
        "Parsed %s into %s section(s): %s",
        pdf_path.name,
        len(sections),
        [section.name for section in sections],
    )

    return ParseResult(
        input_path=pdf_path,
        sections=sections,
        total_pages=len(reader.pages),
    )


def extract_top_holdings_companies(section: PdfSection) -> List[str]:
    """Extract company names from a top holdings section."""

    companies: List[str] = []
    for line in section.lines:
        if not line or line.lower().startswith("sector "):
            continue
        if line.lower().startswith("total") or "合共" in line:
            continue

        match = _SECTOR_PATTERN.match(line)
        if match:
            name = match.group("name").strip()
            if name:
                companies.append(name)
                continue

        match_cn = _CHINESE_HOLDING_PATTERN.match(line)
        if match_cn:
            name = match_cn.group("name").strip()
            if name:
                companies.append(name)

    return companies


def _prepare_lines(lines: Iterable[str]) -> List[str]:
    cleaned = []
    for raw in lines:
        line = raw.strip()
        if not line:
            continue
        cleaned.append(line)
    return cleaned


def _match_section(line: str, definitions: Sequence[SectionDefinition]) -> Optional[SectionDefinition]:
    normalized = line.strip()
    if not normalized:
        return None

    for definition in definitions:
        for pattern in definition.patterns:
            if pattern.search(normalized):
                return definition
    return None

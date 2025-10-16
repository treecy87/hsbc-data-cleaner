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
            re.compile(r"十大持倉"),
            re.compile(r"十大持仓"),
            re.compile(r"股票十大持倉"),
            re.compile(r"固定收益十大持倉"),
            re.compile(r"十+大+.*持+股+"),
            re.compile(r"十+大+.*持+倉+"),
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

_VALUE_AT_END_PATTERN = re.compile(r"([0-9]+(?:\.[0-9]+)?)%?$")
_CHINESE_CHAR_PATTERN = re.compile(r"[\u4e00-\u9fff]")


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


@dataclass(frozen=True)
class TopHoldingEntry:
    """Individual holding extracted from a top-holdings section."""

    name: str
    instrument_type: str  # "equity" or "fixed_income"


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


def extract_top_holdings_entries(section: PdfSection) -> List[TopHoldingEntry]:
    """Extract top holdings entries with instrument classification."""

    entries: List[TopHoldingEntry] = []
    stop_keywords = (
        "投資組合",
        "組合分布",
        "組合分佈",
        "類別分布",
        "類別分佈",
        "股票特點",
        "固定收益特點",
        "行業分佈",
        "行業分布",
        "滙豐集合",
        "月度報告",
        "有關詞彙",
        "重要資訊",
        "關注我們",
    )
    buffer: List[str] = []
    current_type: Optional[str] = _infer_type_from_title(section.title)

    def _flush_buffer() -> None:
        if not buffer:
            return
        candidate = " ".join(buffer).strip()
        candidate = re.sub(r"\s+", " ", candidate)
        match = _VALUE_AT_END_PATTERN.search(candidate)
        if not match or match.end() != len(candidate):
            return
        name_part = candidate[: match.start()].strip()
        if not name_part or "%" in name_part:
            return

        # If the line matches the sector-based pattern, reuse that parsing.
        sector_match = _SECTOR_PATTERN.match(name_part)
        if sector_match:
            name = sector_match.group("name").strip()
        else:
            name = _clean_company_name(name_part)

        if name:
            inferred_type = current_type or _infer_type_from_name(name)
            entries.append(
                TopHoldingEntry(
                    name=name,
                    instrument_type=inferred_type or "equity",
                )
            )

    for line in section.lines:
        text = line.strip()
        if not text:
            continue
        lowered = text.lower()
        if lowered.startswith("sector "):
            continue
        if lowered.startswith("total") or "合共" in text:
            continue
        title_type = _infer_type_from_title(text)
        if title_type:
            _flush_buffer()
            buffer = []
            current_type = title_type
            continue
        if any(keyword in text for keyword in ("查閱", "請掃描")):
            _flush_buffer()
            buffer = []
            continue
        if any(keyword in text for keyword in stop_keywords):
            _flush_buffer()
            buffer = []
            break

        buffer.append(text)
        candidate = " ".join(buffer)
        candidate = re.sub(r"\s+", " ", candidate)
        match = _VALUE_AT_END_PATTERN.search(candidate)
        if match and match.end() == len(candidate):
            _flush_buffer()
            buffer = []

    _flush_buffer()
    return entries


def extract_top_holdings_companies(section: PdfSection) -> List[str]:
    """Legacy helper returning only equity holdings."""

    return [
        entry.name
        for entry in extract_top_holdings_entries(section)
        if entry.instrument_type != "fixed_income"
    ]


def _infer_type_from_title(title: Optional[str]) -> Optional[str]:
    if not title:
        return None
    normalized = title.lower()
    header_tokens = ("持倉", "持仓", "holdings", "holding")
    if not any(token in normalized for token in header_tokens):
        return None
    if any(keyword in normalized for keyword in ("固定收益", "fixed income", "債券", "债券", "bond")):
        return "fixed_income"
    if any(keyword in normalized for keyword in ("股票", "equity")):
        return "equity"
    return None


def _infer_type_from_name(name: str) -> Optional[str]:
    normalized = name.lower()
    bond_tokens = (
        " bond",
        "bond ",
        "bnd ",
        " note",
        "notes",
        "債",
        "债",
        "debenture",
        "bill",
        "treasury",
        "certificate",
    )
    if any(token in normalized for token in bond_tokens):
        return "fixed_income"
    return None


def _contains_cjk(text: str) -> bool:
    return bool(_CHINESE_CHAR_PATTERN.search(text))


def _clean_company_name(raw: str) -> str:
    """Trim trailing region/sector metadata from a holdings string."""

    normalized = re.sub(r"\s+", " ", raw).strip()
    if not normalized:
        return ""

    tokens = normalized.split()
    primary: List[str] = []
    for token in tokens:
        if _contains_cjk(token) and primary:
            break
        primary.append(token)

    cleaned = " ".join(primary).strip()
    return cleaned or normalized


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

"""Text normalization utilities for cleaned HSBC PDF sections."""

from __future__ import annotations

import re
from typing import Iterable, List


WHITESPACE_RE = re.compile(r"\s+")
FULLWIDTH_PUNCTUATION = {
    "，": ",",
    "、": ",",
    "。": ".",
    "：": ":",
    "；": ";",
    "？": "?",
    "！": "!",
    "（": "(",
    "）": ")",
    "％": "%",
}


def normalize_line(text: str) -> str:
    """Normalize a single line of text."""

    if not text:
        return ""

    normalized = text.strip()
    for src, dst in FULLWIDTH_PUNCTUATION.items():
        normalized = normalized.replace(src, dst)
    normalized = WHITESPACE_RE.sub(" ", normalized)
    normalized = _ensure_spacing_after_punctuation(normalized)
    return normalized


def normalize_lines(lines: Iterable[str]) -> List[str]:
    """Normalize an iterable of lines, dropping empty results."""

    result: List[str] = []
    for line in lines:
        normalized = normalize_line(line)
        if normalized:
            result.append(normalized)
    return result


def _ensure_spacing_after_punctuation(text: str) -> str:
    # Ensure common punctuation is followed by a space when letter/number follows.
    for symbol in [",", ";", ":"]:
        text = re.sub(rf"{re.escape(symbol)}(?=\S)", symbol + " ", text)
    return text

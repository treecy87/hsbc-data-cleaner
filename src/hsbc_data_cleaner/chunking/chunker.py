"""Chunk generation and change summary utilities."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Iterable, List, Optional


@dataclass
class TextChunk:
    section: str
    index: int
    text: str
    start_offset: int
    end_offset: int


def chunk_section_text(
    section_name: str,
    text: str,
    *,
    chunk_size: int = 500,
    overlap: int = 80,
) -> List[TextChunk]:
    """Split section text into overlapping chunks."""

    if chunk_size <= 0:
        raise ValueError("chunk_size must be positive")
    if overlap < 0 or overlap >= chunk_size:
        raise ValueError("overlap must be >=0 and < chunk_size")

    clean_text = text.strip()
    if not clean_text:
        return []

    tokens = list(clean_text)
    chunks: List[TextChunk] = []
    start = 0
    index = 0
    step = chunk_size - overlap

    while start < len(tokens):
        end = min(start + chunk_size, len(tokens))
        chunk_text = "".join(tokens[start:end]).strip()
        if chunk_text:
            chunks.append(
                TextChunk(
                    section=section_name,
                    index=index,
                    text=chunk_text,
                    start_offset=start,
                    end_offset=end,
                )
            )
            index += 1
        start += step

    return chunks


def generate_change_summary(
    section_name: str,
    status: str,
    previous_hash: Optional[str],
    current_hash: str,
) -> Optional[str]:
    if status == "reuse":
        return None
    if status == "new":
        return f"Section {section_name} is new in this quarter."
    if status == "updated":
        return (
            f"Section {section_name} changed (prev_hash={previous_hash}, "
            f"new_hash={current_hash})."
        )
    return None


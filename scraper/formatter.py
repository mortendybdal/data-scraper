"""Format extracted text into JSONL files for Unsloth CPT and SFT."""

from __future__ import annotations

import json
import logging
from pathlib import Path

logger = logging.getLogger(__name__)

# Conservative chars-per-token ratio for multilingual (Danish) text.
# Gemma's SentencePiece tokenizer averages ~3 chars/token for non-English.
_CHARS_PER_TOKEN = 3


def chunk_text(text: str, max_tokens: int = 1900) -> list[str]:
    """Split text into chunks of approximately max_tokens.

    Splits on paragraph boundaries first, then sentence boundaries if a
    single paragraph exceeds the limit. Uses ~3 chars/token (safe for Danish).
    Target 1900 tokens to leave headroom for <bos>/<eos> under Gemma's 2048 limit.
    """
    max_chars = max_tokens * _CHARS_PER_TOKEN

    if len(text) <= max_chars:
        return [text]

    paragraphs = text.split("\n\n")
    chunks: list[str] = []
    current: list[str] = []
    current_len = 0

    for para in paragraphs:
        para_len = len(para)

        # If a single paragraph exceeds the limit, split it by sentences
        if para_len > max_chars:
            # Flush current buffer first
            if current:
                chunks.append("\n\n".join(current))
                current = []
                current_len = 0

            sentences = _split_sentences(para)
            sent_buf: list[str] = []
            sent_len = 0
            for sent in sentences:
                if sent_len + len(sent) > max_chars and sent_buf:
                    chunks.append(" ".join(sent_buf))
                    sent_buf = []
                    sent_len = 0
                sent_buf.append(sent)
                sent_len += len(sent) + 1
            if sent_buf:
                chunks.append(" ".join(sent_buf))
            continue

        # Would adding this paragraph exceed the limit?
        separator_len = 2 if current else 0  # "\n\n"
        if current_len + separator_len + para_len > max_chars and current:
            chunks.append("\n\n".join(current))
            current = []
            current_len = 0

        current.append(para)
        current_len += (2 if current_len > 0 else 0) + para_len

    if current:
        chunks.append("\n\n".join(current))

    return [c for c in chunks if len(c.strip()) >= 50]


def _split_sentences(text: str) -> list[str]:
    """Naive sentence splitter on common punctuation."""
    import re

    parts = re.split(r"(?<=[.!?])\s+", text)
    return [p for p in parts if p.strip()]


def save_cpt_jsonl(
    texts: list[dict[str, str]],
    output_path: Path,
) -> Path:
    """Save documents as JSONL in SFT format.

    Each line: {"instruction": "", "input": "", "output": "document content..."}

    Args:
        texts: List of dicts with at least "text" (and optionally "source", "url").
        output_path: Path to the output .jsonl file.

    Returns:
        The path to the written file.
    """
    output_path.parent.mkdir(parents=True, exist_ok=True)

    count = 0
    with open(output_path, "w", encoding="utf-8") as f:
        for doc in texts:
            record = {
                "instruction": "",
                "input": "",
                "output": doc["text"],
            }
            f.write(json.dumps(record, ensure_ascii=False) + "\n")
            count += 1

    logger.info("Wrote %d documents to %s", count, output_path)
    return output_path


def save_cpt_jsonl_with_metadata(
    texts: list[dict[str, str]],
    output_path: Path,
) -> Path:
    """Save documents as JSONL in SFT format with metadata.

    Each line: {"instruction": "", "input": "", "output": "...", "source": "...", "url": "..."}
    """
    output_path.parent.mkdir(parents=True, exist_ok=True)

    count = 0
    with open(output_path, "w", encoding="utf-8") as f:
        for doc in texts:
            record = {
                "instruction": "",
                "input": "",
                "output": doc["text"],
                "source": doc.get("source", ""),
                "url": doc.get("url", ""),
                "chunk": doc.get("chunk", ""),
            }
            f.write(json.dumps(record, ensure_ascii=False) + "\n")
            count += 1

    logger.info("Wrote %d documents (with metadata) to %s", count, output_path)
    return output_path


def load_cpt_jsonl(path: Path) -> list[dict[str, str]]:
    """Load a CPT JSONL file back into memory."""
    docs = []
    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if line:
                docs.append(json.loads(line))
    return docs


def merge_cpt_files(input_paths: list[Path], output_path: Path) -> Path:
    """Merge multiple JSONL files into one (for combining scrapes over time)."""
    seen_texts: set[int] = set()  # hash-based dedup
    output_path.parent.mkdir(parents=True, exist_ok=True)

    count = 0
    with open(output_path, "w", encoding="utf-8") as out:
        for path in input_paths:
            for doc in load_cpt_jsonl(path):
                text = doc.get("output", doc.get("text", ""))
                text_hash = hash(text)
                if text_hash not in seen_texts:
                    seen_texts.add(text_hash)
                    record = {
                        "instruction": "",
                        "input": "",
                        "output": text,
                    }
                    out.write(json.dumps(record, ensure_ascii=False) + "\n")
                    count += 1

    logger.info("Merged %d unique documents into %s", count, output_path)
    return output_path

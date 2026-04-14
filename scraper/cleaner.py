"""Text cleaning utilities for scraped healthcare data.

Two layers:
  1. light_filter(text) — fast inline check used during scraping.
     Returns False if the text is obvious junk (cookie banners, too short).
  2. deep_clean(records) — heavier post-processing on JSONL records.
     Deduplicates, strips repeated boilerplate lines, removes low-quality entries.
"""

from __future__ import annotations

import hashlib
import logging
import re
from collections import Counter

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Patterns for cookie / consent banners (case-insensitive)
# ---------------------------------------------------------------------------
_COOKIE_PATTERNS: list[re.Pattern] = [
    re.compile(p, re.IGNORECASE)
    for p in [
        r"dette websted bruger cookies",
        r"this website uses cookies",
        r"vi bruger cookies",
        r"we use cookies",
        r"nødvendige cookies",
        r"necessary cookies",
        r"cookie.?politik",
        r"cookie.?policy",
        r"samtykke.*cookies",
        r"consent.*cookies",
        r"cookies.*samtykke",
        r"gemmes kun i din browser",
        r"stored in your browser",
        r"ikke-nødvendige cookies",
    ]
]

# ---------------------------------------------------------------------------
# Patterns for navigation / UI boilerplate
# ---------------------------------------------------------------------------
_NAV_PATTERNS: list[re.Pattern] = [
    re.compile(p, re.IGNORECASE)
    for p in [
        r"^videre til indhold$",
        r"^skip to content$",
        r"^gå til indhold$",
        r"^søg$",
        r"^search$",
        r"^menu$",
        r"^log ind$",
        r"^log in$",
        r"^tilmelding$",
    ]
]

# ---------------------------------------------------------------------------
# Patterns for promotional / contact boilerplate
# ---------------------------------------------------------------------------
_PROMO_PATTERNS: list[re.Pattern] = [
    re.compile(p, re.IGNORECASE)
    for p in [
        r"copyright\s*©",
        r"alle rettigheder forbeholdes",
        r"all rights reserved",
        r"powered by",
        r"udviklet af",
    ]
]

# Minimum characters for meaningful medical content
MIN_CONTENT_LENGTH = 200

# Threshold for fuzzy duplicate detection (ratio of shared lines)
FUZZY_DUPLICATE_THRESHOLD = 0.85


# ===================================================================
# Layer 1: Light inline filter (used during scraping)
# ===================================================================


def light_filter(text: str) -> bool:
    """Return True if text should be KEPT, False if it's junk.

    Designed to be fast and conservative — only rejects obvious noise.
    """
    if not text or not text.strip():
        return False

    stripped = text.strip()

    # Too short to be useful medical content
    if len(stripped) < MIN_CONTENT_LENGTH:
        return False

    # Predominantly a cookie/consent banner
    cookie_matches = sum(1 for p in _COOKIE_PATTERNS if p.search(stripped))
    if cookie_matches >= 2:
        return False

    # Single cookie pattern + short text = likely just a banner
    if cookie_matches >= 1 and len(stripped) < 500:
        return False

    return True


# ===================================================================
# Layer 2: Deep cleaning (post-processing on JSONL records)
# ===================================================================


def _text_hash(text: str) -> str:
    """Stable hash for exact dedup."""
    return hashlib.md5(text.encode("utf-8")).hexdigest()


def _text_lines(text: str) -> list[str]:
    """Split text into non-empty lines."""
    return [ln.strip() for ln in text.split("\n") if ln.strip()]


def _find_boilerplate_lines(records: list[dict], threshold: float = 0.3) -> set[str]:
    """Find lines that appear in more than `threshold` fraction of records.

    These are likely shared navigation, headers, or footers.
    Only considers lines that are short (< 100 chars) to avoid removing
    legitimately repeated medical facts.
    """
    total = len(records)
    if total < 5:
        return set()

    line_counts: Counter[str] = Counter()
    for rec in records:
        text = rec.get("output", rec.get("text", ""))
        unique_lines = set(_text_lines(text))
        for line in unique_lines:
            if len(line) < 100:
                line_counts[line] += 1

    min_count = max(3, int(total * threshold))
    boilerplate = {line for line, count in line_counts.items() if count >= min_count}
    return boilerplate


def _is_fuzzy_duplicate(text_a: str, text_b: str) -> bool:
    """Check if two texts are near-duplicates based on shared lines."""
    lines_a = set(_text_lines(text_a))
    lines_b = set(_text_lines(text_b))
    if not lines_a or not lines_b:
        return False
    shared = lines_a & lines_b
    ratio_a = len(shared) / len(lines_a) if lines_a else 0
    ratio_b = len(shared) / len(lines_b) if lines_b else 0
    return max(ratio_a, ratio_b) >= FUZZY_DUPLICATE_THRESHOLD


def _strip_boilerplate_lines(text: str, boilerplate: set[str]) -> str:
    """Remove known boilerplate lines from text while preserving structure."""
    lines = text.split("\n")
    cleaned = []
    for line in lines:
        stripped = line.strip()
        if stripped and stripped in boilerplate:
            continue
        # Also strip lines matching nav/promo patterns
        if stripped and any(p.search(stripped) for p in _NAV_PATTERNS):
            continue
        if stripped and any(p.search(stripped) for p in _PROMO_PATTERNS):
            continue
        cleaned.append(line)

    # Collapse excessive blank lines
    result = re.sub(r"\n{3,}", "\n\n", "\n".join(cleaned)).strip()
    return result


def _strip_cookie_blocks(text: str) -> str:
    """Remove contiguous blocks that look like cookie consent text."""
    paragraphs = text.split("\n\n")
    kept = []
    for para in paragraphs:
        cookie_hits = sum(1 for p in _COOKIE_PATTERNS if p.search(para))
        if cookie_hits >= 1 and len(para) < 800:
            continue
        kept.append(para)
    return "\n\n".join(kept)


def deep_clean(records: list[dict]) -> tuple[list[dict], list[dict]]:
    """Clean a list of JSONL records.

    Returns:
        (kept, rejected) — two lists of records.
    """
    if not records:
        return [], []

    kept: list[dict] = []
    rejected: list[dict] = []

    # --- Step 1: Find boilerplate lines across all records ---
    boilerplate_lines = _find_boilerplate_lines(records)
    if boilerplate_lines:
        logger.info("Found %d boilerplate lines to strip.", len(boilerplate_lines))

    # --- Step 2: Clean each record ---
    seen_hashes: set[str] = set()

    for rec in records:
        text_key = "output" if "output" in rec else "text"
        original_text = rec.get(text_key, "")

        # Strip cookie blocks
        text = _strip_cookie_blocks(original_text)

        # Strip boilerplate lines
        text = _strip_boilerplate_lines(text, boilerplate_lines)

        # Normalize whitespace
        text = re.sub(r"[ \t]+", " ", text)
        text = re.sub(r"\n{3,}", "\n\n", text)
        text = text.strip()

        # Check minimum length after cleaning
        if len(text) < MIN_CONTENT_LENGTH:
            rejected.append({**rec, "_reject_reason": "too_short_after_cleaning"})
            continue

        # Exact dedup
        h = _text_hash(text)
        if h in seen_hashes:
            rejected.append({**rec, "_reject_reason": "exact_duplicate"})
            continue
        seen_hashes.add(h)

        # Update the record with cleaned text
        cleaned_rec = {**rec, text_key: text}
        kept.append(cleaned_rec)

    # --- Step 3: Fuzzy dedup (on kept records) ---
    final_kept: list[dict] = []
    for i, rec in enumerate(kept):
        text_key = "output" if "output" in rec else "text"
        text = rec[text_key]
        is_dup = False
        # Only check against recent records to keep O(n) reasonable
        lookback = min(50, len(final_kept))
        for prev in final_kept[-lookback:]:
            prev_text = prev.get(text_key, "")
            if _is_fuzzy_duplicate(text, prev_text):
                is_dup = True
                break
        if is_dup:
            rejected.append({**rec, "_reject_reason": "fuzzy_duplicate"})
        else:
            final_kept.append(rec)

    return final_kept, rejected

#!/usr/bin/env python3
"""Post-processing cleaner for scraped JSONL files.

Applies deep cleaning: deduplication, boilerplate removal, cookie stripping,
and minimum content length checks.

Writes cleaned output and optionally a rejected sidecar file for auditing.

Usage:
    python scripts/clean.py output/vaccination-dk.jsonl              # clean one file in-place
    python scripts/clean.py output/*.jsonl                           # clean all files
    python scripts/clean.py output/vaccination-dk.jsonl --keep-rejected  # also write .rejected.jsonl
"""

from __future__ import annotations

import argparse
import json
import logging
import sys
from pathlib import Path

# Add project root to path so we can import scraper modules
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from scraper.cleaner import deep_clean


def process_file(
    input_path: Path,
    keep_rejected: bool = False,
) -> tuple[int, int, int]:
    """Clean a single JSONL file in-place.

    Returns (original_count, kept_count, rejected_count).
    """
    # Read all records
    records = []
    with open(input_path, "r", encoding="utf-8") as f:
        for line_num, line in enumerate(f, 1):
            line = line.strip()
            if not line:
                continue
            try:
                records.append(json.loads(line))
            except json.JSONDecodeError:
                logging.warning("Skipping invalid JSON at %s:%d", input_path, line_num)

    original_count = len(records)
    if original_count == 0:
        logging.info("[%s] Empty file, skipping.", input_path.name)
        return 0, 0, 0

    # Clean
    kept, rejected = deep_clean(records)

    # Write cleaned file (to tmp first, then rename for safety)
    tmp_path = input_path.with_suffix(".jsonl.tmp")
    with open(tmp_path, "w", encoding="utf-8") as f:
        for rec in kept:
            # Remove internal fields before writing
            out_rec = {k: v for k, v in rec.items() if not k.startswith("_")}
            f.write(json.dumps(out_rec, ensure_ascii=False) + "\n")
    tmp_path.rename(input_path)

    # Optionally write rejected records for auditing
    if keep_rejected and rejected:
        rejected_path = input_path.with_name(input_path.stem + ".rejected.jsonl")
        with open(rejected_path, "w", encoding="utf-8") as f:
            for rec in rejected:
                f.write(json.dumps(rec, ensure_ascii=False) + "\n")
        logging.info(
            "[%s] Rejected records written to %s", input_path.name, rejected_path.name
        )

    return original_count, len(kept), len(rejected)


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Clean scraped JSONL files (dedup, boilerplate removal, etc.)"
    )
    parser.add_argument(
        "files",
        nargs="+",
        type=Path,
        help="JSONL files to clean.",
    )
    parser.add_argument(
        "--keep-rejected",
        action="store_true",
        help="Write rejected records to a .rejected.jsonl sidecar file.",
    )
    parser.add_argument(
        "--verbose",
        "-v",
        action="store_true",
        help="Enable debug logging.",
    )
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
        datefmt="%H:%M:%S",
    )

    total_original = 0
    total_kept = 0
    total_rejected = 0

    for filepath in args.files:
        if not filepath.exists():
            logging.warning("File not found: %s", filepath)
            continue
        if filepath.name == "combined_cpt.jsonl":
            logging.info("Skipping combined file: %s", filepath.name)
            continue
        if ".rejected." in filepath.name:
            continue

        logging.info("Cleaning: %s", filepath.name)
        orig, kept, rej = process_file(filepath, args.keep_rejected)
        total_original += orig
        total_kept += kept
        total_rejected += rej
        print(f"  {filepath.name}: {orig} → {kept} kept, {rej} rejected")

    print(f"\nTotal: {total_original} → {total_kept} kept, {total_rejected} rejected")


if __name__ == "__main__":
    main()

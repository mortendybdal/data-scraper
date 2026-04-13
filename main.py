#!/usr/bin/env python3
"""
DataScraping — Scrape websites and format for Unsloth CPT / SFT.

Usage:
    python main.py                          # Scrape all sites in config
    python main.py --sites python-docs      # Scrape specific site(s)
    python main.py --merge                  # Merge all existing outputs into one file
    python main.py --list                   # List configured sites
"""

from __future__ import annotations

import argparse
import logging
import sys
from datetime import datetime, timezone
from pathlib import Path

import yaml

from scraper.crawler import Crawler, SiteConfig
from scraper.extractor import extract_text
from scraper.formatter import (
    chunk_text,
    merge_cpt_files,
    save_cpt_jsonl,
    save_cpt_jsonl_with_metadata,
)

CONFIG_PATH = Path(__file__).parent / "config" / "sites.yaml"
OUTPUT_DIR = Path(__file__).parent / "output"


def load_configs(config_path: Path) -> list[SiteConfig]:
    with open(config_path, "r") as f:
        data = yaml.safe_load(f)
    return [SiteConfig.from_dict(s) for s in data.get("sites", [])]


def scrape_site(config: SiteConfig) -> list[dict[str, str]]:
    """Crawl a site and extract clean text from each page."""
    crawler = Crawler(config)
    pages = crawler.crawl()

    documents = []
    for page in pages:
        text = extract_text(page, content_selector=config.content_selector)
        if text:
            chunks = chunk_text(text, max_tokens=1900)
            for i, chunk in enumerate(chunks):
                documents.append(
                    {
                        "text": chunk,
                        "source": config.name,
                        "url": page.url,
                        "chunk": f"{i+1}/{len(chunks)}",
                    }
                )

    logging.info(
        "[%s] Extracted %d documents from %d pages.",
        config.name,
        len(documents),
        len(pages),
    )
    return documents


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Scrape websites for Unsloth CPT training data."
    )
    parser.add_argument(
        "--sites",
        nargs="*",
        help="Names of specific sites to scrape (default: all).",
    )
    parser.add_argument(
        "--config",
        type=Path,
        default=CONFIG_PATH,
        help="Path to sites.yaml config file.",
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=OUTPUT_DIR,
        help="Output directory for JSONL files.",
    )
    parser.add_argument(
        "--merge",
        action="store_true",
        help="Merge all existing per-site JSONL files into one combined file.",
    )
    parser.add_argument(
        "--list",
        action="store_true",
        help="List all configured sites and exit.",
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

    configs = load_configs(args.config)

    if args.list:
        print(f"Configured sites ({len(configs)}):")
        for c in configs:
            print(f"  - {c.name}: {c.base_url} (max {c.max_pages} pages)")
        return

    if args.merge:
        per_site_files = sorted(args.output.glob("*.jsonl"))
        per_site_files = [f for f in per_site_files if f.name != "combined_cpt.jsonl"]
        if not per_site_files:
            print("No JSONL files found to merge.")
            return
        out = merge_cpt_files(per_site_files, args.output / "combined_cpt.jsonl")
        print(f"Merged into: {out}")
        return

    # Filter to requested sites
    if args.sites:
        configs = [c for c in configs if c.name in args.sites]
        if not configs:
            print(f"No matching sites found for: {args.sites}")
            sys.exit(1)

    all_documents: list[dict[str, str]] = []

    for config in configs:
        print(f"\n{'='*60}")
        print(f"Scraping: {config.name}")
        print(f"{'='*60}")
        documents = scrape_site(config)
        all_documents.extend(documents)

        # Save per-site file
        site_path = args.output / f"{config.name}.jsonl"
        save_cpt_jsonl_with_metadata(documents, site_path)
        print(f"  → Saved {len(documents)} documents to {site_path}")

    # Save combined CPT-ready file (text only, for direct upload to Unsloth)
    if all_documents:
        cpt_path = args.output / "combined_cpt.jsonl"
        save_cpt_jsonl(all_documents, cpt_path)
        print(f"\n{'='*60}")
        print(f"CPT training file: {cpt_path}")
        print(f"Total documents:   {len(all_documents)}")
        print(f"{'='*60}")
    else:
        print("\nNo documents extracted.")


if __name__ == "__main__":
    main()

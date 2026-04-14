#!/usr/bin/env python3
"""
DataScraping — Scrape websites and format for Unsloth CPT / SFT.

Usage:
    python main.py                          # Scrape all sites in config
    python main.py --sites python-docs      # Scrape specific site(s)
    python main.py --merge                  # Merge all existing outputs into one file
    python main.py --list                   # List configured sites
    python main.py --recrawl                # Re-crawl all pages (ignore resume state)
    python main.py --continuous             # Keep crawling without max_pages limit
"""

from __future__ import annotations

import argparse
import json
import logging
import sys
from pathlib import Path

import yaml

from scraper.cleaner import light_filter
from scraper.crawler import Crawler, ScrapedPage, SiteConfig
from scraper.extractor import extract_text
from scraper.formatter import (
    chunk_text,
    merge_cpt_files,
    save_cpt_jsonl,
)

CONFIG_PATH = Path(__file__).parent / "config" / "sites.yaml"
OUTPUT_DIR = Path(__file__).parent / "output"


def load_configs(config_path: Path) -> list[SiteConfig]:
    with open(config_path, "r") as f:
        data = yaml.safe_load(f)
    return [SiteConfig.from_dict(s) for s in data.get("sites", [])]


def _extract_and_format(
    pages: list[ScrapedPage], config: SiteConfig
) -> list[dict[str, str]]:
    """Extract text from pages and return formatted document dicts."""
    documents = []
    for page in pages:
        text = extract_text(page, content_selector=config.content_selector)
        if text and light_filter(text):
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
    return documents


def _append_jsonl(documents: list[dict[str, str]], output_path: Path) -> None:
    """Append documents to a JSONL file in SFT format."""
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with open(output_path, "a", encoding="utf-8") as f:
        for doc in documents:
            record = {
                "instruction": "",
                "input": "",
                "output": doc["text"],
                "source": doc.get("source", ""),
                "url": doc.get("url", ""),
                "chunk": doc.get("chunk", ""),
            }
            f.write(json.dumps(record, ensure_ascii=False) + "\n")


def scrape_site(
    config: SiteConfig,
    output_dir: Path,
    recrawl: bool = False,
    continuous: bool = False,
) -> int:
    """Crawl a site with continuous saving. Returns total document count."""
    site_path = output_dir / f"{config.name}.jsonl"

    # If recrawling, start the file fresh
    if recrawl and site_path.exists():
        site_path.unlink()

    crawler = Crawler(
        config,
        output_path=site_path,
        recrawl=recrawl,
        continuous=continuous,
    )

    total_docs = 0

    def on_batch(pages: list[ScrapedPage]) -> None:
        nonlocal total_docs
        documents = _extract_and_format(pages, config)
        if documents:
            _append_jsonl(documents, site_path)
            total_docs += len(documents)
            logging.info(
                "[%s] Saved %d docs (total: %d)",
                config.name,
                len(documents),
                total_docs,
            )

    crawler.on_pages(on_batch)
    crawler.crawl()

    logging.info(
        "[%s] Done — %d documents saved to %s",
        config.name,
        total_docs,
        site_path,
    )
    return total_docs


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
    parser.add_argument(
        "--recrawl",
        action="store_true",
        help="Ignore resume state and recrawl all pages from scratch.",
    )
    parser.add_argument(
        "--continuous",
        action="store_true",
        help="Keep crawling without max_pages limit (until queue is empty or Ctrl+C).",
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
        per_site_files = [
            f for f in per_site_files
            if f.name != "combined_cpt.jsonl" and ".rejected." not in f.name
        ]
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

    total_all = 0

    for config in configs:
        print(f"\n{'='*60}")
        print(f"Scraping: {config.name}")
        if args.recrawl:
            print("  (recrawl mode — ignoring previous state)")
        if args.continuous:
            print("  (continuous mode — no page limit)")
        print(f"{'='*60}")
        count = scrape_site(config, args.output, args.recrawl, args.continuous)
        total_all += count
        print(f"  → {count} documents saved to {args.output / f'{config.name}.jsonl'}")

    # Merge into combined file
    if total_all:
        per_site_files = sorted(args.output.glob("*.jsonl"))
        per_site_files = [
            f for f in per_site_files
            if f.name != "combined_cpt.jsonl" and ".rejected." not in f.name
        ]
        if per_site_files:
            out = merge_cpt_files(per_site_files, args.output / "combined_cpt.jsonl")
            print(f"\n{'='*60}")
            print(f"Combined file: {out}")
            print(f"Total documents: {total_all}")
            print(f"{'='*60}")
    else:
        print("\nNo documents extracted.")


if __name__ == "__main__":
    main()

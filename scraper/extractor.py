"""Extract clean text from HTML pages using trafilatura."""

from __future__ import annotations

import logging

import trafilatura

from scraper.crawler import ScrapedPage

logger = logging.getLogger(__name__)


def extract_text(page: ScrapedPage, content_selector: str | None = None) -> str | None:
    """Extract the main text content from a scraped page.

    Uses trafilatura for high-quality boilerplate removal. If a content_selector
    is provided, narrows the HTML to that element first.
    """
    html = page.html

    # Optionally narrow to a specific container before extraction
    if content_selector:
        from bs4 import BeautifulSoup

        soup = BeautifulSoup(html, "html.parser")
        container = soup.select_one(content_selector)
        if container:
            html = str(container)

    text = trafilatura.extract(
        html,
        include_comments=False,
        include_tables=True,
        no_fallback=False,
        favor_recall=True,
        url=page.url,
    )

    if not text or len(text.strip()) < 50:
        logger.debug("[%s] Skipping %s — too little content.", page.site_name, page.url)
        return None

    return text.strip()

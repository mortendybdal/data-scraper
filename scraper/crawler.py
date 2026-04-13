"""Generic async web crawler that reads site configs and yields page content."""

from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass, field
from urllib.parse import urljoin, urlparse

import httpx
from bs4 import BeautifulSoup

logger = logging.getLogger(__name__)


@dataclass
class SiteConfig:
    """Parsed configuration for a single site."""

    name: str
    base_url: str
    start_urls: list[str]
    follow_links: bool = True
    link_selector: str = "a[href]"
    content_selector: str | None = None
    max_pages: int = 100
    delay: float = 1.0
    concurrency: int = 5
    allowed_paths: list[str] = field(default_factory=list)

    @classmethod
    def from_dict(cls, data: dict) -> SiteConfig:
        return cls(
            name=data["name"],
            base_url=data["base_url"],
            start_urls=data["start_urls"],
            follow_links=data.get("follow_links", True),
            link_selector=data.get("link_selector", "a[href]"),
            content_selector=data.get("content_selector"),
            max_pages=data.get("max_pages", 100),
            delay=data.get("delay", 1.0),
            concurrency=data.get("concurrency", 5),
            allowed_paths=data.get("allowed_paths", []),
        )


@dataclass
class ScrapedPage:
    """A single scraped page."""

    url: str
    html: str
    site_name: str


class Crawler:
    """Async breadth-first crawler with concurrency control."""

    DEFAULT_HEADERS = {
        "User-Agent": (
            "Mozilla/5.0 (compatible; DataScraping-CPT-Bot/1.0; "
            "+https://github.com/your-repo)"
        ),
        "Accept": "text/html,application/xhtml+xml",
        "Accept-Language": "en-US,en;q=0.9",
    }

    def __init__(self, config: SiteConfig, timeout: float = 30.0):
        self.config = config
        self.timeout = timeout

    def _is_allowed(self, url: str) -> bool:
        """Check if URL falls within the allowed paths for this site."""
        if not self.config.allowed_paths:
            return urlparse(url).netloc == urlparse(self.config.base_url).netloc

        parsed = urlparse(url)
        base_parsed = urlparse(self.config.base_url)
        if parsed.netloc != base_parsed.netloc:
            return False
        return any(parsed.path.startswith(p) for p in self.config.allowed_paths)

    def _extract_links(self, html: str, current_url: str) -> list[str]:
        """Find all links matching the configured selector."""
        soup = BeautifulSoup(html, "html.parser")
        links = []
        for tag in soup.select(self.config.link_selector):
            href = tag.get("href")
            if not href or href.startswith(("#", "javascript:", "mailto:")):
                continue
            absolute = urljoin(current_url, href)
            absolute = absolute.split("#")[0]
            if self._is_allowed(absolute):
                links.append(absolute)
        return links

    async def _fetch_one(
        self,
        client: httpx.AsyncClient,
        url: str,
        semaphore: asyncio.Semaphore,
    ) -> ScrapedPage | None:
        """Fetch a single URL with concurrency and delay control."""
        async with semaphore:
            try:
                logger.info("[%s] Fetching: %s", self.config.name, url)
                resp = await client.get(url)
                resp.raise_for_status()
            except httpx.HTTPError as exc:
                logger.warning("[%s] Failed %s: %s", self.config.name, url, exc)
                return None

            content_type = resp.headers.get("content-type", "")
            if "text/html" not in content_type:
                return None

            # Polite delay per request (inside semaphore so it limits throughput)
            await asyncio.sleep(self.config.delay)
            return ScrapedPage(url=url, html=resp.text, site_name=self.config.name)

    async def crawl_async(self) -> list[ScrapedPage]:
        """Crawl the site asynchronously with bounded concurrency."""
        visited: set[str] = set()
        queue: list[str] = list(self.config.start_urls)
        pages: list[ScrapedPage] = []
        semaphore = asyncio.Semaphore(self.config.concurrency)

        async with httpx.AsyncClient(
            headers=self.DEFAULT_HEADERS,
            timeout=self.timeout,
            follow_redirects=True,
        ) as client:
            while queue and len(pages) < self.config.max_pages:
                # Grab a batch of unvisited URLs up to concurrency limit
                batch: list[str] = []
                while queue and len(batch) < self.config.concurrency:
                    url = queue.pop(0)
                    if url not in visited:
                        visited.add(url)
                        batch.append(url)

                if not batch:
                    break

                remaining = self.config.max_pages - len(pages)
                batch = batch[:remaining]

                tasks = [self._fetch_one(client, url, semaphore) for url in batch]
                results = await asyncio.gather(*tasks)

                for result in results:
                    if result is None:
                        continue
                    pages.append(result)
                    logger.info(
                        "[%s] Progress: %d/%d pages",
                        self.config.name,
                        len(pages),
                        self.config.max_pages,
                    )

                    if self.config.follow_links:
                        for link in self._extract_links(result.html, result.url):
                            if link not in visited:
                                queue.append(link)

        logger.info("[%s] Crawled %d pages.", self.config.name, len(pages))
        return pages

    def crawl(self) -> list[ScrapedPage]:
        """Synchronous wrapper around crawl_async."""
        return asyncio.run(self.crawl_async())

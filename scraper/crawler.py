"""Generic async web crawler that reads site configs and yields page content."""

from __future__ import annotations

import asyncio
import json
import logging
import signal
from dataclasses import dataclass, field
from pathlib import Path
from urllib.parse import urljoin, urlparse

import httpx
from bs4 import BeautifulSoup

logger = logging.getLogger(__name__)

BATCH_SIZE = 10
BATCH_DELAY = 1.5


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


class CrawlState:
    """Tracks crawled URLs on disk for resume support."""

    def __init__(self, state_path: Path):
        self.state_path = state_path
        self.crawled_urls: set[str] = set()
        self._load()

    def _load(self) -> None:
        if self.state_path.exists():
            with open(self.state_path, "r") as f:
                for line in f:
                    line = line.strip()
                    if line:
                        self.crawled_urls.add(line)
            logger.info(
                "Loaded %d previously crawled URLs from %s",
                len(self.crawled_urls),
                self.state_path,
            )

    def add(self, url: str) -> None:
        self.crawled_urls.add(url)

    def has(self, url: str) -> bool:
        return url in self.crawled_urls

    def save(self) -> None:
        self.state_path.parent.mkdir(parents=True, exist_ok=True)
        with open(self.state_path, "w") as f:
            for url in sorted(self.crawled_urls):
                f.write(url + "\n")

    def clear(self) -> None:
        self.crawled_urls.clear()
        if self.state_path.exists():
            self.state_path.unlink()


class Crawler:
    """Async breadth-first crawler with batch fetching, resume, and continuous save."""

    DEFAULT_HEADERS = {
        "User-Agent": (
            "Mozilla/5.0 (compatible; DataScraping-CPT-Bot/1.0; "
            "+https://github.com/your-repo)"
        ),
        "Accept": "text/html,application/xhtml+xml",
        "Accept-Language": "en-US,en;q=0.9",
    }

    def __init__(
        self,
        config: SiteConfig,
        timeout: float = 30.0,
        output_path: Path | None = None,
        recrawl: bool = False,
        continuous: bool = False,
    ):
        self.config = config
        self.timeout = timeout
        self.output_path = output_path
        self.recrawl = recrawl
        self.continuous = continuous
        self._stop_requested = False
        self._unsaved_pages: list[ScrapedPage] = []

        # State file lives next to the output
        state_dir = output_path.parent if output_path else Path("output")
        self._state = CrawlState(state_dir / f".{config.name}.crawl_state")

        if recrawl:
            self._state.clear()
            logger.info("[%s] Recrawl mode — cleared crawl state.", config.name)

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
            href = href.strip()
            absolute = urljoin(current_url, href)
            absolute = absolute.split("#")[0]
            if self._is_allowed(absolute):
                links.append(absolute)
        return links

    async def _fetch_one(
        self,
        client: httpx.AsyncClient,
        url: str,
    ) -> ScrapedPage | None:
        """Fetch a single URL."""
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

        return ScrapedPage(url=url, html=resp.text, site_name=self.config.name)

    def _request_stop(self) -> None:
        """Signal the crawler to stop after the current batch."""
        logger.info(
            "[%s] Stop requested — finishing current batch...", self.config.name
        )
        self._stop_requested = True

    async def crawl_async(self) -> list[ScrapedPage]:
        """Crawl the site in batches of BATCH_SIZE with BATCH_DELAY between batches."""
        visited: set[str] = set()
        queue: list[str] = list(self.config.start_urls)
        pages: list[ScrapedPage] = []

        # Set up signal handler for graceful stop (Ctrl+C)
        loop = asyncio.get_running_loop()
        original_handler = signal.getsignal(signal.SIGINT)

        def _handle_sigint(sig, frame):
            self._request_stop()

        signal.signal(signal.SIGINT, _handle_sigint)

        try:
            async with httpx.AsyncClient(
                headers=self.DEFAULT_HEADERS,
                timeout=self.timeout,
                follow_redirects=True,
            ) as client:
                while queue and not self._stop_requested:
                    if not self.continuous and len(pages) >= self.config.max_pages:
                        break

                    # Build a batch of up to BATCH_SIZE unvisited URLs
                    batch: list[str] = []
                    while queue and len(batch) < BATCH_SIZE:
                        url = queue.pop(0)
                        if url in visited:
                            continue
                        visited.add(url)
                        # Skip already-crawled URLs (resume support)
                        if not self.recrawl and self._state.has(url):
                            continue
                        batch.append(url)

                    if not batch:
                        break

                    if not self.continuous:
                        remaining = self.config.max_pages - len(pages)
                        batch = batch[:remaining]

                    # Fetch all pages in the batch in parallel
                    tasks = [self._fetch_one(client, url) for url in batch]
                    results = await asyncio.gather(*tasks, return_exceptions=True)

                    for result in results:
                        if isinstance(result, Exception):
                            logger.warning(
                                "[%s] Batch error: %s", self.config.name, result
                            )
                            continue
                        if result is None:
                            continue
                        pages.append(result)
                        self._unsaved_pages.append(result)
                        self._state.add(result.url)
                        logger.info(
                            "[%s] Progress: %d pages crawled",
                            self.config.name,
                            len(pages),
                        )

                        if self.config.follow_links:
                            for link in self._extract_links(result.html, result.url):
                                if link not in visited:
                                    queue.append(link)

                    # Save state and flush unsaved pages after each batch
                    self._flush_pages()
                    self._state.save()

                    # Wait between batches
                    if queue and not self._stop_requested:
                        logger.debug(
                            "[%s] Waiting %.1fs before next batch...",
                            self.config.name,
                            BATCH_DELAY,
                        )
                        await asyncio.sleep(BATCH_DELAY)

        except Exception:
            # On any unexpected error, flush what we have
            logger.exception(
                "[%s] Crawler error — saving collected data...", self.config.name
            )
            self._flush_pages()
            self._state.save()
            raise
        finally:
            # Restore original signal handler
            signal.signal(signal.SIGINT, original_handler)
            # Final flush in case anything remains
            self._flush_pages()
            self._state.save()

        logger.info("[%s] Crawled %d pages.", self.config.name, len(pages))
        return pages

    def _flush_pages(self) -> None:
        """Append unsaved pages to the output callback if set."""
        if not self._unsaved_pages or not self.output_path:
            return
        # Notify via callback — main.py handles extraction and writing
        if self._on_pages_callback:
            self._on_pages_callback(self._unsaved_pages)
        self._unsaved_pages = []

    _on_pages_callback = None

    def on_pages(self, callback) -> None:
        """Register a callback called with list[ScrapedPage] after each batch."""
        self._on_pages_callback = callback

    def crawl(self) -> list[ScrapedPage]:
        """Synchronous wrapper around crawl_async."""
        return asyncio.run(self.crawl_async())

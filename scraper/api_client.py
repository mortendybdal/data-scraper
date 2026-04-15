"""API client for medicinpriser.dk — discovers products via search and fetches details."""

from __future__ import annotations

import asyncio
import json
import logging
import signal
import string
from pathlib import Path

import httpx

from scraper.crawler import CrawlState, SiteConfig

logger = logging.getLogger(__name__)

BATCH_SIZE = 10
BATCH_DELAY = 1.0

# Danish alphabet letters used to enumerate products via search
_SEARCH_ALPHABET = list(string.ascii_lowercase) + ["æ", "ø", "å"]


def _format_product_text(product: dict) -> str | None:
    """Convert a product detail JSON dict into readable Danish text."""
    name = product.get("Navn")
    if not name:
        return None

    lines = [f"{name} (varenummer {product.get('Varenummer', 'ukendt')})"]

    field_map = [
        ("VirksomtStof", "Virksomt stof"),
        ("Styrke", "Styrke"),
        ("Pakning", "Pakning"),
        ("Firma", "Firma"),
        ("AtcKode", "ATC-kode"),
        ("Udleveringsgruppe", "Udleveringsgruppe"),
        ("Dosering", "Dosering"),
        ("Indikation", "Indikation"),
        ("Opbevaringsbetingelser", "Opbevaring"),
        ("TilskudTekst", "Tilskud"),
        ("TilskudKode", "Tilskudskode"),
        ("NbsSpeciale", "NBS-speciale"),
    ]
    for key, label in field_map:
        val = product.get(key)
        if val and str(val).strip() and str(val).strip() != "-":
            lines.append(f"{label}: {val}")

    if product.get("Haandkoeb"):
        lines.append("Håndkøb: Ja")
    else:
        lines.append("Håndkøb: Nej")

    if product.get("Dosisdispensering"):
        lines.append("Dosisdispensering: Ja")

    if product.get("TrafikAdvarsel"):
        lines.append("Trafikadvarsel: Ja")

    price = product.get("PrisPrPakning")
    if price is not None:
        lines.append(f"Pris pr. pakning: {price} kr.")
    price_unit = product.get("PrisPrEnhed")
    if price_unit is not None:
        lines.append(f"Pris pr. enhed: {price_unit} kr.")
    aip = product.get("AIP")
    if aip is not None:
        lines.append(f"AIP: {aip} kr.")

    if product.get("Udgaaet"):
        lines.append("Status: Udgået")

    substitutions = product.get("Substitutioner", [])
    if substitutions:
        sub_names = [s["Navn"] for s in substitutions if s.get("Navn")]
        if sub_names:
            lines.append(f"Substitutioner: {', '.join(sub_names)}")

    cheaper = product.get("BilligereKombinationer", [])
    if cheaper:
        cheap_names = [c["Navn"] for c in cheaper if c.get("Navn")]
        if cheap_names:
            lines.append(f"Billigere kombinationer: {', '.join(cheap_names)}")

    return "\n".join(lines)


class ApiScraper:
    """Fetches product data from the medicinpriser.dk API.

    Workflow:
      1. Discover varenummers by searching alphabet prefixes (a, b, ..., å, æ, ø)
         and expanding 2-letter prefixes when a search hits the 100-result cap.
      2. Fetch /produkter/detaljer/{vnr} for each unique varenummer.
      3. Format the structured JSON into natural-language text.
    """

    DEFAULT_HEADERS = {
        "User-Agent": (
            "Mozilla/5.0 (compatible; DataScraping-CPT-Bot/1.0; "
            "+https://github.com/your-repo)"
        ),
        "Accept": "application/json",
    }

    def __init__(
        self,
        config: SiteConfig,
        output_path: Path,
        recrawl: bool = False,
    ):
        self.config = config
        self.output_path = output_path
        self.recrawl = recrawl
        self._stop_requested = False

        state_dir = output_path.parent
        self._state = CrawlState(state_dir / f".{config.name}.crawl_state")

        if recrawl:
            self._state.clear()
            logger.info("[%s] Recrawl mode — cleared API state.", config.name)

        self._on_pages_callback = None

    def on_pages(self, callback) -> None:
        self._on_pages_callback = callback

    def _request_stop(self) -> None:
        logger.info(
            "[%s] Stop requested — finishing current batch...", self.config.name
        )
        self._stop_requested = True

    async def _search_products(
        self, client: httpx.AsyncClient, query: str
    ) -> list[dict]:
        """Search for products by name prefix. Returns list of product dicts."""
        url = f"{self.config.base_url}/produkter/{query}?format=json"
        try:
            resp = await client.get(url)
            if resp.status_code == 200:
                data = resp.json()
                if isinstance(data, list):
                    return data
        except (httpx.HTTPError, json.JSONDecodeError) as exc:
            logger.warning("[%s] Search '%s' failed: %s", self.config.name, query, exc)
        return []

    async def _discover_varenummers(self, client: httpx.AsyncClient) -> set[str]:
        """Discover all varenummers via alphabet-expansion search."""
        discovered: set[str] = set()

        queries = list(self.config.search_queries) if self.config.search_queries else []
        if not queries:
            queries = list(_SEARCH_ALPHABET)

        expand_queue: list[str] = list(queries)
        searched: set[str] = set()

        while expand_queue and not self._stop_requested:
            query = expand_queue.pop(0)
            if query in searched:
                continue
            searched.add(query)

            logger.info("[%s] Searching: '%s'", self.config.name, query)
            results = await self._search_products(client, query)

            for product in results:
                vnr = product.get("Varenummer")
                if vnr:
                    discovered.add(vnr)

            # If we hit the API's 100-result cap, expand to 2-letter prefixes
            if len(results) >= 100 and len(query) < 3:
                logger.info(
                    "[%s] Search '%s' returned %d results (cap) — expanding...",
                    self.config.name,
                    query,
                    len(results),
                )
                for letter in _SEARCH_ALPHABET:
                    sub_query = query + letter
                    if sub_query not in searched:
                        expand_queue.append(sub_query)

            await asyncio.sleep(self.config.delay)

        logger.info(
            "[%s] Discovery complete — found %d unique varenummers.",
            self.config.name,
            len(discovered),
        )
        return discovered

    async def _fetch_detail(self, client: httpx.AsyncClient, vnr: str) -> dict | None:
        """Fetch full product details for a given varenummer."""
        url = f"{self.config.base_url}/produkter/detaljer/{vnr}?format=json"
        try:
            resp = await client.get(url)
            if resp.status_code == 404:
                return None
            resp.raise_for_status()
            data = resp.json()
            if isinstance(data, dict) and "Code" not in data:
                return data
        except (httpx.HTTPError, json.JSONDecodeError) as exc:
            logger.warning(
                "[%s] Detail fetch %s failed: %s", self.config.name, vnr, exc
            )
        return None

    async def scrape_async(self) -> int:
        """Run the full API scrape: discover → fetch details → format → save."""
        loop = asyncio.get_running_loop()
        original_handler = signal.getsignal(signal.SIGINT)

        def _handle_sigint(sig, frame):
            self._request_stop()

        signal.signal(signal.SIGINT, _handle_sigint)

        total_docs = 0

        try:
            async with httpx.AsyncClient(
                headers=self.DEFAULT_HEADERS,
                timeout=30.0,
                follow_redirects=True,
            ) as client:
                # Phase 1: Discover varenummers
                all_vnrs = await self._discover_varenummers(client)

                # Filter out already-fetched varenummers (resume support)
                to_fetch = sorted(vnr for vnr in all_vnrs if not self._state.has(vnr))
                logger.info(
                    "[%s] %d new varenummers to fetch (%d already done).",
                    self.config.name,
                    len(to_fetch),
                    len(all_vnrs) - len(to_fetch),
                )

                # Phase 2: Fetch details in batches
                for i in range(0, len(to_fetch), BATCH_SIZE):
                    if self._stop_requested:
                        break

                    batch = to_fetch[i : i + BATCH_SIZE]
                    tasks = [self._fetch_detail(client, vnr) for vnr in batch]
                    results = await asyncio.gather(*tasks, return_exceptions=True)

                    batch_texts = []
                    for vnr, result in zip(batch, results):
                        if isinstance(result, Exception):
                            logger.warning(
                                "[%s] Error for %s: %s", self.config.name, vnr, result
                            )
                            continue
                        if result is None:
                            self._state.add(vnr)
                            continue

                        text = _format_product_text(result)
                        if text:
                            batch_texts.append(
                                {
                                    "text": text,
                                    "source": self.config.name,
                                    "url": f"{self.config.base_url}/produkter/detaljer/{vnr}",
                                }
                            )
                        self._state.add(vnr)

                    if batch_texts and self._on_pages_callback:
                        self._on_pages_callback(batch_texts)
                        total_docs += len(batch_texts)
                        logger.info(
                            "[%s] Saved %d products (total: %d)",
                            self.config.name,
                            len(batch_texts),
                            total_docs,
                        )

                    self._state.save()

                    if i + BATCH_SIZE < len(to_fetch) and not self._stop_requested:
                        await asyncio.sleep(BATCH_DELAY)

        except Exception:
            logger.exception(
                "[%s] API scraper error — saving state...", self.config.name
            )
            self._state.save()
            raise
        finally:
            signal.signal(signal.SIGINT, original_handler)
            self._state.save()

        logger.info("[%s] Done — %d products saved.", self.config.name, total_docs)
        return total_docs

    def scrape(self) -> int:
        """Synchronous wrapper around scrape_async."""
        return asyncio.run(self.scrape_async())

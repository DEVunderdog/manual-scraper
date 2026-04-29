"""
Printify Scraper - Full Catalog with Variant-Level Pricing

Scrapes ALL products from Printify's internal product catalog API:
  https://printify.com/app/products

Discovered working endpoints (no auth required):
  - /product-catalog-service/api/v1/blueprints/search  (1368+ blueprints)
  - /product-catalog-service/api/v1/print-providers    (74 providers)
  - /product-catalog-service/api/v2/blueprints/{id}/print-providers
  - /product-catalog-service/api/v2/blueprints/{id}/print-providers/{pid}
  - /product-catalog-service/api/v2/blueprints/{id}/print-providers/{pid}/variants

Output format: one MongoDB document per (blueprint × provider × variant).
"""

import asyncio
import random
import structlog
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

import httpx
from tenacity import (
    retry,
    retry_if_exception_type,
    stop_after_attempt,
    wait_exponential,
)

from shared.exceptions import ScrapeFailedError
from shared.scrapers.base import BaseScraper, ScrapeResult
from shared.scrapers.registry import register_scraper
from shared.config.settings import get_settings

logger = structlog.get_logger(__name__)

_settings = get_settings()

# ─── API base URLs ────────────────────────────────────────────────────────────
_BASE = "https://printify.com/product-catalog-service"
BLUEPRINT_SEARCH_URL = f"{_BASE}/api/v1/blueprints/search"
PRINT_PROVIDERS_URL = f"{_BASE}/api/v1/print-providers"
BLUEPRINT_PROVIDERS_URL = f"{_BASE}/api/v2/blueprints/{{id}}/print-providers"
PROVIDER_DETAIL_URL = f"{_BASE}/api/v2/blueprints/{{bid}}/print-providers/{{pid}}"
VARIANTS_URL = f"{_BASE}/api/v2/blueprints/{{bid}}/print-providers/{{pid}}/variants"
IMAGE_CDN = "https://images.printify.com/"

# ─── User-agent rotation ─────────────────────────────────────────────────────
USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
]

# ─── Scraper constants ────────────────────────────────────────────────────────
DEFAULT_PAGE_SIZE = 100  # items per API page
# NOTE: The runtime default for HTTP concurrency comes from
# ``settings.effective_scraper_http_concurrency`` (env-aware: dev=2, prod=8).
# This module-level constant is kept only as a hard fallback if settings
# ever fail to load; do not reference it directly in scraping logic.
DEFAULT_CONCURRENCY = 8  # max simultaneous HTTP requests (legacy fallback)
DEFAULT_REQUEST_DELAY = 0.05  # seconds between releasing semaphore slots
DIRECT_STORE_THRESHOLD = 200  # blueprints above which we store directly to DB

# ─── Provider country allowlist ──────────────────────────────────────────────
# Only providers whose location resolves to one of these ISO-3166 alpha-2
# country codes will be scraped. Extend this set to include more countries.
ALLOWED_PROVIDER_COUNTRIES = {"US"}

# Maps Printify's `countryName` (full names returned by
# /api/v1/print-providers) to ISO-3166 alpha-2 codes. Only entries we
# care about are listed; unknown names fall through to the raw value.
_COUNTRY_NAME_TO_ISO = {
    "US": "US",
    "USA": "US",
    "U.S.": "US",
    "U.S.A.": "US",
    "United States": "US",
    "United States of America": "US",
}


def _country_to_iso(country_name: str) -> str:
    """Best-effort normalization of a country name/code to ISO-3166 alpha-2.

    Returns the mapped code if known, otherwise the original (stripped) value
    so callers can still compare against an allowlist that uses raw names.
    """
    if not country_name:
        return ""
    name = country_name.strip()
    if name in _COUNTRY_NAME_TO_ISO:
        return _COUNTRY_NAME_TO_ISO[name]
    # If it already looks like a 2-letter code, normalize to upper-case.
    if len(name) == 2 and name.isalpha():
        return name.upper()
    return name


@register_scraper(
    site_id="printify",
    name="Printify Product Catalog",
    base_url="https://printify.com/app/products",
    description="Scrapes the full Printify print-on-demand catalog — all blueprints, providers and variant-level pricing.",
    tags=["ecommerce", "print-on-demand", "products", "pricing"],
)
class PrintifyScraper(BaseScraper):
    """
    Full Printify catalog scraper.

    Payload ``extra`` options
    -------------------------
    max_blueprints : int | None
        Cap the number of blueprints to scrape.  ``None`` (default) = all.
    concurrency : int
        Max concurrent HTTP requests (default 8).
    scrape_mode : str
        ``"variants"`` (default) – one record per variant (most granular, full pricing).
        ``"providers"`` – one record per blueprint×provider (summary pricing).
    """

    def __init__(self, payload: dict) -> None:
        super().__init__(payload)
        self.headers = {
            "Accept": "application/json",
            "User-Agent": random.choice(USER_AGENTS),
            "Referer": "https://printify.com/app/products",
        }

    # ──────────────────────────────────────────────────────────────────────────
    # Public interface
    # ──────────────────────────────────────────────────────────────────────────

    def scrape(self) -> ScrapeResult:
        """Execute the Printify scraping logic (sync wrapper around async)."""
        logger.info("printify.scraper.started")

        try:
            loop = asyncio.get_event_loop()
        except RuntimeError:
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)

        result_data = loop.run_until_complete(self._async_scrape())

        return ScrapeResult(
            site=self.site_id,
            url=self.payload.url or "https://printify.com/app/products",
            data=result_data,
            metadata={
                "blueprints_scraped": result_data.get("blueprints_scraped", 0),
                "providers_scraped": result_data.get("providers_scraped", 0),
                "products_count": result_data.get("products_count", 0),
                "is_partial_scrape": result_data.get("is_partial_scrape", True),
                "scraped_at": datetime.now(timezone.utc).isoformat(),
            },
        )

    # ──────────────────────────────────────────────────────────────────────────
    # Async scraping orchestration
    # ──────────────────────────────────────────────────────────────────────────

    async def _async_scrape(self) -> Dict[str, Any]:
        max_blueprints = self.payload.extra.get("max_blueprints", None)
        concurrency = self.payload.extra.get(
            "concurrency", _settings.effective_scraper_http_concurrency
        )
        scrape_mode = self.payload.extra.get("scrape_mode", "variants")

        # Use direct DB storage for large / unlimited runs (avoids memory overflow)
        use_direct_storage = (
            max_blueprints is None or max_blueprints > DIRECT_STORE_THRESHOLD
        ) and self._db is not None

        semaphore = asyncio.Semaphore(concurrency)

        async with httpx.AsyncClient(
            headers=self.headers,
            timeout=httpx.Timeout(30.0, connect=10.0),
            follow_redirects=True,
            limits=httpx.Limits(max_connections=concurrency + 4),
        ) as client:
            # Step 1 – fetch all print providers (for name/country lookups)
            logger.info("printify.fetching_providers")
            provider_map = await self._fetch_all_providers(client, semaphore)
            logger.info("printify.providers_fetched", count=len(provider_map))

            # Step 2 – fetch all blueprint stubs
            logger.info("printify.fetching_blueprints")
            blueprints = await self._fetch_all_blueprints(
                client, semaphore, max_blueprints
            )
            logger.info("printify.blueprints_fetched", count=len(blueprints))

            # Step 3 – scrape each blueprint
            if use_direct_storage:
                return await self._scrape_with_direct_storage(
                    client, semaphore, blueprints, provider_map, scrape_mode
                )
            else:
                return await self._scrape_to_memory(
                    client,
                    semaphore,
                    blueprints,
                    provider_map,
                    scrape_mode,
                    max_blueprints,
                )

    # ──────────────────────────────────────────────────────────────────────────
    # Large-catalog mode: store directly to MongoDB per-batch
    # ──────────────────────────────────────────────────────────────────────────

    async def _scrape_with_direct_storage(
        self,
        client: httpx.AsyncClient,
        semaphore: asyncio.Semaphore,
        blueprints: List[Dict],
        provider_map: Dict[int, Dict],
        scrape_mode: str,
    ) -> Dict[str, Any]:
        from worker.database.product import ProductRepository

        product_repo = ProductRepository(self._db)
        session_id = product_repo.create_scrape_session(
            source="printify", task_id=self._task_id, scrape_type="full"
        )

        total_products = 0
        total_providers = 0
        batch: List[Dict] = []
        BATCH_SIZE = 500  # flush to DB every N records

        for idx, blueprint in enumerate(blueprints):
            self._check_cancelled()

            bp_products, bp_providers = await self._scrape_blueprint(
                client, semaphore, blueprint, provider_map, scrape_mode
            )
            batch.extend(bp_products)
            total_providers += bp_providers

            logger.info(
                "printify.blueprint.done",
                blueprint_id=blueprint.get("blueprintId"),
                name=blueprint.get("name", ""),
                variants=len(bp_products),
                progress=f"{idx + 1}/{len(blueprints)}",
            )

            # Flush batch
            if len(batch) >= BATCH_SIZE:
                product_repo.store_products_bulk("printify", batch, self._task_id)
                total_products += len(batch)
                logger.info("printify.batch_stored", stored=total_products)
                batch = []

        # Flush remaining
        if batch:
            product_repo.store_products_bulk("printify", batch, self._task_id)
            total_products += len(batch)

        product_repo.update_scrape_session(
            session_id=session_id,
            status="completed",
            products_found=total_products,
            products_stored=total_products,
        )

        logger.info(
            "printify.scrape_complete",
            blueprints=len(blueprints),
            products=total_products,
        )

        return {
            "products": [],  # already stored – task will skip storage
            "blueprints_scraped": len(blueprints),
            "providers_scraped": total_providers,
            "products_count": total_products,
            "is_partial_scrape": True,  # prevent task from soft-deleting
            "stored_directly": True,
        }

    # ──────────────────────────────────────────────────────────────────────────
    # Small / limited run: collect everything in memory
    # ──────────────────────────────────────────────────────────────────────────

    async def _scrape_to_memory(
        self,
        client: httpx.AsyncClient,
        semaphore: asyncio.Semaphore,
        blueprints: List[Dict],
        provider_map: Dict[int, Dict],
        scrape_mode: str,
        max_blueprints: Optional[int],
    ) -> Dict[str, Any]:
        all_products: List[Dict] = []
        total_providers = 0

        for idx, blueprint in enumerate(blueprints):
            self._check_cancelled()

            bp_products, bp_providers = await self._scrape_blueprint(
                client, semaphore, blueprint, provider_map, scrape_mode
            )
            all_products.extend(bp_products)
            total_providers += bp_providers

            logger.info(
                "printify.blueprint.done",
                blueprint_id=blueprint.get("blueprintId"),
                name=blueprint.get("name", ""),
                variants=len(bp_products),
                progress=f"{idx + 1}/{len(blueprints)}",
            )

        return {
            "products": all_products,
            "blueprints_scraped": len(blueprints),
            "providers_scraped": total_providers,
            "products_count": len(all_products),
            "is_partial_scrape": max_blueprints is not None,
        }

    # ──────────────────────────────────────────────────────────────────────────
    # Per-blueprint scraping
    # ──────────────────────────────────────────────────────────────────────────

    async def _scrape_blueprint(
        self,
        client: httpx.AsyncClient,
        semaphore: asyncio.Semaphore,
        blueprint_stub: Dict,
        provider_map: Dict[int, Dict],
        scrape_mode: str,
    ) -> Tuple[List[Dict], int]:
        """
        Scrape one blueprint: fetch all providers → for each provider fetch
        provider detail (options lookup) + variants, then build product records.

        Returns (products, num_providers_scraped).
        """
        bp_id = blueprint_stub["blueprintId"]
        bp_name = blueprint_stub.get("name", "")
        # Use first plain tag as category (e.g. "Tank Tops", "Hoodies"), or fallback to managed_tags
        plain_tags = blueprint_stub.get("tags", [])
        bp_type = plain_tags[0] if plain_tags else ""
        bp_tags = [
            t["label"]
            for t in blueprint_stub.get("managed_tags", [])
            if t.get("visible")
        ]
        # Add all plain tags to tag list as well
        for pt in plain_tags:
            if pt not in bp_tags:
                bp_tags.append(pt)
        bp_brand = blueprint_stub.get("brandName", "")
        bp_images = [
            f"{IMAGE_CDN}{img['src']}"
            for img in blueprint_stub.get("images", [])
            if img.get("src")
        ]
        bp_description = ""  # populated from provider detail or left blank for stubs

        # Cost range info from stub
        cost_ranges_std = blueprint_stub.get(
            "cost_ranges_by_decoration_method_standard", {}
        )
        cost_ranges_sub = blueprint_stub.get(
            "cost_ranges_by_decoration_method_subscription", {}
        )

        # Fetch providers for this blueprint
        url = BLUEPRINT_PROVIDERS_URL.format(id=bp_id)
        try:
            bp_providers_data = await self._get_json(client, semaphore, url)
        except Exception as exc:
            logger.warning(
                "printify.blueprint_providers_failed", bp_id=bp_id, error=str(exc)
            )
            return [], 0

        bp_providers = bp_providers_data.get("data", [])
        if not bp_providers:
            return [], 0

        products: List[Dict] = []
        kept_providers = 0

        for prov_stub in bp_providers:
            pid = prov_stub.get("id")
            if not pid:
                continue

            prov_info = provider_map.get(pid, {})

            # ── Country allowlist filter ──────────────────────────────────
            # Skip non-US providers (or any not in ALLOWED_PROVIDER_COUNTRIES)
            # *before* making any expensive per-provider detail/variant calls.
            prov_country_iso = prov_info.get("country_iso", "")
            if prov_country_iso not in ALLOWED_PROVIDER_COUNTRIES:
                logger.info(
                    "printify.provider_skipped_non_us",
                    provider_id=pid,
                    country=(
                        prov_info.get("countryName", "")
                        or prov_country_iso
                        or "unknown"
                    ),
                )
                continue
            kept_providers += 1

            if scrape_mode == "providers":
                # One record per blueprint×provider (summary, no per-variant call)
                product = self._build_provider_record(
                    bp_id=bp_id,
                    bp_name=bp_name,
                    bp_type=bp_type,
                    bp_tags=bp_tags,
                    bp_brand=bp_brand,
                    bp_images=bp_images,
                    bp_description=bp_description,
                    cost_ranges_std=cost_ranges_std,
                    prov_stub=prov_stub,
                    prov_info=prov_info,
                )
                products.append(product)
                continue

            # variants mode: fetch provider detail + all variants
            try:
                detail_url = PROVIDER_DETAIL_URL.format(bid=bp_id, pid=pid)
                variants_url = VARIANTS_URL.format(bid=bp_id, pid=pid)

                # Fetch both concurrently
                detail_task = self._get_json(client, semaphore, detail_url)
                variants_task = self._fetch_all_pages(client, semaphore, variants_url)
                prov_detail, all_variants = await asyncio.gather(
                    detail_task, variants_task
                )

            except Exception as exc:
                logger.warning(
                    "printify.provider_detail_failed",
                    bp_id=bp_id,
                    pid=pid,
                    error=str(exc),
                )
                continue

            # Build option lookup: {option_id -> {label, hex, type}}
            options_lookup = _build_options_lookup(prov_detail.get("options", []))

            prov_name = prov_detail.get("name") or prov_info.get(
                "name", f"Provider {pid}"
            )
            prov_country = (prov_detail.get("location") or {}).get("country", "")
            prov_scoring = prov_detail.get("scoring", {})
            print_positions = [
                p.get("label", "")
                for p in prov_detail.get("print_position", [])
                if p.get("label")
            ]
            min_shipping_cents = _extract_min_shipping_us(
                prov_detail.get("min_shipping", [])
            )

            for variant in all_variants:
                rec = self._build_variant_record(
                    bp_id=bp_id,
                    bp_name=bp_name,
                    bp_type=bp_type,
                    bp_tags=bp_tags,
                    bp_brand=bp_brand,
                    bp_images=bp_images,
                    prov_stub=prov_stub,
                    pid=pid,
                    prov_name=prov_name,
                    prov_country=prov_country,
                    prov_scoring=prov_scoring,
                    print_positions=print_positions,
                    min_shipping_cents=min_shipping_cents,
                    options_lookup=options_lookup,
                    variant=variant,
                )
                products.append(rec)

        return products, kept_providers

    # ──────────────────────────────────────────────────────────────────────────
    # Record builders
    # ──────────────────────────────────────────────────────────────────────────

    @staticmethod
    def _build_variant_record(
        *,
        bp_id: int,
        bp_name: str,
        bp_type: str,
        bp_tags: List[str],
        bp_brand: str,
        bp_images: List[str],
        prov_stub: Dict,
        pid: int,
        prov_name: str,
        prov_country: str,
        prov_scoring: Dict,
        print_positions: List[str],
        min_shipping_cents: Optional[int],
        options_lookup: Dict[int, Dict],
        variant: Dict,
    ) -> Dict[str, Any]:
        variant_id = variant.get("id", 0)
        opt_ids = variant.get("options", [])

        # Decode color & size from option IDs
        color_info = {}
        size_info = {}
        for oid in opt_ids:
            info = options_lookup.get(oid, {})
            if info.get("type") == "color":
                color_info = info
            elif info.get("type") == "size":
                size_info = info

        # Pricing is in variant["costs"][0]["result"] (in cents)
        costs = variant.get("costs", [])
        cost = costs[0].get("result", 0) if costs else (variant.get("cost") or 0)
        cost_sub = (
            costs[0].get("result_subscription")
            if costs
            else variant.get("cost_subscription")
        )

        return {
            "product_id": f"bp{bp_id}_p{pid}_v{variant_id}",
            "blueprint_id": bp_id,
            "provider_id": pid,
            "variant_id": variant_id,
            # Product identity
            "title": bp_name,
            "description": "",  # description available via blueprint detail if needed
            "brand": bp_brand,
            "category": bp_type,
            "tags": bp_tags,
            # Provider
            "provider_name": prov_name,
            "provider_country": prov_country,
            "provider_scoring": prov_scoring,
            "print_positions": print_positions,
            # Variant attributes
            "color_label": color_info.get("label", ""),
            "color_hex": color_info.get("hex", ""),
            "size_label": size_info.get("label", ""),
            # Pricing (in cents, Printify standard)
            "price_cents": cost,
            "price_usd": round(cost / 100, 2) if cost else None,
            "price_subscription_cents": cost_sub,
            "price_subscription_usd": round(cost_sub / 100, 2) if cost_sub else None,
            "min_price_cents": prov_stub.get("min_price"),
            "min_price_usd": (
                round(prov_stub.get("min_price", 0) / 100, 2)
                if prov_stub.get("min_price")
                else None
            ),
            "min_shipping_cents": min_shipping_cents,
            "min_shipping_usd": (
                round(min_shipping_cents / 100, 2) if min_shipping_cents else None
            ),
            # Availability
            "status": variant.get("status", ""),
            "available": variant.get("available", False),
            # Media
            "images": bp_images,
            "source": "printify",
        }

    @staticmethod
    def _build_provider_record(
        *,
        bp_id: int,
        bp_name: str,
        bp_type: str,
        bp_tags: List[str],
        bp_brand: str,
        bp_images: List[str],
        bp_description: str,
        cost_ranges_std: Dict,
        prov_stub: Dict,
        prov_info: Dict,
    ) -> Dict[str, Any]:
        pid = prov_stub.get("id", 0)
        min_price = prov_stub.get("min_price", 0)
        min_shipping = _extract_min_shipping_us(prov_stub.get("min_shipping", []))

        return {
            "product_id": f"bp{bp_id}_p{pid}",
            "blueprint_id": bp_id,
            "provider_id": pid,
            "variant_id": None,
            "title": bp_name,
            "description": bp_description,
            "brand": bp_brand,
            "category": bp_type,
            "tags": bp_tags,
            "provider_name": prov_stub.get("name")
            or prov_info.get("name", f"Provider {pid}"),
            "provider_country": prov_info.get("countryName", ""),
            "provider_scoring": prov_info.get("scoring", {}),
            "print_positions": [],
            "color_label": "",
            "color_hex": "",
            "size_label": "",
            "price_cents": min_price,
            "price_usd": round(min_price / 100, 2) if min_price else None,
            "price_subscription_cents": prov_stub.get("min_price_subscription"),
            "price_subscription_usd": (
                round(prov_stub["min_price_subscription"] / 100, 2)
                if prov_stub.get("min_price_subscription")
                else None
            ),
            "min_price_cents": min_price,
            "min_price_usd": round(min_price / 100, 2) if min_price else None,
            "min_shipping_cents": min_shipping,
            "min_shipping_usd": round(min_shipping / 100, 2) if min_shipping else None,
            "cost_range_std": cost_ranges_std,
            "status": "available",
            "available": True,
            "images": bp_images,
            "source": "printify",
        }

    # ──────────────────────────────────────────────────────────────────────────
    # Bulk-fetch helpers
    # ──────────────────────────────────────────────────────────────────────────

    async def _fetch_all_providers(
        self, client: httpx.AsyncClient, semaphore: asyncio.Semaphore
    ) -> Dict[int, Dict]:
        """Return {provider_id: {name, countryName, scoring, productsCount}}."""
        first = await self._get_json(
            client, semaphore, f"{PRINT_PROVIDERS_URL}?page=1&limit=1"
        )
        total = first.get("total", 0)
        if not total:
            return {}

        data = await self._get_json(
            client, semaphore, f"{PRINT_PROVIDERS_URL}?page=1&limit={total}"
        )
        return {
            p["id"]: {
                "name": p.get("name", ""),
                "countryName": p.get("countryName", ""),
                "country_iso": _country_to_iso(p.get("countryName", "")),
                "scoring": p.get("scoring", {}),
                "productsCount": p.get("productsCount", 0),
            }
            for p in data.get("data", [])
            if p.get("id")
        }

    async def _fetch_all_blueprints(
        self,
        client: httpx.AsyncClient,
        semaphore: asyncio.Semaphore,
        max_blueprints: Optional[int],
    ) -> List[Dict]:
        """Paginate through blueprint search and return stub list."""
        # Probe first page for total count
        first = await self._get_json(
            client, semaphore, f"{BLUEPRINT_SEARCH_URL}?page=1&limit=1"
        )
        total = first.get("total", 0)
        per_page = DEFAULT_PAGE_SIZE
        pages = (total + per_page - 1) // per_page

        if not total:
            return []

        # Fetch pages concurrently
        tasks = [
            self._get_json(
                client, semaphore, f"{BLUEPRINT_SEARCH_URL}?page={p}&limit={per_page}"
            )
            for p in range(1, pages + 1)
        ]
        results = await asyncio.gather(*tasks, return_exceptions=True)

        blueprints: List[Dict] = []
        for res in results:
            if isinstance(res, Exception):
                logger.warning("printify.blueprint_page_error", error=str(res))
                continue
            blueprints.extend(res.get("data", []))

        if max_blueprints:
            blueprints = blueprints[:max_blueprints]

        return blueprints

    async def _fetch_all_pages(
        self, client: httpx.AsyncClient, semaphore: asyncio.Semaphore, base_url: str
    ) -> List[Dict]:
        """Paginate through any resource endpoint and return combined data list."""
        first = await self._get_json(
            client, semaphore, f"{base_url}?page=1&limit={DEFAULT_PAGE_SIZE}"
        )
        total = first.get("total", 0)
        items = list(first.get("data", []))
        per_page = DEFAULT_PAGE_SIZE

        if total <= per_page:
            return items

        pages = (total + per_page - 1) // per_page
        tasks = [
            self._get_json(client, semaphore, f"{base_url}?page={p}&limit={per_page}")
            for p in range(2, pages + 1)
        ]
        rest = await asyncio.gather(*tasks, return_exceptions=True)
        for res in rest:
            if isinstance(res, Exception):
                continue
            items.extend(res.get("data", []))

        return items

    # ──────────────────────────────────────────────────────────────────────────
    # Low-level HTTP helper (with retry + semaphore + rate-limit delay)
    # ──────────────────────────────────────────────────────────────────────────

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=2, max=15),
        retry=retry_if_exception_type(
            (httpx.TimeoutException, httpx.ConnectError, httpx.HTTPStatusError)
        ),
        reraise=True,
    )
    async def _get_json(
        self,
        client: httpx.AsyncClient,
        semaphore: asyncio.Semaphore,
        url: str,
    ) -> Dict[str, Any]:
        async with semaphore:
            resp = await client.get(url)
            # Respect 429 / 503 rate-limiting
            if resp.status_code in (429, 503):
                retry_after = int(resp.headers.get("Retry-After", "5"))
                logger.warning("printify.rate_limited", url=url, wait=retry_after)
                await asyncio.sleep(retry_after)
                resp.raise_for_status()
            elif resp.status_code >= 400:
                logger.debug("printify.http_error", url=url, status=resp.status_code)
                resp.raise_for_status()

            await asyncio.sleep(DEFAULT_REQUEST_DELAY)
            return resp.json()


# ─── Pure helpers ──────────────────────────────────────────────────────────────


def _build_options_lookup(options: List[Dict]) -> Dict[int, Dict]:
    """
    Build a flat {option_id -> {type, label, hex}} lookup from a provider's
    options array (which contains color and size sub-arrays).
    """
    lookup: Dict[int, Dict] = {}
    for opt_group in options:
        opt_type = opt_group.get("type", "")
        for item in opt_group.get("items", []):
            oid = item.get("id")
            label = item.get("label", "")
            colors = item.get("colors", [])
            hex_val = colors[0].get("hex", "") if colors else ""
            if oid is not None:
                lookup[oid] = {"type": opt_type, "label": label, "hex": hex_val}
    return lookup


def _extract_min_shipping_us(shipping_list: List[Dict]) -> Optional[int]:
    """
    Extract the minimum US/economy shipping rate (in cents) from the provider's
    min_shipping array.  Falls back to standard if economy is not available.
    """
    if not shipping_list:
        return None

    us_rates: List[int] = []
    for entry in shipping_list:
        country = entry.get("country", "")
        if country in ("United States", "US"):
            rate = entry.get("rate_first")
            if rate is not None:
                us_rates.append(rate)

    if us_rates:
        return min(us_rates)

    # Fall back to global minimum
    all_rates = [
        e.get("rate_first") for e in shipping_list if e.get("rate_first") is not None
    ]
    return min(all_rates) if all_rates else None

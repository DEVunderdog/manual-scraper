"""
CustomNapkinsNow Scraper — v2 (schema_version = 2)

Goal: capture every product the site exposes with a full option-axis model,
per-variant tier pricing, structured colors (split into napkin vs imprint
groups), a real product SKU when available, and deal/bundle products.

Output shape (backward-compatible additions):

    {
        "source": "CUSTOMNAPKINSNOW",
        "product_url": str,
        "schema_version": 2,
        "product_subtype": "standard" | "deal",
        "product_sku": str | None,          # real SKU if present, else "SKU<db_tag>"
        "database_tag": str | None,         # internal site product id
        "name", "category", "subcategory", "description", "meta_description",
        "material", "size",

        # --- Legacy (v1) kept for back-compat ---
        "available_colors": List[str],
        "pricing":  List[{print_method, quantity, unit_price}],
        "print_methods": List[str],
        "min_order_qty": int | None,
        "main_image", "gallery_images",

        # --- New structured v2 fields ---
        "colors": List[{label, swatch_image_url, option_value_id,
                        option_id, group, price_modifier}],
        "options": {
            "print_method": [...],
            "napkin":       [...],
            "imprint":      [...],
            "ply_or_size":  [...],
            "orientation":  [...],
            "style":        [...],
            "other":        [...],
        },
        "option_groups_detected": List[str],
        "base_tiers": List[{quantity, unit_price, is_sample, is_outlier}],
        "variants":  List[{
            "variant_key": str,
            "sku": str,
            "print_method": {label, option_value_id} | None,
            "napkin_color": {label, option_value_id} | None,
            "imprint_color": {label, option_value_id} | None,
            "ply_or_size": {label, option_value_id} | None,
            "price_modifier_total": float,
            "tiers": List[{quantity, unit_price, is_sample, is_outlier}],
        }],
    }

The change-tracker keys products by (source, product_id|sku|product_url).  We
intentionally DO NOT populate `product_id` or `sku` at the top level — so the
dedup key remains `(source, product_url)`, identical to v1, preventing a
spurious wave of "deleted/added" change events on the first v2 run.
"""

from __future__ import annotations

import asyncio
import hashlib
import json
import os
import re
import time
import xml.etree.ElementTree as ET
from datetime import datetime, timezone
from statistics import median
from typing import Any, Dict, List, Optional, Set, Tuple
from urllib.parse import urljoin, urlparse

import httpx
import structlog
from bs4 import BeautifulSoup
from tenacity import (
    retry,
    retry_if_exception_type,
    stop_after_attempt,
    wait_exponential,
)

from shared.exceptions import ScrapeFailedError, TaskCancelledError
from shared.scrapers.base import BaseScraper, ScrapeResult
from shared.scrapers.registry import register_scraper

logger = structlog.get_logger(__name__)


# ---------------------------------------------------------------------------
# Group classifier — keyword map for Step-header labels.
# Order matters: more specific phrases first so e.g. "imprint color"
# wins over the generic "imprint" or "color".
# ---------------------------------------------------------------------------
_GROUP_KEYWORDS: List[Tuple[str, List[str]]] = [
    ("imprint", ["imprint color", "ink color", "foil color"]),
    ("napkin", ["napkin color"]),
    (
        "print_method",
        [
            "printing option",
            "print option",
            "print method",
            "imprint method",
            "napkin type",
            "napkin style",
            "select style",
        ],
    ),
    ("ply_or_size", ["napkin size", "select size", "ply"]),
    ("orientation", ["orientation"]),
    ("style", ["style"]),
]

_VARIANT_SOFT_CAP = 500  # hard guardrail against cartesian explosions


# ---------------------------------------------------------------------------
# Phase 3 — concurrency & re-run efficiency knobs (env-tunable)
# ---------------------------------------------------------------------------
_CONCURRENCY_HARD_CAP = 16
_PER_SLOT_MIN_DELAY_S = 0.2  # minimum interval between request *starts*, per slot
_PROGRESS_MIN_INTERVAL_S = 1.0
_PROGRESS_MIN_STEP_PCT = 1


def _resolve_concurrency() -> int:
    """
    Resolve effective concurrency from env.

    - Explicit ``CUSTOMNAPKINSNOW_CONCURRENCY`` override wins (clamped to the
      hard cap).
    - Otherwise dev defaults to 3 and prod to 8.
    - Anything >= _CONCURRENCY_HARD_CAP is clamped + warned.
    """
    raw = os.environ.get("CUSTOMNAPKINSNOW_CONCURRENCY")
    if raw:
        try:
            n = int(raw)
        except ValueError:
            logger.warning(
                "customnapkinsnow.concurrency.invalid_env",
                value=raw,
                fallback=3,
            )
            n = 3
    else:
        env = (os.environ.get("ENVIRONMENT") or "dev").lower()
        n = 8 if env == "prod" else 3

    n = max(1, n)
    if n > _CONCURRENCY_HARD_CAP:
        logger.warning(
            "customnapkinsnow.concurrency.clamped",
            requested=n,
            capped_to=_CONCURRENCY_HARD_CAP,
        )
        n = _CONCURRENCY_HARD_CAP
    return n


def _env_truthy(val: Optional[str]) -> bool:
    return (val or "").strip().lower() in {"1", "true", "yes", "on"}


class _ProgressThrottle:
    """
    Throttle progress emissions so we don't spam SQS / Mongo.

    Emits when the integer percent has advanced by at least
    ``_PROGRESS_MIN_STEP_PCT`` OR when ``_PROGRESS_MIN_INTERVAL_S`` has
    elapsed since the last emit — whichever is sooner.  The first call
    always emits.
    """

    def __init__(self) -> None:
        self._last_pct: int = -1
        self._last_ts: float = 0.0

    def should_emit(self, pct: int) -> bool:
        now = time.monotonic()
        pct = int(pct)
        if self._last_pct < 0:
            self._last_pct, self._last_ts = pct, now
            return True
        pct_delta = abs(pct - self._last_pct)
        elapsed = now - self._last_ts
        if pct_delta >= _PROGRESS_MIN_STEP_PCT or elapsed >= _PROGRESS_MIN_INTERVAL_S:
            self._last_pct, self._last_ts = pct, now
            return True
        return False


def _classify_group(label: str) -> str:
    s = (label or "").lower()
    for group, kws in _GROUP_KEYWORDS:
        for kw in kws:
            if kw in s:
                return group
    return "other"


# ---------------------------------------------------------------------------
# Scraper
# ---------------------------------------------------------------------------
@register_scraper(
    site_id="customnapkinsnow",
    name="CustomNapkinsNow",
    base_url="https://www.customnapkinsnow.com",
    description=(
        "Scrapes the customnapkinsnow.com catalog with full variant matrix, "
        "per-variant tier pricing, structured colors (napkin vs imprint), "
        "and deals/bundles."
    ),
    tags=["ecommerce", "promotional", "napkins"],
)
class CustomNapkinsNowScraper(BaseScraper):
    BASE_URL = "https://www.customnapkinsnow.com"
    SITEMAP_URL = "https://www.customnapkinsnow.com/sitemap.xml"
    REQUEST_DELAY = 0.5
    REQUEST_TIMEOUT = 30
    SCHEMA_VERSION = 2

    # Sitemap traversal safety
    SITEMAP_MAX_DEPTH = 3

    # Category crawl safety
    CATEGORY_CRAWL_MAX_PAGES_PER_ROOT = 50
    CATEGORY_CRAWL_MAX_ROOTS = 200

    HEADERS = {
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/120.0.0.0 Safari/537.36"
        ),
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.9",
    }

    # Non-product paths we never want to treat as product detail URLs.
    # NOTE: /napkins-deals/ and /deal/ are INTENTIONALLY absent — deals are
    # products in this catalog.
    _NON_PRODUCT_PATHS = {
        "/page/",
        "/customer/",
        "/guides/",
        "/sample-product/",
        "/sample-products/",
    }

    _DEAL_PATH_MARKERS = ("/napkins-deals/", "/deal/")

    def __init__(self, payload: dict) -> None:
        super().__init__(payload)
        self.client = httpx.Client(
            headers=self.HEADERS,
            timeout=self.REQUEST_TIMEOUT,
            follow_redirects=True,
        )
        self.stats = {
            "products_found": 0,
            "products_scraped": 0,
            "products_skipped": 0,
            "products_failed": 0,
            "discovery_sitemap": 0,
            "discovery_homepage": 0,
            "discovery_category": 0,
            "deals_count": 0,
            # Phase 3 re-run efficiency counters
            "discovered": 0,
            "fetched": 0,
            "skipped_unchanged": 0,
        }
        # Phase 3 runtime config
        self.concurrency = _resolve_concurrency()
        self.force_refresh = _env_truthy(
            os.environ.get("CUSTOMNAPKINSNOW_FORCE_REFRESH")
        ) or bool((self.payload.extra or {}).get("force_refresh"))
        self._progress = _ProgressThrottle()
        # Lazily populated at catalog-scrape time; maps product_url -> stored doc
        self._existing_docs: Dict[str, Dict[str, Any]] = {}
        # Cooperative async cancellation flag (set from sync cancellation probe)
        self._async_cancelled: bool = False

    # ------------------------------------------------------------------
    # Entry point
    # ------------------------------------------------------------------

    def scrape(self) -> ScrapeResult:
        logger.info(
            "customnapkinsnow.scraper.started",
            schema_version=self.SCHEMA_VERSION,
            concurrency=self.concurrency,
            force_refresh=self.force_refresh,
        )

        try:
            products: List[Dict[str, Any]] = []
            is_partial = False

            # Single-product mode is triggered when a /product/ URL is present
            # either on the payload URL itself *or* overridden via
            # payload.extra["url"].  The API sets task.url to the scraper's
            # base_url (no /product/ segment), so the extra-override is the
            # supported way for tester/ops to trigger a single-product run
            # from the HTTP API without changing the orchestration contract.
            override_url = (self.payload.extra or {}).get("url")
            effective_url = override_url or self.payload.url

            if effective_url and "/product/" in effective_url:
                is_partial = True
                self._emit_progress(10, "Fetching product")
                product = self._scrape_product(
                    {
                        "url": self._canonicalize(effective_url),
                        "lastmod": None,
                        "source": "payload",
                    }
                )
                self._emit_progress(80, "Persisting")
                if product:
                    products.append(product)
                    self.stats["fetched"] = 1
                self.stats["discovered"] = 1
                self._emit_progress(100, "Done")
            else:
                self._emit_progress(0, "Discovering products")
                products, is_partial = self._scrape_catalog()
                self._emit_progress(100, "Run complete")

            return ScrapeResult(
                site=self.site_id,
                url=self.payload.url or self.BASE_URL,
                data={"products": products, "stats": self.stats},
                metadata={
                    "products_count": len(products),
                    "is_partial_scrape": is_partial,
                    "scraped_at": datetime.now(timezone.utc).isoformat(),
                    "schema_version": self.SCHEMA_VERSION,
                    "concurrency": self.concurrency,
                    "force_refresh": self.force_refresh,
                    "discovered": self.stats["discovered"],
                    "fetched": self.stats["fetched"],
                    "skipped_unchanged": self.stats["skipped_unchanged"],
                },
            )
        finally:
            self.client.close()

    # ------------------------------------------------------------------
    # Progress reporting — throttled wrapper around BaseScraper._report_progress
    # ------------------------------------------------------------------

    def _emit_progress(self, pct: int, message: str) -> None:
        if not self._progress.should_emit(pct):
            return
        logger.debug(
            "customnapkinsnow.progress.emitted",
            pct=pct,
            message=message,
        )
        self._report_progress(pct, message)

    # ------------------------------------------------------------------
    # Discovery
    # ------------------------------------------------------------------

    def _scrape_catalog(self) -> Tuple[List[Dict[str, Any]], bool]:
        product_urls = self._discover_product_urls()
        self.stats["products_found"] = len(product_urls)
        self.stats["discovered"] = len(product_urls)
        logger.info(
            "customnapkinsnow.products_discovered",
            count=len(product_urls),
            sitemap=self.stats["discovery_sitemap"],
            homepage=self.stats["discovery_homepage"],
            category=self.stats["discovery_category"],
            deals=self.stats["deals_count"],
        )
        self._emit_progress(5, f"Discovered {len(product_urls)} products")

        max_products = self.payload.extra.get("max_products", 0)
        is_partial = False
        if max_products and max_products > 0:
            product_urls = product_urls[:max_products]
            is_partial = True
            logger.warning(
                "customnapkinsnow.partial_scrape",
                max_products=max_products,
            )

        # Lastmod-based skip: bulk-load existing v2 docs so we can short-circuit
        # identical lastmods without an HTTP fetch.
        self._existing_docs = self._load_existing_docs(product_urls)

        # Split into "to_fetch" vs "to_skip_unchanged" based on lastmod.
        to_fetch: List[Dict[str, Any]] = []
        skipped_unchanged_docs: List[Dict[str, Any]] = []
        for info in product_urls:
            decision = self._classify_for_skip(info)
            if decision is not None:
                skipped_unchanged_docs.append(decision)
                self.stats["skipped_unchanged"] += 1
            else:
                to_fetch.append(info)

        if skipped_unchanged_docs:
            logger.info(
                "customnapkinsnow.lastmod.skipped",
                count=len(skipped_unchanged_docs),
                fetched=len(to_fetch),
                force_refresh=self.force_refresh,
            )

        # Concurrent fetch pool (async runloop, bounded)
        fetched_products: List[Dict[str, Any]] = asyncio.run(
            self._fetch_catalog_async(to_fetch)
        )
        self.stats["products_scraped"] = len(fetched_products)
        self.stats["fetched"] = len(fetched_products)

        # Return union: freshly-scraped v2 products + re-used unchanged docs.
        # The unchanged docs are hash-equal to the already-stored rows, so
        # the change-tracker will mark them "unchanged" (no spurious updates)
        # AND they'll be part of the "alive set" that gates soft-delete.
        merged: List[Dict[str, Any]] = [*fetched_products, *skipped_unchanged_docs]
        return merged, is_partial

    # ------------------------------------------------------------------
    # Lastmod-skip helpers
    # ------------------------------------------------------------------

    def _load_existing_docs(
        self, url_infos: List[Dict[str, Any]]
    ) -> Dict[str, Dict[str, Any]]:
        """
        Bulk-load existing ``scraped_products`` docs for the URLs we're about
        to (re-)scrape.  Only called when we have a live MongoDB handle
        injected by the worker.  Returns an empty dict otherwise.
        """
        if self._db is None or not url_infos:
            return {}
        urls = [info["url"] for info in url_infos]
        try:
            cursor = self._db.scraped_products.find(
                {"source": "customnapkinsnow", "product_url": {"$in": urls}},
            )
            return {doc["product_url"]: doc for doc in cursor}
        except Exception as e:
            logger.warning("customnapkinsnow.existing_docs.load_failed", error=str(e))
            return {}

    # Fields that are added by the worker's storage / change-tracking layer
    # and MUST be stripped before we re-feed a stored doc through
    # ``store_products_with_tracking`` as a "skipped unchanged" entry.  Leaving
    # them in would (a) perturb the re-computed data_hash and (b) collide with
    # the upsert's ``$setOnInsert: {created_at: ...}`` clause.
    _STORAGE_ONLY_FIELDS = frozenset(
        {
            "_id",
            "data_hash",
            "created_at",
            "updated_at",
            "last_task_id",
            "last_scraped_at",
            "scrape_count",
            "deleted_at",
        }
    )

    def _classify_for_skip(self, url_info: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """
        Decide if this URL can be skipped based on sitemap lastmod parity.

        Returns the existing stored doc (stripped of ``_id`` so it round-trips
        cleanly through the change-tracker as "unchanged") when the product
        can be skipped; otherwise returns ``None`` meaning "fetch it".

        Skip predicates (ALL must hold):
          1. Force-refresh is NOT active (neither env nor payload flag).
          2. The URL has a non-empty sitemap ``<lastmod>``.
          3. An existing stored doc is present for this URL.
          4. Stored ``schema_version`` is >= 2.
          5. Stored ``lastmod`` equals the discovered ``lastmod``.
        """
        if self.force_refresh:
            return None

        sitemap_lastmod = (url_info.get("lastmod") or "").strip()
        if not sitemap_lastmod:
            return None

        stored = self._existing_docs.get(url_info["url"])
        if not stored:
            return None

        if int(stored.get("schema_version") or 1) < 2:
            return None

        stored_lastmod = (stored.get("lastmod") or "").strip()
        if not stored_lastmod or stored_lastmod != sitemap_lastmod:
            return None

        reusable = {
            k: v for k, v in stored.items() if k not in self._STORAGE_ONLY_FIELDS
        }
        # Restore the canonical scraper-emitted 'source' value.  store_product
        # lowercases source in its $set payload but computes the stored
        # data_hash BEFORE that override, so the hash was taken over the
        # uppercased value the scraper originally emitted.  Re-feeding the
        # lowercased form would produce a different hash and mis-classify
        # this "unchanged" product as "updated".
        reusable["source"] = "CUSTOMNAPKINSNOW"
        logger.debug(
            "customnapkinsnow.lastmod.skip_decision",
            url=url_info["url"],
            lastmod=sitemap_lastmod,
            action="skip",
        )
        return reusable

    # ------------------------------------------------------------------
    # Concurrent fetch pool
    # ------------------------------------------------------------------

    async def _fetch_catalog_async(
        self, url_infos: List[Dict[str, Any]]
    ) -> List[Dict[str, Any]]:
        """
        Bounded-concurrency async fetch+parse for every URL in ``url_infos``.

        Design notes:
          * We use ``asyncio`` + ``httpx.AsyncClient`` because httpx is already
            on the dependency tree and asyncio gives clean cooperative
            cancellation.  Parsing remains synchronous (BeautifulSoup is
            CPU-light here; switching to threads buys nothing).
          * Concurrency is bounded by an ``asyncio.Semaphore``.
          * A single shared "next-allowed-start" timestamp enforces the
            per-slot politeness budget, giving an effective rate of roughly
            ``concurrency * 5 req/s``.
          * Cancellation is cooperative: we poll :py:meth:`_check_cancelled`
            at the start of every new slot.  Already-in-flight requests are
            allowed to complete but their results are discarded.
          * Per-product errors are isolated — they increment
            ``stats["products_failed"]`` and otherwise do not abort the batch.
        """
        if not url_infos:
            return []

        concurrency = self.concurrency
        semaphore = asyncio.Semaphore(concurrency)
        next_start = {"t": 0.0}  # shared monotonic timestamp
        pacing_lock = asyncio.Lock()
        total = len(url_infos)
        completed = 0
        results: List[Dict[str, Any]] = []

        logger.info(
            "customnapkinsnow.pool.scheduled",
            total=total,
            concurrency=concurrency,
        )

        async with httpx.AsyncClient(
            headers=self.HEADERS,
            timeout=self.REQUEST_TIMEOUT,
            follow_redirects=True,
            limits=httpx.Limits(
                max_connections=concurrency * 2,
                max_keepalive_connections=concurrency,
            ),
        ) as client:

            async def _worker(info: Dict[str, Any]):
                nonlocal completed
                # Cancellation gate BEFORE we grab a slot
                if self._async_cancelled:
                    return
                async with semaphore:
                    # Re-check after waiting for a slot (in case flag flipped)
                    if self._async_cancelled:
                        return

                    # Per-slot politeness: serialize request *starts* via a
                    # shared "next allowed start" stamp.  Each start advances
                    # the stamp by _PER_SLOT_MIN_DELAY_S / concurrency — so
                    # the effective global start-rate is concurrency / 0.2s.
                    async with pacing_lock:
                        now = asyncio.get_event_loop().time()
                        wait = next_start["t"] - now
                        if wait > 0:
                            await asyncio.sleep(wait)
                            now = asyncio.get_event_loop().time()
                        next_start["t"] = now + (_PER_SLOT_MIN_DELAY_S / concurrency)

                    # Now do the fetch outside the pacing lock
                    product = None
                    try:
                        product = await self._fetch_and_parse_async(client, info)
                    except TaskCancelledError:
                        self._async_cancelled = True
                    except Exception as exc:
                        logger.error(
                            "customnapkinsnow.pool.product_error",
                            url=info.get("url"),
                            error=str(exc),
                        )
                        self.stats["products_failed"] += 1

                    # Post-fetch accounting + cancellation re-check
                    completed += 1
                    # Cooperative mid-flight cancellation probe (polls Mongo)
                    try:
                        self._check_cancelled()
                    except TaskCancelledError:
                        self._async_cancelled = True

                    if product:
                        results.append(product)

                    pct = 5 + int(95 * completed / max(total, 1))
                    short = (info.get("url") or "").rsplit("/", 1)[-1][:60]
                    self._emit_progress(
                        pct,
                        f"Scraped {completed}/{total}: {short}",
                    )
                    logger.debug(
                        "customnapkinsnow.pool.completed",
                        url=info.get("url"),
                        completed=completed,
                        total=total,
                    )

            tasks = [asyncio.create_task(_worker(info)) for info in url_infos]
            try:
                await asyncio.gather(*tasks, return_exceptions=False)
            except Exception:
                # gather shouldn't raise since _worker swallows exceptions,
                # but belt-and-braces: cancel any stragglers and re-raise.
                for t in tasks:
                    if not t.done():
                        t.cancel()
                raise

        if self._async_cancelled:
            logger.info(
                "customnapkinsnow.pool.cancelled",
                completed=completed,
                total=total,
            )
            # Propagate cancellation up so the Celery task handles it.
            raise TaskCancelledError(
                f"Task {self._task_id} was cancelled during concurrent fetch"
            )

        return results

    async def _fetch_and_parse_async(
        self,
        client: httpx.AsyncClient,
        url_info: Dict[str, Any],
    ) -> Optional[Dict[str, Any]]:
        """Async sibling of :py:meth:`_scrape_product` (fetch) + sync parse."""
        url = url_info["url"]
        try:
            html = await self._fetch_html_async(client, url)
        except Exception as e:
            logger.error("customnapkinsnow.product.fetch_failed", url=url, error=str(e))
            self.stats["products_failed"] += 1
            return None

        if html is None:
            return None

        soup = BeautifulSoup(html, "lxml")
        h1 = soup.find("h1")
        if not h1 or len(h1.get_text(strip=True)) < 3:
            logger.info("customnapkinsnow.product.skipped_no_title", url=url)
            self.stats["products_skipped"] += 1
            return None

        try:
            return self._extract_product_data(url, soup, html, url_info)
        except Exception as e:
            logger.error("customnapkinsnow.product.parse_failed", url=url, error=str(e))
            self.stats["products_failed"] += 1
            return None

    @staticmethod
    def _is_retryable_exc(exc: BaseException) -> bool:
        return isinstance(exc, (httpx.TimeoutException, httpx.ConnectError))

    async def _fetch_html_async(
        self, client: httpx.AsyncClient, url: str
    ) -> Optional[str]:
        """
        Async fetch with the same retry policy as the sync path:
        3 attempts, exponential 2–10 s on timeout/connect errors.
        404 → returns ``None`` (caller treats as a valid "skip").
        """
        logger.info("customnapkinsnow.product.scraping", url=url)
        last_exc: Optional[Exception] = None
        for attempt in range(1, 4):
            try:
                resp = await client.get(url)
                if resp.status_code == 404:
                    logger.info("customnapkinsnow.product.not_found", url=url)
                    self.stats["products_skipped"] += 1
                    return None
                resp.raise_for_status()
                return resp.text
            except (httpx.TimeoutException, httpx.ConnectError) as e:
                last_exc = e
                if attempt == 3:
                    break
                backoff = min(10.0, 2.0 * attempt)
                logger.warning(
                    "customnapkinsnow.product.retry",
                    url=url,
                    attempt=attempt,
                    backoff=backoff,
                    error=str(e),
                )
                await asyncio.sleep(backoff)
            except Exception as e:
                last_exc = e
                break

        if last_exc:
            raise last_exc
        return None

    def _discover_product_urls(self) -> List[Dict[str, Any]]:
        seen: Set[str] = set()
        urls: List[Dict[str, Any]] = []

        # 1) Sitemap (with nested sitemapindex support, capped recursion)
        for item in self._parse_sitemap_recursive(self.SITEMAP_URL, depth=0):
            if item["url"] not in seen:
                seen.add(item["url"])
                item.setdefault("source", "sitemap")
                urls.append(item)
                self.stats["discovery_sitemap"] += 1

        # 2) Homepage scan
        for item in self._scan_homepage(seen):
            if item["url"] not in seen:
                seen.add(item["url"])
                urls.append(item)
                self.stats["discovery_homepage"] += 1

        # 3) Category root crawl
        for item in self._scan_category_roots(seen):
            if item["url"] not in seen:
                seen.add(item["url"])
                urls.append(item)
                self.stats["discovery_category"] += 1

        # Tag deal subtype + count
        for item in urls:
            if any(m in item["url"] for m in self._DEAL_PATH_MARKERS):
                item["product_subtype"] = "deal"
                self.stats["deals_count"] += 1
            else:
                item["product_subtype"] = "standard"

        return urls

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=2, max=10),
        retry=retry_if_exception_type((httpx.TimeoutException, httpx.ConnectError)),
        reraise=True,
    )
    def _fetch(self, url: str) -> httpx.Response:
        return self.client.get(url)

    def _parse_sitemap_recursive(
        self, sitemap_url: str, depth: int
    ) -> List[Dict[str, Any]]:
        """Walk a sitemap; if it's a <sitemapindex> recurse up to SITEMAP_MAX_DEPTH."""
        if depth > self.SITEMAP_MAX_DEPTH:
            logger.warning("customnapkinsnow.sitemap.max_depth", url=sitemap_url)
            return []

        try:
            response = self._fetch(sitemap_url)
            response.raise_for_status()
        except Exception as e:
            logger.error(
                "customnapkinsnow.sitemap.fetch_failed",
                url=sitemap_url,
                error=str(e),
            )
            return []

        try:
            root = ET.fromstring(response.text)
        except ET.ParseError as e:
            logger.error(
                "customnapkinsnow.sitemap.parse_failed",
                url=sitemap_url,
                error=str(e),
            )
            return []

        tag = root.tag.split("}", 1)[-1]  # strip ns
        if tag == "sitemapindex":
            found: List[Dict[str, Any]] = []
            ns = {"ns": "http://www.sitemaps.org/schemas/sitemap/0.9"}
            for sm in root.findall("ns:sitemap", ns):
                loc = sm.find("ns:loc", ns)
                if loc is not None and loc.text:
                    sub_url = loc.text.strip()
                    found.extend(self._parse_sitemap_recursive(sub_url, depth + 1))
            return found

        # urlset
        return self._parse_urlset(root)

    def _parse_urlset(self, root: ET.Element) -> List[Dict[str, Any]]:
        ns = {"ns": "http://www.sitemaps.org/schemas/sitemap/0.9"}
        found: List[Dict[str, Any]] = []
        seen: Set[str] = set()

        for url_el in root.findall("ns:url", ns):
            loc = url_el.find("ns:loc", ns)
            if loc is None or not loc.text:
                continue

            url = self._canonicalize(loc.text.strip())
            if not self._looks_like_product_url(url):
                continue

            # Skip category root pages (priority == 1.0)
            priority_val = 0.8
            priority_el = url_el.find("ns:priority", ns)
            if priority_el is not None and priority_el.text:
                try:
                    priority_val = float(priority_el.text)
                except ValueError:
                    pass
            if priority_val >= 1.0:
                continue

            if url in seen:
                continue
            seen.add(url)

            lastmod_el = url_el.find("ns:lastmod", ns)
            lastmod = lastmod_el.text if lastmod_el is not None else None

            found.append(
                {
                    "url": url,
                    "lastmod": lastmod,
                    "priority": priority_val,
                    "source": "sitemap",
                }
            )

        logger.info("customnapkinsnow.sitemap.parsed", count=len(found))
        return found

    def _scan_homepage(self, already_known: Set[str]) -> List[Dict[str, Any]]:
        extras: List[Dict[str, Any]] = []
        try:
            response = self._fetch(self.BASE_URL)
            response.raise_for_status()
            soup = BeautifulSoup(response.text, "lxml")

            for a in soup.find_all("a", href=True):
                full = self._canonicalize(urljoin(self.BASE_URL, a["href"]))
                if full not in already_known and self._looks_like_product_url(full):
                    extras.append(
                        {
                            "url": full,
                            "lastmod": None,
                            "priority": 0.8,
                            "source": "homepage",
                        }
                    )
        except Exception as e:
            logger.warning("customnapkinsnow.homepage_scan.failed", error=str(e))

        logger.info("customnapkinsnow.homepage_scan.extras", count=len(extras))
        return extras

    def _scan_category_roots(self, already_known: Set[str]) -> List[Dict[str, Any]]:
        """
        Walk every /product/<category>/ root page linked from the homepage and
        harvest /product/<category>/<slug> product detail URLs.  Follow
        WordPress-style pagination (/page/N/ or ?paged=N) until no new URLs
        appear or the safety cap is hit.
        """
        roots = self._find_category_roots()
        logger.info("customnapkinsnow.category_crawl.roots", count=len(roots))

        extras: List[Dict[str, Any]] = []
        seen_local: Set[str] = set(already_known)

        for root_url in roots[: self.CATEGORY_CRAWL_MAX_ROOTS]:
            page_num = 1
            while page_num <= self.CATEGORY_CRAWL_MAX_PAGES_PER_ROOT:
                page_url = self._category_page_url(root_url, page_num)
                added_this_page = 0
                try:
                    resp = self._fetch(page_url)
                    if resp.status_code != 200:
                        break
                    soup = BeautifulSoup(resp.text, "lxml")
                    for a in soup.find_all("a", href=True):
                        full = self._canonicalize(urljoin(page_url, a["href"]))
                        if full not in seen_local and self._looks_like_product_url(
                            full
                        ):
                            seen_local.add(full)
                            extras.append(
                                {
                                    "url": full,
                                    "lastmod": None,
                                    "priority": 0.8,
                                    "source": "category",
                                }
                            )
                            added_this_page += 1
                except Exception as e:
                    logger.warning(
                        "customnapkinsnow.category_crawl.page_failed",
                        url=page_url,
                        error=str(e),
                    )
                    break

                logger.info(
                    "customnapkinsnow.category_crawl.page",
                    root=root_url,
                    page=page_num,
                    added=added_this_page,
                )

                if page_num == 1 and added_this_page == 0:
                    # Page 1 empty → nothing to paginate through
                    break
                if page_num > 1 and added_this_page == 0:
                    break
                page_num += 1
                time.sleep(self.REQUEST_DELAY)

            logger.info("customnapkinsnow.category_crawl.done", root=root_url)

        logger.info("customnapkinsnow.category_crawl.total_extras", count=len(extras))
        return extras

    def _find_category_roots(self) -> List[str]:
        """Collect /product/<category>/ URLs linked from the homepage."""
        roots: List[str] = []
        seen: Set[str] = set()
        try:
            resp = self._fetch(self.BASE_URL)
            resp.raise_for_status()
            soup = BeautifulSoup(resp.text, "lxml")
            for a in soup.find_all("a", href=True):
                full = self._canonicalize(urljoin(self.BASE_URL, a["href"]))
                if self.BASE_URL not in full:
                    continue
                parts = [p for p in urlparse(full).path.split("/") if p]
                if len(parts) == 2 and parts[0] == "product" and full not in seen:
                    seen.add(full)
                    roots.append(full)
        except Exception as e:
            logger.warning("customnapkinsnow.category_roots.failed", error=str(e))
        return roots

    @staticmethod
    def _category_page_url(root_url: str, page_num: int) -> str:
        if page_num <= 1:
            return root_url
        # Try /page/N/ first — WordPress default
        base = root_url.rstrip("/")
        return f"{base}/page/{page_num}/"

    @staticmethod
    def _canonicalize(url: str) -> str:
        """Strip query+fragment, normalize trailing slash."""
        u = url.split("?", 1)[0].split("#", 1)[0]
        # Normalize trailing slash for comparability
        if u.endswith("/") and u.count("/") > 3:
            u = u.rstrip("/")
        return u

    def _looks_like_product_url(self, url: str) -> bool:
        if self.BASE_URL not in url:
            return False
        if any(p in url for p in self._NON_PRODUCT_PATHS):
            return False
        parts = [p for p in urlparse(url).path.split("/") if p]
        return len(parts) >= 3 and parts[0] == "product"

    # ------------------------------------------------------------------
    # Per-product scraping
    # ------------------------------------------------------------------

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=2, max=10),
        retry=retry_if_exception_type((httpx.TimeoutException, httpx.ConnectError)),
        reraise=True,
    )
    def _scrape_product(self, url_info: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        url = url_info["url"]
        logger.info("customnapkinsnow.product.scraping", url=url)

        try:
            response = self.client.get(url)
            if response.status_code == 404:
                logger.info("customnapkinsnow.product.not_found", url=url)
                self.stats["products_skipped"] += 1
                return None
            response.raise_for_status()
            html = response.text
            soup = BeautifulSoup(html, "lxml")

            h1 = soup.find("h1")
            if not h1 or len(h1.get_text(strip=True)) < 3:
                logger.info("customnapkinsnow.product.skipped_no_title", url=url)
                self.stats["products_skipped"] += 1
                return None

            return self._extract_product_data(url, soup, html, url_info)

        except Exception as e:
            logger.error("customnapkinsnow.product.failed", url=url, error=str(e))
            return None

    # ------------------------------------------------------------------
    # Master extractor
    # ------------------------------------------------------------------

    def _extract_product_data(
        self,
        url: str,
        soup: BeautifulSoup,
        html: str,
        url_info: Dict[str, Any],
    ) -> Dict[str, Any]:
        product: Dict[str, Any] = {
            "source": "CUSTOMNAPKINSNOW",
            "product_url": url,
            "schema_version": self.SCHEMA_VERSION,
            "scraped_at": datetime.now(timezone.utc).isoformat(),
            "lastmod": url_info.get("lastmod"),
            "product_subtype": url_info.get("product_subtype", "standard"),
            "discovery_source": url_info.get("source"),
        }

        # --- Name ---
        h1 = soup.find("h1")
        if h1:
            product["name"] = h1.get_text(strip=True)
        else:
            title = soup.find("title")
            if title:
                product["name"] = (
                    title.text.replace(" - CustomNapkinsNow.Com", "")
                    .replace(" - Custom Napkins Now", "")
                    .strip()
                )

        # --- Category from URL ---
        parsed = urlparse(url)
        path_parts = [p for p in parsed.path.split("/") if p]
        if len(path_parts) > 1:
            product["category"] = path_parts[1].replace("-", " ").title()
        if len(path_parts) > 2:
            product["subcategory"] = path_parts[2].replace("-", " ").title()
        slug = path_parts[-1] if path_parts else ""

        # --- Meta description ---
        meta_desc = soup.find("meta", {"name": "description"})
        if meta_desc:
            product["meta_description"] = (meta_desc.get("content") or "").strip()

        # --- Description ---
        product["description"] = self._extract_description(soup)

        # --- Specs ---
        product.update(self._extract_specifications(soup))

        # --- SKU + database_tag ---
        sku, db_tag, sku_source = self._extract_sku(soup, html)
        if sku:
            product["product_sku"] = sku
        if db_tag:
            product["database_tag"] = db_tag
        logger.debug("customnapkinsnow.sku.source", url=url, source=sku_source, sku=sku)

        # --- Option groups (structured) ---
        option_groups = self._extract_option_groups(soup)
        product["options"] = option_groups
        product["option_groups_detected"] = [
            g for g, opts in option_groups.items() if opts
        ]

        # --- Structured colors (merged napkin + imprint) ---
        colors = self._flatten_colors(option_groups)
        product["colors"] = colors

        # --- Legacy flat color list (labels) for back-compat ---
        legacy_labels = [c["label"] for c in colors if c.get("label")]
        if not legacy_labels:
            # Fallback to v1 heuristics if no Step-based colors found
            legacy_labels = self._legacy_extract_colors(soup)
        product["available_colors"] = legacy_labels

        # --- Base tiers (from tblPrice) ---
        base_tiers = self._extract_base_tiers(soup)
        base_tiers = self._apply_tier_sanity_flags(base_tiers, context=url)
        product["base_tiers"] = base_tiers

        # --- Variants (cartesian over configured axes) ---
        variants = self._build_variants(option_groups, base_tiers, slug, db_tag)
        product["variants"] = variants

        if not variants:
            logger.warning("customnapkinsnow.variant.js_missing", url=url)

        # --- Legacy flat pricing + print_methods + min_order_qty ---
        product["pricing"] = self._legacy_flatten_pricing(variants)
        product["print_methods"] = sorted(
            {
                (v["print_method"] or {}).get("label", "Standard")
                for v in variants
                if (v.get("print_method") or v.get("tiers"))
            }
        )
        min_qty = self._legacy_min_order_qty(variants, base_tiers)
        if min_qty is not None:
            product["min_order_qty"] = min_qty

        # --- Images ---
        images = self._extract_images(soup, html)
        if images:
            product["main_image"] = images[0]
            product["gallery_images"] = images[1:] if len(images) > 1 else []

        return product

    # ------------------------------------------------------------------
    # Description
    # ------------------------------------------------------------------

    def _extract_description(self, soup: BeautifulSoup) -> Optional[str]:
        product_name = ""
        h1 = soup.find("h1")
        if h1:
            product_name = h1.get_text(strip=True)

        for selector in [
            {"class": "pdescription-content"},
            {"class": "product-descriptions"},
        ]:
            div = soup.find("div", selector)
            if not div:
                continue
            text = div.get_text(separator="\n", strip=True)
            if len(text) < 30:
                continue
            if product_name and text.startswith(product_name):
                text = text[len(product_name) :].lstrip("\n").strip()
            spec_idx = text.find("SPECIFICATIONS")
            if spec_idx > 0:
                text = text[:spec_idx].strip()
            if len(text) > 10:
                return text[:5000]
        return None

    # ------------------------------------------------------------------
    # Specifications (v1-compatible)
    # ------------------------------------------------------------------

    def _extract_specifications(self, soup: BeautifulSoup) -> Dict[str, Any]:
        specs: Dict[str, Any] = {}

        for table in soup.find_all("table"):
            headers = [th.get_text(strip=True).lower() for th in table.find_all("th")]
            if "specification" in " ".join(headers) or "details" in " ".join(headers):
                for row in table.find_all("tr"):
                    cells = row.find_all(["td", "th"])
                    if len(cells) >= 2:
                        key = cells[0].get_text(strip=True).lower()
                        value = cells[1].get_text(strip=True)
                        if "material" in key:
                            specs["material"] = value
                        elif "size" in key or "dimension" in key:
                            specs["size"] = value

        desc_div = soup.find("div", class_="pdescription-content")
        if desc_div and "material" not in specs:
            text = desc_div.get_text(separator="\n")
            m = re.search(r"Material\s*:?\s*([^\n]+)", text, re.IGNORECASE)
            if m:
                specs["material"] = m.group(1).strip()

        if "size" not in specs:
            text_content = soup.get_text()
            size_patterns = [
                r"(\d+\.?\d*\s*(?:x|X|×)\s*\d+\.?\d*\s*(?:in|inch|cm|mm|ft))",
                r"(\d+\s*(?:in|inch)\s*(?:x|X|×)\s*\d+\s*(?:in|inch))",
            ]
            for pat in size_patterns:
                match = re.search(pat, text_content, re.IGNORECASE)
                if match:
                    candidate = match.group(1).strip()
                    if 3 < len(candidate) < 40:
                        specs["size"] = candidate
                        break

        if "material" not in specs:
            material_keywords = [
                "polypropylene",
                "vinyl",
                "cotton",
                "polyester",
                "paper",
                "fabric",
                "plastic",
                "linen",
                "cloth",
            ]
            text_lower = soup.get_text().lower()
            for kw in material_keywords:
                if kw in text_lower:
                    specs["material"] = kw.title()
                    break

        return specs

    # ------------------------------------------------------------------
    # SKU detection
    # ------------------------------------------------------------------

    def _extract_sku(
        self, soup: BeautifulSoup, html: str
    ) -> Tuple[Optional[str], Optional[str], str]:
        """
        Returns (sku, database_tag, source_label).  source_label is one of:
          - "json_sku"     : real SKU mined from inline JSON
          - "itemprop_sku" : schema.org itemprop
          - "data_sku"     : data-sku attribute
          - "db_tag"       : synthesized from database_tag / data-product-id
          - "none"         : nothing found
        """
        db_tag: Optional[str] = None
        m = re.search(r'database_tag"\s*:\s*"([^"]+)"', html)
        if m:
            db_tag = m.group(1)
        if not db_tag:
            m = re.search(r'data-product-id="(\d+)"', html)
            if m:
                db_tag = m.group(1)

        # 1) Inline JSON "sku":"..."
        m = re.search(r'"sku"\s*:\s*"([^"]+)"', html)
        if m and m.group(1).strip():
            return m.group(1).strip(), db_tag, "json_sku"

        # 2) schema.org itemprop
        el = soup.find(attrs={"itemprop": "sku"})
        if el:
            val = (el.get("content") or el.get_text(strip=True) or "").strip()
            if val:
                return val, db_tag, "itemprop_sku"

        # 3) data-sku attribute
        el = soup.find(attrs={"data-sku": True})
        if el:
            val = (el.get("data-sku") or "").strip()
            if val:
                return val, db_tag, "data_sku"

        # 4) schema.org Product in ld+json
        for script in soup.find_all("script", attrs={"type": "application/ld+json"}):
            try:
                payload = json.loads(script.string or "")
            except Exception:
                continue
            blobs = payload if isinstance(payload, list) else [payload]
            for blob in blobs:
                if isinstance(blob, dict) and blob.get("@type") == "Product":
                    s = blob.get("sku")
                    if s:
                        return str(s), db_tag, "ldjson_sku"

        # 5) Synthesize from db_tag
        if db_tag:
            return f"SKU{db_tag}", db_tag, "db_tag"

        return None, None, "none"

    # ------------------------------------------------------------------
    # Option groups (the real variant axes)
    # ------------------------------------------------------------------

    def _extract_option_groups(
        self, soup: BeautifulSoup
    ) -> Dict[str, List[Dict[str, Any]]]:
        """
        Walk the DOM in document order.  Each time we encounter a step
        heading (`<h3|h4|h5 class*="badgeh*">Step N …</h3>`), we latch its
        text as the *current* step label.  Each time we encounter an option
        <div> bearing `data-product-option-value-id`, we file it under the
        current step's classified group.

        This traversal-based approach is robust to BeautifulSoup / lxml not
        recording `sourceline` on every node.
        """
        groups: Dict[str, List[Dict[str, Any]]] = {
            "print_method": [],
            "napkin": [],
            "imprint": [],
            "ply_or_size": [],
            "orientation": [],
            "style": [],
            "other": [],
        }
        seen_option_ids_per_group: Dict[str, Set[str]] = {g: set() for g in groups}

        current_step_label = ""
        step_re = re.compile(r"^\s*Step\s+\d+\b", re.IGNORECASE)

        for el in soup.descendants:
            # Only element nodes have .name / .attrs
            if getattr(el, "name", None) is None:
                continue

            # 1) Heading update?
            if el.name in ("h2", "h3", "h4", "h5"):
                classes = " ".join(el.get("class") or [])
                if "badgeh" in classes.lower():
                    txt = el.get_text(" ", strip=True)
                    if step_re.match(txt):
                        current_step_label = txt
                        continue
                else:
                    # Defensive fallback: any heading text starting with "Step N "
                    txt = el.get_text(" ", strip=True)
                    if re.match(r"^\s*Step\s+\d+\s+\S", txt, re.IGNORECASE):
                        current_step_label = txt
                        continue

            # 2) Option element?
            if el.get("data-product-option-value-id") is not None:
                classes = " ".join(el.get("class") or [])
                # Skip hidden rush-shipping quantity inputs (dup of swatch divs)
                if "form-control" in classes and "quantities" in classes:
                    continue

                opt_label = (
                    el.get("data-original-option-value-name")
                    or el.get_text(" ", strip=True)
                    or ""
                ).strip()
                if not opt_label:
                    continue

                option_value_id = el.get("data-product-option-value-id") or ""
                group_key = _classify_group(current_step_label)

                dedup = f"{option_value_id}:{opt_label}"
                if dedup in seen_option_ids_per_group[group_key]:
                    continue
                seen_option_ids_per_group[group_key].add(dedup)

                entry = {
                    "label": opt_label,
                    "option_value_id": option_value_id,
                    "option_id": el.get("data-option-id"),
                    "price_modifier": self._safe_float(el.get("data-price", 0)),
                    "swatch_image_url": self._swatch_image_url(el),
                    "step_label": current_step_label[:80],
                }
                groups[group_key].append(entry)

                logger.debug(
                    (
                        "customnapkinsnow.color.group_detected"
                        if group_key in ("napkin", "imprint")
                        else "customnapkinsnow.option.group_detected"
                    ),
                    group=group_key,
                    step=current_step_label[:50],
                    label=entry["label"],
                    option_value_id=option_value_id,
                )

        return groups

    @staticmethod
    def _swatch_image_url(opt_el) -> Optional[str]:
        img = opt_el.find("img")
        if img is not None:
            src = img.get("data-src") or img.get("src")
            if src and "static.customnapkinsnow.com" in src:
                return src
        # data-img attr (raw filename)
        raw = opt_el.get("data-img")
        if raw:
            return f"https://static.customnapkinsnow.com/fit-in/400x400/{raw}"
        return None

    @staticmethod
    def _safe_float(v: Any, default: float = 0.0) -> float:
        try:
            if isinstance(v, (int, float)):
                return float(v)
            if isinstance(v, str):
                return float(v.replace("$", "").replace(",", "").strip() or 0)
        except (ValueError, TypeError):
            pass
        return default

    def _flatten_colors(
        self, option_groups: Dict[str, List[Dict[str, Any]]]
    ) -> List[Dict[str, Any]]:
        colors: List[Dict[str, Any]] = []
        for grp in ("napkin", "imprint"):
            for opt in option_groups.get(grp, []):
                colors.append(
                    {
                        "label": opt["label"],
                        "swatch_image_url": opt.get("swatch_image_url"),
                        "option_value_id": opt.get("option_value_id"),
                        "option_id": opt.get("option_id"),
                        "group": grp,
                        "price_modifier": opt.get("price_modifier", 0.0),
                    }
                )
        return colors

    def _legacy_extract_colors(self, soup: BeautifulSoup) -> List[str]:
        """v1 fallback — only used if no structured color Step is found."""
        colors: List[str] = []
        for table in soup.find_all("table"):
            for row in table.find_all("tr"):
                cells = row.find_all(["td", "th"])
                if len(cells) >= 2:
                    key = cells[0].get_text(strip=True).lower()
                    if "available color" in key or "colour" in key:
                        # Prefer a <br>/newline-aware split from the raw HTML
                        raw_html = cells[1].decode_contents()
                        normalised = re.sub(r"<br[^>]*>", "\n", raw_html, flags=re.I)
                        normalised_txt = BeautifulSoup(normalised, "lxml").get_text(
                            "\n", strip=True
                        )
                        parts = re.split(r"[\n,•/;]+", normalised_txt)
                        # If we still ended up with one giant mash, split by
                        # capital-letter word boundaries as a last resort.
                        if len(parts) <= 1 and parts and len(parts[0]) > 40:
                            parts = re.findall(
                                r"[A-Z][a-z]+(?:\s+[A-Z][a-z]+)?",
                                parts[0],
                            )
                        colors.extend([c.strip() for c in parts if c.strip()])
                        break
            if colors:
                break
        # dedup preserving order
        seen: Set[str] = set()
        out: List[str] = []
        for c in colors:
            if c not in seen:
                seen.add(c)
                out.append(c)
        return out[:30]

    # ------------------------------------------------------------------
    # Base tiers (tblPrice)
    # ------------------------------------------------------------------

    def _extract_base_tiers(self, soup: BeautifulSoup) -> List[Dict[str, Any]]:
        tiers: List[Dict[str, Any]] = []

        table = soup.find("table", id="tblPrice")
        if table:
            rows = table.find_all("tr")
            if len(rows) >= 2:
                header_cells = rows[0].find_all(["td", "th"])
                price_cells = rows[1].find_all(["td", "th"])
                # qty/price cells are aligned; first cell is label ("Qty"/"Price")
                for hc, pc in zip(header_cells[1:], price_cells[1:]):
                    qty_txt = hc.get_text(strip=True)
                    pr_txt = pc.get_text(strip=True)

                    qty_m = re.match(r"^(\d[\d,]*)", qty_txt.replace(",", ""))
                    price_m = re.search(r"\$?\s*([\d.]+)", pr_txt.replace(",", ""))
                    if not qty_m or not price_m:
                        continue
                    try:
                        qty = int(qty_m.group(1))
                        price = float(price_m.group(1))
                    except ValueError:
                        continue
                    tiers.append({"quantity": qty, "unit_price": price})

        # Fallback: cells with id="price_connect_<qty>"
        if not tiers:
            for cell in soup.find_all("td", id=re.compile(r"^price_connect_\d+$")):
                m = re.match(r"^price_connect_(\d+)$", cell.get("id", ""))
                if not m:
                    continue
                try:
                    qty = int(m.group(1))
                    pm = re.search(r"\$?\s*([\d.]+)", cell.get_text(strip=True))
                    if not pm:
                        continue
                    price = float(pm.group(1))
                    tiers.append({"quantity": qty, "unit_price": price})
                except (ValueError, TypeError):
                    continue

        # Deduplicate & sort by quantity
        by_qty: Dict[int, float] = {}
        for t in tiers:
            by_qty[t["quantity"]] = t["unit_price"]
        return [{"quantity": q, "unit_price": p} for q, p in sorted(by_qty.items())]

    def _apply_tier_sanity_flags(
        self, tiers: List[Dict[str, Any]], context: str = ""
    ) -> List[Dict[str, Any]]:
        """
        Adaptive flagging (NOT dropping):
          - is_sample: quantity == 1 AND there are >= 2 tiers with qty >= 25.
          - is_outlier: unit_price > 10x median of the non-sample tiers.
        """
        if not tiers:
            return tiers

        has_multi = sum(1 for t in tiers if t["quantity"] >= 25) >= 2

        non_sample_prices = [
            t["unit_price"] for t in tiers if not (t["quantity"] == 1 and has_multi)
        ]
        med = median(non_sample_prices) if non_sample_prices else 0.0

        out: List[Dict[str, Any]] = []
        for t in tiers:
            entry = dict(t)
            entry["is_sample"] = entry["quantity"] == 1 and has_multi
            if med > 0 and entry["unit_price"] > 10 * med:
                entry["is_outlier"] = True
                logger.warning(
                    "customnapkinsnow.pricing.outlier_tier",
                    url=context,
                    quantity=entry["quantity"],
                    unit_price=entry["unit_price"],
                    median=med,
                )
            else:
                entry["is_outlier"] = False
            out.append(entry)
        return out

    # ------------------------------------------------------------------
    # Variants (cartesian over axes × base tiers)
    # ------------------------------------------------------------------

    def _build_variants(
        self,
        option_groups: Dict[str, List[Dict[str, Any]]],
        base_tiers: List[Dict[str, Any]],
        slug: str,
        db_tag: Optional[str],
    ) -> List[Dict[str, Any]]:
        axes_order = ("print_method", "napkin", "imprint", "ply_or_size")
        present_axes = [a for a in axes_order if option_groups.get(a)]

        # When no axes at all, emit a single "base" variant.
        if not present_axes:
            if not base_tiers:
                return []
            return [
                self._make_variant(
                    axes_values={},
                    base_tiers=base_tiers,
                    slug=slug,
                    db_tag=db_tag,
                )
            ]

        # Cartesian product
        combos: List[Dict[str, Dict[str, Any]]] = [{}]
        for axis in present_axes:
            new_combos: List[Dict[str, Dict[str, Any]]] = []
            opts = option_groups[axis]
            for c in combos:
                for opt in opts:
                    nc = dict(c)
                    nc[axis] = opt
                    new_combos.append(nc)
                    if len(new_combos) >= _VARIANT_SOFT_CAP:
                        break
                if len(new_combos) >= _VARIANT_SOFT_CAP:
                    break
            combos = new_combos
            if len(combos) >= _VARIANT_SOFT_CAP:
                logger.warning(
                    "customnapkinsnow.variant.soft_cap_hit",
                    cap=_VARIANT_SOFT_CAP,
                    axes=present_axes,
                )
                combos = combos[:_VARIANT_SOFT_CAP]
                break

        variants = [self._make_variant(c, base_tiers, slug, db_tag) for c in combos]
        for v in variants[:5]:
            logger.debug(
                "customnapkinsnow.variant.parsed",
                key=v["variant_key"],
                sku=v["sku"],
                tier_count=len(v["tiers"]),
            )
        return variants

    def _make_variant(
        self,
        axes_values: Dict[str, Dict[str, Any]],
        base_tiers: List[Dict[str, Any]],
        slug: str,
        db_tag: Optional[str],
    ) -> Dict[str, Any]:
        def axis_ref(axis_name: str) -> Optional[Dict[str, Any]]:
            o = axes_values.get(axis_name)
            if not o:
                return None
            return {"label": o["label"], "option_value_id": o.get("option_value_id")}

        price_modifier_total = float(
            sum(
                self._safe_float(o.get("price_modifier", 0))
                for o in axes_values.values()
            )
        )

        variant_tuple_parts = [slug or (db_tag or "product")]
        for axis in ("print_method", "napkin", "imprint", "ply_or_size"):
            o = axes_values.get(axis)
            if o:
                variant_tuple_parts.append(f"{axis}:{o['label']}")

        sku_human = "|".join(
            part.replace(" ", "-").replace("/", "-") for part in variant_tuple_parts
        )
        variant_key = hashlib.sha1(sku_human.encode()).hexdigest()[:16]

        return {
            "variant_key": variant_key,
            "sku": sku_human,
            "print_method": axis_ref("print_method"),
            "napkin_color": axis_ref("napkin"),
            "imprint_color": axis_ref("imprint"),
            "ply_or_size": axis_ref("ply_or_size"),
            "price_modifier_total": round(price_modifier_total, 2),
            "tiers": [dict(t) for t in base_tiers],
        }

    # ------------------------------------------------------------------
    # Legacy flatteners
    # ------------------------------------------------------------------

    def _legacy_flatten_pricing(
        self, variants: List[Dict[str, Any]]
    ) -> List[Dict[str, Any]]:
        """
        Collapse variants across non-print-method axes and return v1-style
        [{print_method, quantity, unit_price}].  Deduped by (print_method, qty).
        Samples and outliers are kept (no drop), matching v1's qty > 1 output.
        """
        seen: Set[Tuple[str, int]] = set()
        out: List[Dict[str, Any]] = []
        for v in variants:
            pm = (v.get("print_method") or {}).get("label") or "Standard"
            for t in v.get("tiers", []):
                # v1 skipped qty==1 (sample) in its flat list
                if t.get("is_sample") or t.get("quantity", 0) <= 1:
                    continue
                key = (pm, t["quantity"])
                if key in seen:
                    continue
                seen.add(key)
                out.append(
                    {
                        "print_method": pm,
                        "quantity": t["quantity"],
                        "unit_price": t["unit_price"],
                    }
                )
        return sorted(out, key=lambda x: (x["print_method"], x["quantity"]))

    @staticmethod
    def _legacy_min_order_qty(
        variants: List[Dict[str, Any]], base_tiers: List[Dict[str, Any]]
    ) -> Optional[int]:
        qtys: List[int] = []
        for v in variants:
            for t in v.get("tiers", []):
                if not t.get("is_sample"):
                    qtys.append(t["quantity"])
        if not qtys:
            qtys = [t["quantity"] for t in base_tiers if t["quantity"] > 1]
        return min(qtys) if qtys else None

    # ------------------------------------------------------------------
    # Images (ported from v1)
    # ------------------------------------------------------------------

    def _extract_images(self, soup: BeautifulSoup, html: str) -> List[str]:
        images: List[str] = []
        seen: Set[str] = set()

        js_pattern = r'custom_ads_data\s*=\s*\{[^}]*"images"\s*:\s*\[(.*?)\]'
        match = re.search(js_pattern, html, re.DOTALL)
        if match:
            for url in re.findall(r'"(https?:[^"]+)"', match.group(1)):
                clean = url.replace("\\/", "/")
                if not any(
                    skip in clean.lower()
                    for skip in [".mp4", ".webm", ".svg", "loader"]
                ):
                    if clean not in seen:
                        seen.add(clean)
                        images.append(clean)

        if not images:
            for img in soup.find_all("img", attrs={"data-src": True}):
                src = img.get("data-src", "")
                if (
                    "static.customnapkinsnow.com" in src
                    and self._is_valid_product_image(src)
                    and src not in seen
                ):
                    seen.add(src)
                    images.append(src)

        if not images:
            pattern = (
                r"https?://static\.customnapkinsnow\.com"
                r'/fit-in/\d+x\d+/[^\s"\'<>]+\.(?:webp|jpg|jpeg|png)'
            )
            for m in re.finditer(pattern, html, re.IGNORECASE):
                url = m.group(0)
                if self._is_valid_product_image(url) and url not in seen:
                    seen.add(url)
                    images.append(url)

        return images

    @staticmethod
    def _is_valid_product_image(url: str) -> bool:
        if not url:
            return False
        url_lower = url.lower()
        exclude = [
            "blank.png",
            "loader-",
            ".svg",
            "/cache/blank",
            "placeholder",
            "icon",
            "logo",
        ]
        return not any(p in url_lower for p in exclude)

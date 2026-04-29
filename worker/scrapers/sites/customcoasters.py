"""
CustomCoasters Scraper - Worker Integration

Scrapes full product catalog from customcoastersnow.com via sitemap.
Extracts per-shape pricing tables, descriptions, specifications,
imprint styles, and images.
"""

import re
import json
import time
import structlog
from datetime import datetime, timezone
from typing import Dict, Any, List, Optional, Set
from urllib.parse import urljoin
import xml.etree.ElementTree as ET

import httpx
from bs4 import BeautifulSoup
from tenacity import (
    retry,
    retry_if_exception_type,
    stop_after_attempt,
    wait_exponential,
)

from shared.exceptions import ScrapeFailedError
from shared.scrapers.base import BaseScraper, ScrapeResult
from shared.scrapers.registry import register_scraper

logger = structlog.get_logger(__name__)


@register_scraper(
    site_id="customcoasters",
    name="CustomCoastersNow",
    base_url="https://www.customcoastersnow.com",
    description="Scrapes custom coaster products",
    tags=["ecommerce", "promotional", "coasters"],
)
class CustomCoastersScraper(BaseScraper):
    """
    CustomCoastersNow product scraper.

    Uses sitemap to discover all product URLs (unlimited), validates each
    page has a price chart, and extracts full per-shape pricing matrix,
    descriptions, specifications, imprint styles, and images.
    """

    BASE_URL = "https://www.customcoastersnow.com"
    SITEMAP_URL = "https://www.customcoastersnow.com/sitemap.xml"
    REQUEST_DELAY = 0.5
    REQUEST_TIMEOUT = 30

    HEADERS = {
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/120.0.0.0 Safari/537.36"
        ),
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.9",
    }

    # Sitemap paths that are non-product pages or have no static pricing
    _NON_PRODUCT_PATHS = {
        "/guides",
        "/page/",
        "/customer/",
        "/coasters-deals/",
        "/sample-product/",
    }

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
        }

    def scrape(self) -> ScrapeResult:
        """Execute CustomCoasters scraping."""
        logger.info("customcoasters.scraper.started")

        try:
            products = []
            is_partial = False  # flag: True when max_products cap is applied

            if self.payload.url and "/product/" in self.payload.url:
                # Single-product mode — always partial, never triggers bulk deletion
                is_partial = True
                product = self._scrape_product(
                    {"url": self.payload.url, "lastmod": None}
                )
                if product:
                    products.append(product)
            else:
                products, is_partial = self._scrape_from_sitemap()

            return ScrapeResult(
                site=self.site_id,
                url=self.payload.url or self.BASE_URL,
                data={"products": products, "stats": self.stats},
                metadata={
                    "products_count": len(products),
                    "is_partial_scrape": is_partial,
                    "scraped_at": datetime.now(timezone.utc).isoformat(),
                },
            )
        finally:
            self.client.close()

    # ------------------------------------------------------------------ #
    # Sitemap discovery
    # ------------------------------------------------------------------ #

    def _scrape_from_sitemap(self) -> tuple:
        """
        Discover and scrape all product pages from the sitemap.

        Returns:
            (products list, is_partial bool)
            is_partial=True when max_products cap was applied — callers should
            NOT trigger bulk deletion in this case.
        """
        products = []

        product_urls = self._parse_sitemap()
        self.stats["products_found"] = len(product_urls)
        logger.info("customcoasters.sitemap.parsed", count=len(product_urls))

        # Optional cap for testing (default: unlimited)
        max_products = self.payload.extra.get("max_products", 0)
        is_partial = False
        if max_products and max_products > 0:
            product_urls = product_urls[:max_products]
            is_partial = True
            logger.warning(
                "customcoasters.partial_scrape",
                max_products=max_products,
                note="Deletion tracking disabled for partial runs",
            )

        for url_info in product_urls:
            product = self._scrape_product(url_info)
            if product:
                products.append(product)
                self.stats["products_scraped"] += 1
            else:
                self.stats["products_failed"] += 1

            time.sleep(self.REQUEST_DELAY)

        return products, is_partial

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=2, max=10),
        retry=retry_if_exception_type((httpx.TimeoutException, httpx.ConnectError)),
        reraise=True,
    )
    def _parse_sitemap(self) -> List[Dict]:
        """
        Parse sitemap and return URL infos for product pages only.

        Filters out:
        - Category root pages (priority == 1.0)
        - Guide / page / customer URLs
        - Bare /product/ root
        """
        response = self.client.get(self.SITEMAP_URL)
        response.raise_for_status()

        products: List[Dict] = []
        seen_urls: Set[str] = set()

        root = ET.fromstring(response.text)
        ns = {"ns": "http://www.sitemaps.org/schemas/sitemap/0.9"}

        for url_el in root.findall("ns:url", ns):
            loc = url_el.find("ns:loc", ns)
            if loc is None or not loc.text:
                continue

            url = loc.text.strip()

            # Must be a /product/ URL
            if "/product/" not in url:
                continue

            # Skip non-product paths
            if any(p in url for p in self._NON_PRODUCT_PATHS):
                continue

            # Skip bare category roots (priority == 1.0)
            priority_val = 0.8
            priority_el = url_el.find("ns:priority", ns)
            if priority_el is not None and priority_el.text:
                try:
                    priority_val = float(priority_el.text)
                except ValueError:
                    pass

            if priority_val >= 1.0:
                continue

            # Deduplicate
            if url in seen_urls:
                continue
            seen_urls.add(url)

            lastmod = None
            lastmod_el = url_el.find("ns:lastmod", ns)
            if lastmod_el is not None:
                lastmod = lastmod_el.text

            products.append({"url": url, "lastmod": lastmod, "priority": priority_val})

        return products

    # ------------------------------------------------------------------ #
    # Per-product scraping
    # ------------------------------------------------------------------ #

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=2, max=10),
        retry=retry_if_exception_type((httpx.TimeoutException, httpx.ConnectError)),
        reraise=True,
    )
    def _scrape_product(self, url_info: Dict) -> Optional[Dict]:
        """Fetch a single product page and extract all data."""
        url = url_info["url"]
        logger.info("customcoasters.product.scraping", url=url)

        try:
            response = self.client.get(url)
            response.raise_for_status()
            soup = BeautifulSoup(response.text, "lxml")

            # Skip pages that don't have a real pricing chart (content/SEO pages)
            price_section = soup.find("div", class_="price_chart")
            if not price_section or not price_section.find("table"):
                logger.info("customcoasters.product.skipped_no_price", url=url)
                self.stats["products_skipped"] += 1
                return None

            # Skip deal/dynamic pages: price_chart hidden AND no actual price cells
            is_hidden = "display:none" in (price_section.get("style") or "").replace(
                " ", ""
            )
            has_price_cells = bool(
                price_section.find("td", id=lambda x: x and "price_connect" in x)
            )
            if is_hidden and not has_price_cells:
                logger.info("customcoasters.product.skipped_dynamic_pricing", url=url)
                self.stats["products_skipped"] += 1
                return None

            return self._extract_product_data(url, soup, url_info)

        except Exception as e:
            logger.error("customcoasters.product.failed", url=url, error=str(e))
            return None

    # ------------------------------------------------------------------ #
    # Data extraction
    # ------------------------------------------------------------------ #

    def _extract_product_data(
        self, url: str, soup: BeautifulSoup, url_info: Dict
    ) -> Dict:
        """Extract all structured product data from parsed HTML."""

        product: Dict[str, Any] = {
            "source": "CUSTOMCOASTERSNOW",
            "product_url": url,
            "scraped_at": datetime.now(timezone.utc).isoformat(),
            "lastmod": url_info.get("lastmod"),
        }

        # --- Name ---
        h1 = soup.find("h1")
        if h1:
            product["name"] = h1.get_text(strip=True)
        else:
            title = soup.find("title")
            if title:
                product["name"] = (
                    title.text.replace(" - CustomCoastersNow.Com", "")
                    .replace(" - Custom Coasters Now", "")
                    .strip()
                )

        # --- Category from URL ---
        path_parts = [p for p in url.replace(self.BASE_URL, "").split("/") if p]
        # path_parts[0] == "product"
        if len(path_parts) > 1:
            product["category"] = path_parts[1].replace("-", " ").title()
        if len(path_parts) > 2 and path_parts[2] != "custom":
            product["subcategory"] = path_parts[2].replace("-", " ").title()
        elif len(path_parts) > 3:
            product["subcategory"] = path_parts[3].replace("-", " ").title()

        # --- Meta description ---
        meta_desc = soup.find("meta", {"name": "description"})
        if meta_desc:
            product["meta_description"] = meta_desc.get("content", "").strip()

        # --- Description ---
        product["description"] = self._extract_description(soup)

        # --- Specifications ---
        specs = self._extract_specifications(soup)
        product.update(specs)

        # --- Imprint styles ---
        product["imprint_styles"] = self._extract_imprint_styles(soup)

        # --- Pricing (flat, shape × size × qty) ---
        product["pricing"] = self._extract_pricing(soup)

        # --- Derived fields from pricing ---
        if product["pricing"]:
            product["min_order_qty"] = min(r["quantity"] for r in product["pricing"])
            product["available_shapes"] = sorted(
                {r["shape"] for r in product["pricing"]}
            )

        # --- Images ---
        images = self._extract_images(soup)
        if images:
            product["main_image"] = images[0]
            product["gallery_images"] = images[1:] if len(images) > 1 else []

        return product

    # ------------------------------------------------------------------ #
    # Description
    # ------------------------------------------------------------------ #

    def _extract_description(self, soup: BeautifulSoup) -> Optional[str]:
        """Extract product description text, excluding the product title and specs section."""
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

            # Strip product name if it appears at the very start of the text
            if product_name and text.startswith(product_name):
                text = text[len(product_name) :].lstrip("\n").strip()

            # Trim everything from SPECIFICATIONS onwards
            spec_idx = text.find("SPECIFICATIONS")
            if spec_idx > 0:
                text = text[:spec_idx].strip()

            if len(text) > 10:
                return text[:5000]
        return None

    # ------------------------------------------------------------------ #
    # Specifications
    # ------------------------------------------------------------------ #

    def _extract_specifications(self, soup: BeautifulSoup) -> Dict:
        """Extract material, thickness, shapes, and sizes from the description section."""
        specs: Dict[str, Any] = {}

        desc_div = soup.find("div", class_="pdescription-content")
        if not desc_div:
            return specs

        text = desc_div.get_text(separator="\n")

        # Material
        m = re.search(r"Material\s*:\s*([^\n]+)", text, re.IGNORECASE)
        if m:
            specs["material"] = m.group(1).strip()

        # Thickness
        t = re.search(r"Thickness\s*:\s*([^\n]+)", text, re.IGNORECASE)
        if t:
            specs["thickness"] = t.group(1).strip()

        # Shapes (from SPECIFICATIONS block)
        sh = re.search(r"Shapes\s*:\s*([^\n]+)", text, re.IGNORECASE)
        if sh:
            specs["shapes"] = sh.group(1).strip()

        return specs

    # ------------------------------------------------------------------ #
    # Imprint styles
    # ------------------------------------------------------------------ #

    def _extract_imprint_styles(self, soup: BeautifulSoup) -> List[str]:
        """
        Extract available imprint methods from the customization form.

        Captures options from the print/imprint-method select elements,
        excluding generic placeholder text and shape/size/quantity selectors.
        """
        styles: List[str] = []
        seen: Set[str] = set()

        # Keywords that indicate an imprint method / print technology
        imprint_keywords = {
            "matte",
            "glossy",
            "deboss",
            "foil",
            "emboss",
            "digital",
            "full color",
            "full-color",
            "4 color",
            "screen",
            "photo",
            "one color",
            "two color",
            "imprint",
            "print",
            "front",
            "back",
            "side",
        }
        # Generic noise to skip
        skip_phrases = {"--please select--", "select", "choose"}

        for opt in soup.find_all("option", attrs={"data-option-value-id": True}):
            text = opt.get_text(strip=True)
            if not text or text.lower() in skip_phrases:
                continue
            if any(kw in text.lower() for kw in imprint_keywords):
                if text not in seen:
                    seen.add(text)
                    styles.append(text)

        return styles

    # ------------------------------------------------------------------ #
    # Pricing
    # ------------------------------------------------------------------ #

    def _extract_pricing(self, soup: BeautifulSoup) -> List[Dict]:
        """
        Extract pricing as a flat list for CSV-export convenience.

        Each row: {shape, size, quantity, unit_price}

        Reads the price_chart section → shape tabs → per-shape tables.
        Falls back to scanning all tables if the structured section is absent.
        """
        pricing: List[Dict] = []

        price_section = soup.find("div", class_="price_chart")
        if price_section:
            pricing = self._extract_structured_pricing(price_section)

        # Fallback: scan all tables on the page
        if not pricing:
            pricing = self._extract_fallback_pricing(soup)

        return pricing

    def _extract_structured_pricing(self, price_section: BeautifulSoup) -> List[Dict]:
        """
        Parse pricing from the canonical price_chart div.

        Two layouts exist on the site:
        1. Shape-tab layout  – nav with '#nav-price-*' links → tab-content panes
        2. Single/flat table – no nav, one or more tables directly in price_chart
        """
        pricing: List[Dict] = []

        # Look for the shape-selection nav (links with href="#nav-price-...")
        shape_nav = None
        shape_links: List = []
        for nav in price_section.find_all("nav"):
            links = nav.find_all("a", href=re.compile(r"^#nav-price-"))
            if links:
                shape_nav = nav
                shape_links = links
                break

        tab_content = price_section.find("div", id="nav-tabContent")

        if shape_nav and tab_content:
            # Shape-tab layout
            for link in shape_links:
                shape = link.get_text(strip=True)
                pane_href = link.get("href", "").lstrip("#")
                if not pane_href:
                    continue
                pane = tab_content.find("div", id=pane_href)
                if not pane:
                    continue
                table = pane.find("table")
                if not table:
                    continue
                pricing.extend(self._parse_price_table(table, shape=shape))
        else:
            # Single / flat layout – parse every table in the price_chart section
            for table in price_section.find_all("table"):
                pricing.extend(self._parse_price_table(table, shape="Default"))

        return pricing

    def _parse_price_table(
        self, table: BeautifulSoup, shape: str = "Default"
    ) -> List[Dict]:
        """
        Parse one price table into flat {shape, size, quantity, unit_price} rows.

        Quantity extraction strategy (in priority order):
        1. Cell id="price_connect_{option_id}_{qty}"  – most reliable
        2. Cell id="price_connect_{qty}"              – simple premade tables
        3. Header column index                        – fallback

        Cells without actual $ prices (e.g. 'For the best pricing') are skipped.
        qty < 2 placeholder cells (often $0.01 fillers) are skipped.
        """
        rows_data: List[Dict] = []

        rows = table.find_all("tr")
        if len(rows) < 2:
            return rows_data

        # --- Build qty-per-column map from header row (fallback) ---
        header_cells = rows[0].find_all(["th", "td"])
        qty_from_col: Dict[int, int] = {}  # col_index → qty
        for col_idx, cell in enumerate(header_cells[1:], start=1):
            text = cell.get_text(strip=True)
            m = re.match(r"^(\d+)", text)
            if m:
                qty_from_col[col_idx] = int(m.group(1))

        # Non-size labels in first column (flat single-row tables like premade products)
        _NON_SIZE_LABELS = {
            "price",
            "qty",
            "size",
            "size / qty",
            "material / qty",
            "qty / price",
        }

        # --- Parse data rows ---
        for row in rows[1:]:
            if "price_chart_size_row" not in row.get("class", []):
                continue

            cells = row.find_all(["th", "td"])
            if not cells:
                continue

            size = cells[0].get_text(strip=True)
            if not size:
                continue

            # Normalise non-size labels: flat single-row table where first cell is "Price" or "Qty"
            if size.lower() in _NON_SIZE_LABELS:
                size = "Standard"

            # -- Method 1: ID-based quantity extraction --
            found_id_cells = False
            for cell in cells[1:]:
                cell_id = cell.get("id", "")
                # Matches: price_connect_35128_50  OR  price_connect_50
                m = re.match(r"^price_connect_(?:\d+_)?(\d+)$", cell_id)
                if not m:
                    continue
                found_id_cells = True
                qty = int(m.group(1))
                if qty < 2:
                    continue  # skip placeholder qty=1 cells
                price_text = cell.get_text(strip=True)
                pm = re.search(r"\$?(\d+\.?\d*)", price_text)
                if not pm:
                    continue
                try:
                    price = float(pm.group(1))
                    if 0.001 <= price <= 10000:
                        rows_data.append(
                            {
                                "shape": shape,
                                "size": size,
                                "quantity": qty,
                                "unit_price": price,
                            }
                        )
                except ValueError:
                    pass

            # -- Method 2: Header column fallback --
            if not found_id_cells and qty_from_col:
                for col_idx, qty in qty_from_col.items():
                    if qty < 2:
                        continue
                    if col_idx >= len(cells):
                        break
                    price_text = cells[col_idx].get_text(strip=True)
                    pm = re.search(r"\$?(\d+\.?\d*)", price_text)
                    if not pm:
                        continue
                    try:
                        price = float(pm.group(1))
                        if 0.001 <= price <= 10000:
                            rows_data.append(
                                {
                                    "shape": shape,
                                    "size": size,
                                    "quantity": qty,
                                    "unit_price": price,
                                }
                            )
                    except ValueError:
                        pass

        return rows_data

    def _extract_fallback_pricing(self, soup: BeautifulSoup) -> List[Dict]:
        """
        Fallback pricing for pages where price_chart div is absent.
        Scans all page tables and returns rows from the first valid one.
        """
        pricing: List[Dict] = []
        for table in soup.find_all("table"):
            if "$" not in table.get_text():
                continue
            rows = self._parse_price_table(table, shape="Default")
            if rows:
                pricing.extend(rows)
                break
        return pricing

    # ------------------------------------------------------------------ #
    # Images
    # ------------------------------------------------------------------ #

    def _extract_images(self, soup: BeautifulSoup) -> List[str]:
        """Extract product-specific images (lazy-loaded, 500x500 static CDN)."""
        images: List[str] = []
        seen: Set[str] = set()

        # Primary: lazy-loaded data-src attributes pointing to the static CDN
        for img in soup.find_all("img", attrs={"data-src": True}):
            src = img.get("data-src", "")
            if (
                "static.customcoastersnow.com" in src
                and "/product_" in src
                and self._is_valid_product_image(src)
                and src not in seen
            ):
                seen.add(src)
                images.append(src)

        # Secondary: regex scan for any remaining fit-in URLs not in data-src
        if not images:
            html_text = str(soup)
            pattern = (
                r"https?://static\.customcoastersnow\.com"
                r'/fit-in/\d+x\d+/product_[^\s"\'<>]+\.(?:webp|jpg|jpeg|png)'
            )
            for match in re.finditer(pattern, html_text, re.IGNORECASE):
                url = match.group(0)
                if self._is_valid_product_image(url) and url not in seen:
                    seen.add(url)
                    images.append(url)

        return images

    def _is_valid_product_image(self, url: str) -> bool:
        """Return True only for real product images (no loaders / icons / SVGs)."""
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
            "upsells_",
            "guide_",
        ]
        return not any(p in url_lower for p in exclude)

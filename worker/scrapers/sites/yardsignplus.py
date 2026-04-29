"""
YardSignPlus Scraper - Worker Integration

Scrapes yard sign products from yardsignplus.com.
Extracts JSON-LD and embedded React props for pricing.
"""

import re
import json
import html as html_module
import time
import structlog
from datetime import datetime, timezone
from typing import Dict, Any, List, Optional
from decimal import Decimal
from urllib.parse import urljoin, urlparse

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
    site_id="yardsignplus",
    name="YardSignPlus",
    base_url="https://www.yardsignplus.com",
    description="Scrapes yard sign and banner products",
    tags=["ecommerce", "signage", "yard-signs"],
)
class YardSignPlusScraper(BaseScraper):
    """
    YardSignPlus product scraper.
    
    Uses sitemap for discovery and extracts JSON-LD + embedded pricing.
    """
    
    BASE_URL = "https://www.yardsignplus.com"
    REQUEST_DELAY = 1.0
    REQUEST_TIMEOUT = 30
    
    HEADERS = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    }
    
    def __init__(self, payload: dict) -> None:
        super().__init__(payload)
        self.client = httpx.Client(headers=self.HEADERS, timeout=self.REQUEST_TIMEOUT)
        self.stats = {
            "products_found": 0,
            "products_scraped": 0,
            "products_failed": 0,
        }
    
    def scrape(self) -> ScrapeResult:
        """Execute YardSignPlus scraping."""
        logger.info("yardsignplus.scraper.started")
        
        try:
            products = []
            
            if self.payload.url:
                # Scrape single product
                product = self._scrape_product(self.payload.url)
                if product:
                    products.append(product)
            else:
                # Scrape from sitemap
                products = self._scrape_from_sitemap()
            
            return ScrapeResult(
                site=self.site_id,
                url=self.payload.url or self.BASE_URL,
                data={
                    "products": products,
                    "stats": self.stats,
                },
                metadata={
                    "products_count": len(products),
                    "scraped_at": datetime.now(timezone.utc).isoformat(),
                },
            )
        finally:
            self.client.close()
    
    def _scrape_from_sitemap(self) -> List[Dict]:
        """Scrape products from sitemap."""
        products = []
        
        # Get product URLs from sitemap
        product_urls = self._get_product_urls()
        self.stats["products_found"] = len(product_urls)
        
        logger.info("yardsignplus.sitemap.parsed", count=len(product_urls))
        
        # Limit products per task
        max_products = self.payload.extra.get("max_products", 0)
        product_urls = product_urls[:max_products]
        
        for url in product_urls:
            product = self._scrape_product(url)
            if product:
                products.append(product)
                self.stats["products_scraped"] += 1
            else:
                self.stats["products_failed"] += 1
            
            time.sleep(self.REQUEST_DELAY)
        
        return products
    
    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=2, max=10),
        retry=retry_if_exception_type((httpx.TimeoutException, httpx.ConnectError)),
        reraise=True,
    )
    def _get_product_urls(self) -> List[str]:
        """Get product URLs from sitemap."""
        sitemap_url = f"{self.BASE_URL}/sitemap.xml"
        response = self.client.get(sitemap_url)
        response.raise_for_status()
        
        soup = BeautifulSoup(response.text, "lxml-xml")
        urls = [loc.text for loc in soup.find_all("loc")]
        
        # Filter product URLs (those with /shop/)
        return [url for url in urls if "/shop/" in url]
    
    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=2, max=10),
        retry=retry_if_exception_type((httpx.TimeoutException, httpx.ConnectError)),
        reraise=True,
    )
    def _scrape_product(self, product_url: str) -> Optional[Dict]:
        """Scrape a single product page."""
        logger.info("yardsignplus.product.scraping", url=product_url)
        
        try:
            response = self.client.get(product_url)
            response.raise_for_status()
            
            html_content = response.text
            
            # Extract JSON-LD
            json_ld = self._extract_json_ld(html_content)
            if not json_ld:
                logger.warning("yardsignplus.product.no_json_ld", url=product_url)
                return None
            
            # Extract size from URL
            size = self._extract_size_from_url(product_url)
            
            # Extract images
            image_data = self._extract_images(html_content, size)
            
            # Extract pricing
            pricing_tiers = self._extract_pricing(html_content, size)
            
            # Map to structured data
            return self._map_product_data(
                json_ld, product_url, pricing_tiers, image_data
            )
            
        except Exception as e:
            logger.error("yardsignplus.product.failed", url=product_url, error=str(e))
            return None
    
    def _extract_json_ld(self, html: str) -> Optional[Dict]:
        """Extract JSON-LD product data."""
        try:
            soup = BeautifulSoup(html, "lxml")
            json_ld_script = soup.find("script", {"type": "application/ld+json"})
            
            if json_ld_script and json_ld_script.string:
                data = json.loads(json_ld_script.string)
                if data.get("@type") == "Product":
                    return data
            
            return None
        except Exception as e:
            logger.error("yardsignplus.json_ld.failed", error=str(e))
            return None
    
    def _extract_images(self, html: str, size: str = None) -> Dict:
        """Extract product images from HTML."""
        result = {"main_image": None, "gallery_images": []}
        
        try:
            soup = BeautifulSoup(html, "lxml")
            
            # Try embedded React props
            container = soup.find("div", id="product-editor-container")
            if container:
                props_value = container.get("data-symfony--ux-react--react-props-value")
                if props_value:
                    decoded = html_module.unescape(props_value)
                    data = json.loads(decoded)
                    
                    product_data = data.get("product", {})
                    product_images = product_data.get("productImages", [])
                    
                    if product_images:
                        result["main_image"] = product_images[0]
                        result["gallery_images"] = product_images[1:]
                        return result
                    
                    # Check variants for size-specific image
                    variants = product_data.get("variants", [])
                    if size and variants:
                        size_normalized = size.lower()
                        for variant in variants:
                            if variant.get("name", "").lower() == size_normalized:
                                if variant.get("image"):
                                    result["main_image"] = variant["image"]
                                    return result
                    
                    # Use first variant image
                    if variants and variants[0].get("image"):
                        result["main_image"] = variants[0]["image"]
                        return result
            
            # Fallback to JSON-LD
            json_ld_script = soup.find("script", {"type": "application/ld+json"})
            if json_ld_script:
                try:
                    data = json.loads(json_ld_script.string)
                    if data.get("image"):
                        result["main_image"] = data["image"]
                except json.JSONDecodeError:
                    pass
            
            return result
            
        except Exception as e:
            logger.error("yardsignplus.images.failed", error=str(e))
            return result
    
    def _extract_pricing(self, html: str, size: str = None) -> List[Dict]:
        """Extract pricing tiers from embedded JSON."""
        try:
            soup = BeautifulSoup(html, "lxml")
            container = soup.find("div", id="product-editor-container")
            
            if not container:
                return []
            
            props_value = container.get("data-symfony--ux-react--react-props-value")
            if not props_value:
                return []
            
            decoded = html_module.unescape(props_value)
            data = json.loads(decoded)
            
            product_data = data.get("product", {})
            pricing_info = product_data.get("pricing", {})
            
            if not pricing_info:
                return []
            
            variants = pricing_info.get("variants", {})
            quantities = pricing_info.get("quantities", [])
            
            # Get pricing key
            pricing_key = None
            if size:
                pricing_key = f"pricing_{size.lower()}"
            
            if not pricing_key or pricing_key not in variants:
                if variants:
                    pricing_key = list(variants.keys())[0]
            
            if not pricing_key or pricing_key not in variants:
                return []
            
            variant_pricing = variants[pricing_key]
            pricing_tiers_data = variant_pricing.get("pricing", {})
            
            pricing_tiers = []
            for qty in quantities:
                qty_key = f"qty_{qty}"
                if qty_key in pricing_tiers_data:
                    tier_data = pricing_tiers_data[qty_key]
                    pricing_tiers.append({
                        "quantity": tier_data["qty"]["from"],
                        "price": float(tier_data["usd"]),
                    })
            
            return pricing_tiers
            
        except Exception as e:
            logger.error("yardsignplus.pricing.failed", error=str(e))
            return []
    
    def _extract_size_from_url(self, url: str) -> Optional[str]:
        """Extract size from product URL."""
        size_match = re.search(r"(\d+x\d+)", url)
        if size_match:
            return size_match.group(1)
        
        if "variant=" in url:
            variant_match = re.search(r"variant=([^&]+)", url)
            if variant_match:
                return variant_match.group(1)
        
        return None
    
    def _map_product_data(
        self,
        json_ld: Dict,
        product_url: str,
        pricing_tiers: List[Dict],
        image_data: Dict,
    ) -> Dict:
        """Map extracted data to structured format."""
        name = json_ld.get("name", "")
        description = json_ld.get("description", "")
        brand_data = json_ld.get("brand", {})
        brand = brand_data.get("name") if isinstance(brand_data, dict) else None
        
        offers = json_ld.get("offers", {})
        availability = offers.get("availability", "")
        
        # Extract category from URL
        parsed = urlparse(product_url)
        path_parts = [p for p in parsed.path.split("/") if p]
        category = path_parts[0].replace("-", " ").title() if path_parts else None
        subcategory = path_parts[2].replace("-", " ").title() if len(path_parts) > 2 else None
        
        # Extract product ID
        product_id = None
        match = re.search(r"/([A-Z]+\d+|CUSTOM)(?:\?|$)", product_url)
        if match:
            product_id = match.group(1)
        
        return {
            "source": "YARDSIGNPLUS",
            "product_id": product_id,
            "name": name,
            "description": description,
            "brand": brand,
            "category": category,
            "subcategory": subcategory,
            "size": self._extract_size_from_url(product_url),
            "main_image": image_data.get("main_image"),
            "gallery_images": image_data.get("gallery_images", []),
            "pricing": pricing_tiers,
            "is_active": "InStock" in availability,
            "material": "Corrugated Plastic (Coroplast)" if "sign" in name.lower() else None,
            "product_url": product_url,
            "scraped_at": datetime.now(timezone.utc).isoformat(),
        }

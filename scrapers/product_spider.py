"""
Discovers all product URLs on josephperrier.com and scrapes each product page.
Returns a list of parsed product dicts.
"""

import asyncio
import json
import re
from typing import Optional

from playwright.async_api import async_playwright, Page

from scrapers.product_parser import parse_product_page

BASE = "https://www.josephperrier.com"

# All product slugs confirmed from nav exploration.
# Spider will also crawl the listing page to catch any additions.
KNOWN_SLUGS = [
    "cuvee-royale-brut",
    "cuvee-royale-brut-nature",
    "cuvee-royale-brut-blanc-de-blancs",
    "cuvee-royale-brut-rose",
    "cuvee-royale-vintage-2018",
    "cuvee-royale-demi-sec",
    "cuvee-ciergelot-2020",
    "la-cote-a-bras-2016",
    "josephine-2014",
    "cuvee-200",
]

PRODUCT_BASE_PATH = "/en/champagnes-et-cuvees/"


async def _discover_product_urls(page: Page) -> list[str]:
    """
    Collect product URLs from:
      1. The hardcoded known list (seed)
      2. The nav mega-menu CHAMPAGNES links
      3. The /champagnes/ listing page
    Returns deduplicated, ordered URLs.
    """
    urls = [f"{BASE}{PRODUCT_BASE_PATH}{slug}/" for slug in KNOWN_SLUGS]

    # Crawl the main champagnes listing page for any extras
    try:
        await page.goto(f"{BASE}/en/champagnes/", wait_until="networkidle", timeout=30000)
        await page.wait_for_timeout(1500)
        found = await page.evaluate("""() =>
            Array.from(document.querySelectorAll('a[href]'))
                .map(a => a.href)
                .filter(h => h.includes('/champagnes-et-cuvees/') && !h.includes('#'))
        """)
        for u in found:
            # Normalise to /en/ version
            u_en = re.sub(r"josephperrier\.com/(?!en/)", "josephperrier.com/en/", u)
            u_en = u_en.rstrip("/") + "/"
            if u_en not in urls:
                urls.append(u_en)
    except Exception as e:
        print(f"  [warn] listing page crawl failed: {e}")

    return urls


async def _scrape_product(page: Page, url: str) -> Optional[dict]:
    print(f"  Scraping: {url}")
    try:
        await page.goto(url, wait_until="networkidle", timeout=40000)
        await page.wait_for_timeout(1500)

        # Body text
        body_text = await page.evaluate("() => document.body.innerText")

        # Raw HTML (for BeautifulSoup if needed later)
        html = await page.content()

        # WooCommerce variation JSON
        wc_json = await page.evaluate("""() => {
            const form = document.querySelector('form.variations_form[data-product_variations]');
            return form ? form.getAttribute('data-product_variations') : null;
        }""")

        # Additional images (product gallery)
        images = await page.evaluate("""() =>
            Array.from(document.querySelectorAll(
                'figure.woocommerce-product-gallery__wrapper img, ' +
                '.woocommerce-product-gallery img, ' +
                'img[class*="wp-image"]'
            ))
            .map(i => ({
                url: i.src || i.getAttribute('data-src'),
                alt: i.alt || '',
                width_px: i.naturalWidth || null,
                height_px: i.naturalHeight || null,
            }))
            .filter(i => i.url && i.url.includes('josephperrier.com'))
        """)

        parsed = parse_product_page(html, url, body_text, wc_json or "")
        parsed["raw_images"] = images
        return parsed

    except Exception as e:
        print(f"  [error] {url}: {e}")
        return {"source_url": url, "error": str(e)}


async def crawl_all_products() -> list[dict]:
    """Entry point: returns a list of parsed product dicts."""
    results = []
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        page = await browser.new_page()

        print("Discovering product URLs...")
        urls = await _discover_product_urls(page)
        print(f"Found {len(urls)} product URLs")

        for url in urls:
            product = await _scrape_product(page, url)
            if product:
                results.append(product)

        await browser.close()

    return results

"""
Exploration script: crawl josephperrier.com/en and map all products + site structure.
Outputs a JSON report to output/exploration.json.
"""

import asyncio
import json
import re
from pathlib import Path
from playwright.async_api import async_playwright

BASE_URL = "https://www.josephperrier.com/en"
OUTPUT = Path("output/exploration.json")

async def get_all_links(page, url: str) -> dict:
    await page.goto(url, wait_until="domcontentloaded", timeout=30000)
    await page.wait_for_timeout(1500)
    return await page.evaluate("""() => {
        return Array.from(document.querySelectorAll('a[href]'))
            .map(a => ({ text: a.innerText.trim(), href: a.href }))
            .filter(l => l.href.includes('josephperrier.com'));
    }""")

async def scrape_product_page(page, url: str) -> dict:
    await page.goto(url, wait_until="domcontentloaded", timeout=30000)
    await page.wait_for_timeout(1500)

    data = await page.evaluate("""() => {
        const getText = sel => {
            const el = document.querySelector(sel);
            return el ? el.innerText.trim() : null;
        };
        const getAll = sel =>
            Array.from(document.querySelectorAll(sel)).map(e => e.innerText.trim());
        const getSrc = sel => {
            const el = document.querySelector(sel);
            return el ? (el.src || el.getAttribute('data-src') || el.srcset) : null;
        };
        const getAllSrc = sel =>
            Array.from(document.querySelectorAll(sel))
                .map(e => e.src || e.getAttribute('data-src') || e.srcset)
                .filter(Boolean);

        return {
            title:       document.title,
            h1:          getText('h1'),
            h2s:         getAll('h2'),
            description: getText('meta[name="description"]') ||
                         document.querySelector('meta[name="description"]')?.getAttribute('content'),
            body_text:   document.body.innerText.substring(0, 4000),
            images:      getAllSrc('img'),
            links:       Array.from(document.querySelectorAll('a[href]'))
                             .map(a => ({ text: a.innerText.trim(), href: a.href }))
                             .filter(l => l.href.includes('josephperrier.com')),
        };
    }""")
    data["url"] = url
    return data

async def main():
    OUTPUT.parent.mkdir(exist_ok=True)
    report = {"base_url": BASE_URL, "pages": {}, "products": [], "nav_links": []}

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        page = await browser.new_page()

        # --- 1. Homepage ---
        print("Visiting homepage...")
        homepage = await scrape_product_page(page, BASE_URL)
        report["pages"]["homepage"] = homepage

        # --- 2. Collect nav links ---
        nav_links = [l for l in homepage["links"] if "/en" in l["href"]]
        # Deduplicate
        seen = set()
        unique_nav = []
        for l in nav_links:
            if l["href"] not in seen:
                seen.add(l["href"])
                unique_nav.append(l)
        report["nav_links"] = unique_nav
        print(f"Found {len(unique_nav)} unique links on homepage")

        # --- 3. Find product/champagne listing pages ---
        candidate_sections = [
            l for l in unique_nav
            if any(kw in l["href"].lower() for kw in [
                "champagne", "cuvee", "cuvée", "wine", "product", "collection",
                "wines", "shop", "catalog"
            ])
        ]
        # Also try common URL patterns
        known_paths = [
            "/en/champagnes",
            "/en/our-champagnes",
            "/en/collection",
            "/en/wines",
            "/en/products",
            "/en/cuvees",
        ]
        for path in known_paths:
            url = f"https://www.josephperrier.com{path}"
            if not any(l["href"] == url for l in candidate_sections):
                candidate_sections.append({"text": "(probe)", "href": url})

        report["candidate_sections"] = candidate_sections
        print(f"Candidate section pages: {[l['href'] for l in candidate_sections]}")

        # --- 4. Visit each candidate section ---
        product_urls = set()
        for link in candidate_sections:
            url = link["href"]
            print(f"  Probing: {url}")
            try:
                await page.goto(url, wait_until="domcontentloaded", timeout=20000)
                await page.wait_for_timeout(1000)
                status = page.url  # follow redirects
                links_on_page = await page.evaluate("""() =>
                    Array.from(document.querySelectorAll('a[href]'))
                        .map(a => a.href)
                        .filter(h => h.includes('josephperrier.com'))
                """)
                # Heuristic: product URLs tend to be deeper paths
                for l in links_on_page:
                    if re.search(r'/en/[^/]+/[^/]+', l):
                        product_urls.add(l)
                report["pages"][url] = {
                    "final_url": status,
                    "links_found": len(links_on_page),
                    "body_snippet": await page.evaluate("() => document.body.innerText.substring(0, 800)"),
                }
            except Exception as e:
                report["pages"][url] = {"error": str(e)}

        print(f"\nPotential product URLs found: {len(product_urls)}")
        report["potential_product_urls"] = sorted(product_urls)

        # --- 5. Visit up to 5 product pages for deep inspection ---
        sample_products = sorted(product_urls)[:8]
        for url in sample_products:
            print(f"  Deep-scraping: {url}")
            try:
                data = await scrape_product_page(page, url)
                report["products"].append(data)
            except Exception as e:
                report["products"].append({"url": url, "error": str(e)})

        await browser.close()

    OUTPUT.write_text(json.dumps(report, indent=2, ensure_ascii=False))
    print(f"\nReport saved to {OUTPUT}")

    # --- Summary ---
    print("\n=== SUMMARY ===")
    print(f"Nav links on homepage: {len(report['nav_links'])}")
    print(f"Potential product URLs: {len(report['potential_product_urls'])}")
    print("\nAll nav links:")
    for l in report["nav_links"]:
        print(f"  [{l['text'][:40]}]  {l['href']}")
    print("\nPotential product URLs:")
    for u in report["potential_product_urls"]:
        print(f"  {u}")

asyncio.run(main())

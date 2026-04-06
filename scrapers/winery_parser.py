"""
Scrapes winery-level pages (histoire, cave, vignoble) to populate the Winery table.
"""

import re
import asyncio
from playwright.async_api import async_playwright, Page

WINERY_PAGES = {
    "histoire":    "https://www.josephperrier.com/en/maison/histoire/",
    "famille":     "https://www.josephperrier.com/en/maison/famille/",
    "cave":        "https://www.josephperrier.com/en/maison/cave/",
    "vignoble":    "https://www.josephperrier.com/en/maison/vignoble/",
    "savoir_faire":"https://www.josephperrier.com/en/maison/savoirs-faire/",
}

TIMEOUT_MS = 60000  # 60 s — histoire page is slow


def _find_number(text: str, pattern: str) -> re.Match:
    return re.search(pattern, text, re.IGNORECASE)


async def _get_body(page: Page, url: str) -> str:
    for attempt in range(2):
        try:
            await page.goto(url, wait_until="domcontentloaded", timeout=TIMEOUT_MS)
            await page.wait_for_timeout(2500)
            return await page.evaluate("() => document.body.innerText")
        except Exception as e:
            if attempt == 0:
                print(f"  [retry] {url}: {e}")
            else:
                print(f"  [warn] Could not fetch {url}: {e}")
    return ""


async def scrape_winery() -> dict:
    winery = {
        "name": "Joseph Perrier",
        "country": "France",
        "region": "Champagne",
        "website_url": "https://www.josephperrier.com/en/",
    }

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        page = await browser.new_page()

        texts = {}
        for key, url in WINERY_PAGES.items():
            print(f"  Fetching winery page: {url}")
            texts[key] = await _get_body(page, url)

        await browser.close()

    full_text = "\n".join(texts.values())

    # ---- Founding year ----
    m = _find_number(full_text, r"\b(18\d{2})\b")
    if m:
        winery["founding_year"] = int(m.group(1))

    # ---- Village / subregion ----
    if "Châlons-en-Champagne" in full_text:
        winery["village"] = "Châlons-en-Champagne"
        winery["subregion"] = "Vallée de la Marne"

    # ---- Cellar length ----
    m = _find_number(full_text, r"([\d,\.]+)\s*km(?:s)?\b")
    if m:
        winery["cellar_length_km"] = float(m.group(1).replace(",", "."))

    # ---- Cellar depth ----
    m = _find_number(full_text, r"([\d,\.]+)\s*m(?:ètres?|eters?)?\s+(?:de\s+)?prof")
    if m:
        winery["cellar_depth_m"] = float(m.group(1).replace(",", "."))

    # ---- Hectares ----
    m = _find_number(full_text, r"([\d,\.]+)\s*hectares?")
    if m:
        winery["total_hectares"] = float(m.group(1).replace(",", "."))

    # ---- Village / subregion ----
    for village_str, village, subregion in [
        ("Châlons-en-Champagne", "Châlons-en-Champagne", "Vallée de la Marne"),
        ("Chalons-en-Champagne",  "Châlons-en-Champagne", "Vallée de la Marne"),
    ]:
        if village_str in full_text:
            winery["village"] = village
            winery["subregion"] = subregion
            break

    # ---- Number of generations ----
    m = re.search(r"(\d+)\s+générations?", full_text, re.IGNORECASE)
    if m:
        winery["_generations"] = int(m.group(1))

    # ---- Description / philosophy ----
    # Use histoire page; strip nav/footer noise then take first two meaty paragraphs
    histoire = texts.get("histoire", "")
    # Remove everything up to and including the last known nav item
    for nav_sentinel in ["MENTIONS LÉGALES", "CONDITIONS GÉNÉRALES", "CONTACT", "JOJO"]:
        idx = histoire.rfind(nav_sentinel)
        if idx != -1:
            histoire = histoire[:idx]
            break
    lines = [l.strip() for l in histoire.splitlines() if len(l.strip()) > 60
             and not l.strip().isupper()]
    if lines:
        winery["description"] = lines[0]
    if len(lines) > 1:
        winery["philosophy"] = " ".join(lines[1:3])

    # ---- Cave / cellar description ----
    cave = texts.get("cave", "")
    cave_lines = [l.strip() for l in cave.splitlines()
                  if len(l.strip()) > 60 and not l.strip().isupper()]
    if cave_lines:
        winery["_cellar_description"] = cave_lines[0]

    # ---- Vignoble / vineyard description ----
    vignoble = texts.get("vignoble", "")
    vines_lines = [l.strip() for l in vignoble.splitlines()
                   if len(l.strip()) > 60 and not l.strip().isupper()]
    if vines_lines:
        winery["_vineyard_description"] = vines_lines[0]

    # ---- Contact ----
    m = re.search(r"(\+33[\s\.\-\d]{8,}|\b0\d[\s\.\-]\d{2}[\s\.\-]\d{2}[\s\.\-]\d{2}[\s\.\-]\d{2})", full_text)
    if m:
        winery["phone"] = m.group(1).strip()
    m = re.search(r"[a-z0-9._%+\-]+@josephperrier\.com", full_text, re.IGNORECASE)
    if m:
        winery["email"] = m.group(0)

    # Store raw snippets for debugging
    winery["_raw_pages"] = {k: v[:500] for k, v in texts.items()}

    return winery

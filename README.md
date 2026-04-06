# Joseph Perrier — Champagne Data Ingestion Tool

A fully automated web scraper that crawls [josephperrier.com/en](https://www.josephperrier.com/en), extracts structured champagne product and winery data, downloads all media assets, and exports everything to **SQLite**, **CSV**, and **Excel**.

---

## What it does

- Renders JavaScript-heavy Elementor pages via headless Chromium (Playwright)
- Extracts structured data from WooCommerce's embedded JSON (pricing, bottle sizes, stock)
- Parses French-language label sections from `document.body.innerText` for technical fields
- Downloads and catalogs 345+ media assets concurrently (6 async workers)
- Writes a normalized SQLite database with 6 tables and full referential integrity
- Exports CSVs and a formatted Excel workbook

---

## Database Schema

```
winery              — house identity, certifications, cellar specs, contact
product             — 1 row per cuvée (10 products scraped)
grape_composition   — many-to-one; queryable blend percentages per product
tasting_note        — 1:1 with product; structured sensory profile
product_bottle_size — many-to-one; all bottle formats + individual prices
media               — 345 images, polymorphic (winery or product)
```

Entity-relationship overview:

```
winery ──< product ──< grape_composition
                  ──< tasting_note (1:1)
                  ──< product_bottle_size
                  ──< media
       ──< media (winery-level assets)
```

---

## Quickstart

### 1. Install dependencies

```bash
python -m venv .venv
source .venv/bin/activate          # Windows: .venv\Scripts\activate
pip install -r requirements.txt
playwright install chromium
```

### 2. Run the scraper

```bash
# Full run (winery + all products + media)
python run_scraper.py

# Products only (faster, skips winery pages)
python run_scraper.py --products

# Winery metadata only
python run_scraper.py --winery
```

### 3. Export to CSV + Excel

```bash
python export.py
```

Outputs written to `output/`:

| File | Contents |
|---|---|
| `winery.db` | SQLite database (6 tables) |
| `products.csv` | One row per cuvée |
| `grape_composition.csv` | Blend percentages |
| `bottle_sizes.csv` | Formats + prices |
| `tasting_notes.csv` | Structured sensory data |
| `media.csv` | All 345 image records |
| `joseph_perrier.xlsx` | All sheets in one workbook |

---

## Inspecting the results

### SQLite CLI

```sql
sqlite3 output/winery.db

-- All products with pricing
SELECT name, category, dosage_type, dosage_gl, price_eur FROM product;

-- Blend breakdown
SELECT p.name, g.grape_variety, g.percentage
FROM grape_composition g JOIN product p ON p.id = g.product_id
ORDER BY p.id, g.percentage DESC;

-- All bottle formats and prices
SELECT p.name, b.size_label, b.size_cl, b.price_eur
FROM product_bottle_size b JOIN product p ON p.id = b.product_id
ORDER BY p.id, b.size_cl;
```

### Python / pandas

```python
import pandas as pd
df = pd.read_csv("output/products.csv")
print(df[["name","category","dosage_type","dosage_gl","price_eur","aging_months"]].to_string())
```

### Excel
Open `output/joseph_perrier.xlsx` — 6 pre-formatted sheets with auto-fitted columns.

---

## Sample data — Products scraped

| Product | Category | Dosage | g/L | Price (75cl) |
|---|---|---|---|---|
| Cuvée Royale Brut | Non-Vintage | Brut | 7.0 | €40.90 |
| Cuvée Royale Brut Nature | Non-Vintage | Brut Nature | 0.0 | €44.00 |
| Cuvée Royale Blanc de Blancs | Non-Vintage | Brut | 6.0 | €58.90 |
| Cuvée Royale Brut Rosé | Non-Vintage | Brut | 7.0 | €58.90 |
| Cuvée Royale Vintage 2018 | Vintage | Extra Brut | 2.0 | €69.90 |
| Cuvée Royale Demi-Sec | Non-Vintage | Demi-Sec | 38.0 | €40.90 |
| Le Ciergelot 2020 | Parcellaire | Brut Nature | 0.0 | €87.50 |
| La Côte à Bras 2016 | Parcellaire | Brut Nature | 0.0 | €87.50 |
| Joséphine 2014 | Prestige | Brut | 5.0 | €165.00 |
| Cuvée 200 | Anniversary | Extra Brut | 3.0 | — |

---

## Key technical decisions

### 1. Playwright over requests/httpx for page fetching
The site uses Elementor (WordPress page builder) rendered entirely in JavaScript. Static HTTP requests return skeleton HTML. Playwright with headless Chromium fully renders pages before any scraping begins.

### 2. WooCommerce JSON over DOM scraping for commerce data
Elementor uses generic `div.elementor-widget-heading` elements with no semantic class names — CSS selectors for pricing and bottle sizes were completely unreliable. The fix: WooCommerce embeds a `data-product_variations` JSON blob directly on the `<form class="variations_form">` element, containing all variants, prices, stock status, and image URLs in clean, structured JSON.

### 3. Label-scanning approach for technical fields
With no semantic HTML anchors, all technical fields (dosage, aging, grape %, tasting notes) are extracted from `document.body.innerText` using a French label dictionary. Lines are matched against known labels (`DOSAGE`, `AU NEZ`, `VIEILLISSEMENT`, etc.) and the following lines are consumed as the value until the next known label appears.

### 4. Slug-based product name mapping
The Cuvée Royale pages and parcellaire pages use inconsistent name layouts in the DOM. Rather than parsing fragile layout heuristics, canonical product names are derived from the URL slug via a hardcoded map (`product_parser.py:_SLUG_NAME_MAP`).

### 5. Deduplicating bottle sizes
WooCommerce lists both `bouteille (75cl)` and `bouteille avec étui (75cl)` as separate variants — both resolve to 75cl, causing a `UNIQUE(product_id, size_cl)` constraint violation. Fixed in the parser by deduplicating on `size_cl`, preferring the base format (no gift box) and its lower price.

---

## Project structure

```
jp_winery_project/
├── run_scraper.py          # Main orchestrator (CLI entry point)
├── export.py               # CSV + Excel exporter
├── models/
│   └── schema.py           # SQLAlchemy ORM (6 tables)
├── scrapers/
│   ├── product_spider.py   # Discovers and crawls all product URLs
│   ├── product_parser.py   # Parses a single product page → structured dict
│   ├── winery_parser.py    # Parses winery-level pages (about, history, cellar)
│   ├── db_writer.py        # Upserts parsed data into SQLite
│   ├── media_fetcher.py    # Async image downloader (6 workers, httpx)
│   └── explore.py          # One-off exploration script used during dev
└── output/
    ├── winery.db           # SQLite database
    ├── products.csv
    ├── grape_composition.csv
    ├── bottle_sizes.csv
    ├── tasting_notes.csv
    ├── media.csv
    ├── joseph_perrier.xlsx
    ├── scraped_raw.json    # Raw debug dump
    └── images/             # Downloaded media assets
```

---

## Dependencies

| Library | Role |
|---|---|
| `playwright` | Headless Chromium — renders JS-heavy Elementor pages |
| `sqlalchemy` | ORM + SQLite engine; declarative schema with relationships |
| `httpx` | Async HTTP client for concurrent image downloads (6 workers) |
| `beautifulsoup4` | HTML parsing fallback |
| `pandas` | DataFrame assembly, CSV export, wide pivots |
| `openpyxl` | Excel workbook writer with auto-fitted column widths |

---

*Built as a technical assignment demonstrating structured data extraction from a real-world champagne producer website.*

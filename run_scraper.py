"""
Main orchestrator.

Usage:
    python run_scraper.py              # full run
    python run_scraper.py --products   # products only (skip winery pages)
    python run_scraper.py --winery     # winery pages only
"""

import asyncio
import json
import sys
from pathlib import Path

from sqlalchemy.orm import Session

from models.schema import init_db
from scrapers.product_spider import crawl_all_products
from scrapers.winery_parser import scrape_winery
from scrapers.db_writer import upsert_winery, upsert_product

DB_PATH = "output/winery.db"
DUMP_PATH = Path("output/scraped_raw.json")


async def main(run_winery=True, run_products=True):
    engine = init_db(DB_PATH)
    print(f"Database: {DB_PATH}\n")

    raw_dump = {}

    with Session(engine) as session:
        # ---------------------------------------------------------------- #
        # Winery
        # ---------------------------------------------------------------- #
        if run_winery:
            print("=== Scraping winery pages ===")
            winery_data = await scrape_winery()
            raw_dump["winery"] = winery_data
            winery = upsert_winery(session, winery_data)
            session.commit()
            print(f"  Winery saved: {winery.name} (id={winery.id})")
        else:
            from models.schema import Winery
            winery = session.query(Winery).filter_by(name="Joseph Perrier").first()
            if not winery:
                # Minimal stub so products can be written
                from models.schema import Winery as W
                winery = W(name="Joseph Perrier", country="France", region="Champagne",
                           website_url="https://www.josephperrier.com/en/")
                session.add(winery)
                session.commit()

        # ---------------------------------------------------------------- #
        # Products
        # ---------------------------------------------------------------- #
        if run_products:
            print("\n=== Scraping products ===")
            products = await crawl_all_products()
            raw_dump["products"] = products

            saved = 0
            errors = 0
            for p in products:
                if "error" in p:
                    print(f"  [skip] {p['source_url']}: {p['error']}")
                    errors += 1
                    continue
                try:
                    upsert_product(session, winery, p)
                    session.commit()
                    name = p.get("name", p.get("slug", "?"))
                    grapes = ", ".join(
                        f"{g['grape_variety']} {g['percentage']}%"
                        for g in p.get("grape_composition", [])
                    )
                    sizes = ", ".join(
                        f"{b['size_cl']}cl=€{b['price_eur']}"
                        for b in p.get("bottle_sizes", [])
                    )
                    print(f"  ✓ {name}")
                    if grapes:
                        print(f"    Grapes: {grapes}")
                    if sizes:
                        print(f"    Sizes:  {sizes}")
                    saved += 1
                except Exception as e:
                    session.rollback()
                    print(f"  [db error] {p.get('slug')}: {e}")
                    errors += 1

            print(f"\nProducts saved: {saved}  errors: {errors}")

    # Save raw dump for debugging
    DUMP_PATH.write_text(
        json.dumps(raw_dump, indent=2, ensure_ascii=False, default=str)
    )
    print(f"\nRaw dump → {DUMP_PATH}")

    # -------------------------------------------------------------------- #
    # Summary query
    # -------------------------------------------------------------------- #
    print("\n=== Database summary ===")
    with Session(engine) as session:
        from models.schema import Product, GrapeComposition, ProductBottleSize, Media
        products = session.query(Product).all()
        print(f"Products:      {len(products)}")
        print(f"Grape entries: {session.query(GrapeComposition).count()}")
        print(f"Bottle sizes:  {session.query(ProductBottleSize).count()}")
        print(f"Media items:   {session.query(Media).count()}")
        print()
        for prod in products:
            grapes = " / ".join(
                f"{g.grape_variety} {g.percentage}%"
                for g in prod.grape_composition
            )
            sizes = " | ".join(
                f"{b.size_label}({b.size_cl}cl) €{b.price_eur}"
                for b in prod.bottle_sizes
            )
            print(f"  {prod.name:<45} dosage={prod.dosage_gl}g/L  {prod.category}")
            if grapes:
                print(f"    {grapes}")
            if sizes:
                print(f"    {sizes}")


if __name__ == "__main__":
    args = sys.argv[1:]
    run_winery = "--products" not in args
    run_products = "--winery" not in args
    asyncio.run(main(run_winery=run_winery, run_products=run_products))

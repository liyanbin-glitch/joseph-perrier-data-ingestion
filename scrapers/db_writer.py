"""
Writes parsed product and winery dicts into SQLite via SQLAlchemy ORM.
"""

from datetime import datetime
from sqlalchemy.orm import Session

from models.schema import (
    Winery, Product, GrapeComposition, TastingNote, ProductBottleSize, Media
)


def upsert_winery(session: Session, data: dict) -> Winery:
    winery = session.query(Winery).filter_by(name=data["name"]).first()
    if not winery:
        winery = Winery()
        session.add(winery)

    skip = {"_raw_pages"}
    for key, val in data.items():
        if key in skip:
            continue
        if hasattr(winery, key):
            setattr(winery, key, val)

    winery.updated_at = datetime.utcnow()
    session.flush()
    return winery


def upsert_product(session: Session, winery: Winery, data: dict) -> Product:
    slug = data.get("slug", "")
    product = session.query(Product).filter_by(
        winery_id=winery.id, slug=slug
    ).first()
    if not product:
        product = Product(winery_id=winery.id)
        session.add(product)

    # Scalar fields
    scalar_map = {
        "name":               "name",
        "slug":               "slug",
        "category":           "category",
        "is_vintage":         "is_vintage",
        "vintage_year":       "vintage_year",
        "dosage_type":        "dosage_type",
        "dosage_gl":          "dosage_gl",
        "aging_months":       "aging_months",
        "reserve_wine_pct":   "reserve_wine_pct",
        "serving_temp_min_c": "serving_temp_min_c",
        "serving_temp_max_c": "serving_temp_max_c",
        "price_eur":          "price_eur",
        "short_description":  "short_description",
        "food_pairing":       "food_pairing",
        "awards":             "awards",
        "source_url":         "source_url",
        "crus_assembled":     "winemaker_notes",  # closest field
    }
    for src, dst in scalar_map.items():
        val = data.get(src)
        if val is not None:
            setattr(product, dst, val)

    product.product_type = "Champagne"
    product.updated_at = datetime.utcnow()
    session.flush()

    # ---- Grape composition ----
    # Delete existing and re-insert (clean upsert)
    for gc in list(product.grape_composition):
        session.delete(gc)
    session.flush()
    for g in data.get("grape_composition", []):
        session.add(GrapeComposition(
            product_id=product.id,
            grape_variety=g["grape_variety"],
            percentage=g.get("percentage"),
        ))

    # ---- Tasting note ----
    tn = product.tasting_notes
    if not tn:
        tn = TastingNote(product_id=product.id)
        session.add(tn)
    tn.color = _extract_color(data.get("tasting_eye", ""))
    tn.nose_primary = data.get("tasting_nose")
    tn.palate_attack = data.get("tasting_palate")
    tn.aging_potential = data.get("aging_potential_raw")
    tn.raw_text = data.get("tasting_raw")

    # ---- Bottle sizes ----
    for bs in list(product.bottle_sizes):
        session.delete(bs)
    session.flush()
    for b in data.get("bottle_sizes", []):
        session.add(ProductBottleSize(
            product_id=product.id,
            size_cl=b["size_cl"],
            size_label=b.get("size_label"),
            price_eur=b.get("price_eur"),
            available=b.get("available", True),
        ))

    # ---- Hero image ----
    hero = data.get("hero_image")
    if hero and hero.get("url"):
        existing = session.query(Media).filter_by(
            product_id=product.id, role="bottle_shot"
        ).first()
        if not existing:
            session.add(Media(
                product_id=product.id,
                winery_id=winery.id,
                media_type="image",
                role="bottle_shot",
                url=hero["url"],
                alt_text=hero.get("alt", ""),
                width_px=hero.get("width_px"),
                height_px=hero.get("height_px"),
                mime_type="image/webp",
            ))

    # ---- Additional images ----
    for img in data.get("raw_images", []):
        url = img.get("url", "")
        if not url or "josephperrier.com" not in url:
            continue
        if session.query(Media).filter_by(product_id=product.id, url=url).first():
            continue
        session.add(Media(
            product_id=product.id,
            winery_id=winery.id,
            media_type="image",
            role="gallery",
            url=url,
            alt_text=img.get("alt", ""),
            width_px=img.get("width_px"),
            height_px=img.get("height_px"),
            mime_type="image/webp" if url.endswith(".webp") else "image/jpeg",
        ))

    return product


def _extract_color(eye_text: str) -> str:
    """Pull colour descriptor from the À L'OEIL tasting note."""
    if not eye_text:
        return None
    m = __import__("re").search(
        r"(or\s+\w+|jaune\s+\w+|rosé\s*\w*|doré\s*\w*|pale\s+\w+|pâle\s+\w+|salmon|pink|gold\w*)",
        eye_text, __import__("re").IGNORECASE
    )
    return m.group(0).strip() if m else eye_text[:80]

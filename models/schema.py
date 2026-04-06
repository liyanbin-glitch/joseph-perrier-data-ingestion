"""
SQLAlchemy ORM schema for Joseph Perrier winery data ingestion.

Design rationale:
- Winery: house-level identity, history, certifications, contact/location
- Product (Cuvée): the core commercial entity — one row per SKU/variant
- GrapeComposition: many-to-one back to Product; a cuvée blends multiple varieties
- TastingNote: structured sensory profile (separate from marketing copy)
- Media: images & videos, polymorphic via a discriminator column
- ProductBottleSize: a single cuvée ships in multiple formats (75cl, jeroboam, etc.)
"""

from datetime import datetime
from sqlalchemy import (
    Boolean, Column, DateTime, Float, ForeignKey, Integer, String, Text,
    UniqueConstraint, create_engine
)
from sqlalchemy.orm import DeclarativeBase, relationship


class Base(DeclarativeBase):
    pass


# ---------------------------------------------------------------------------
# Winery
# ---------------------------------------------------------------------------

class Winery(Base):
    """House-level identity. Typically one row, but schema supports multiple."""
    __tablename__ = "winery"

    id                  = Column(Integer, primary_key=True)
    name                = Column(String(120), nullable=False)           # "Joseph Perrier"
    founding_year       = Column(Integer)                               # 1825
    region              = Column(String(80))                            # "Champagne"
    subregion           = Column(String(80))                            # "Vallée de la Marne"
    village             = Column(String(80))                            # "Châlons-en-Champagne"
    country             = Column(String(60), default="France")
    website_url         = Column(String(255))
    description         = Column(Text)                                  # brand narrative
    philosophy          = Column(Text)                                  # winemaking approach
    # Certifications
    is_organic          = Column(Boolean, default=False)
    is_biodynamic       = Column(Boolean, default=False)
    certifications      = Column(String(255))                           # "HVE3, Terra Vitis"
    # Estate
    total_hectares      = Column(Float)
    estate_hectares     = Column(Float)                                 # owned vs sourced
    # Cellar
    cellar_depth_m      = Column(Float)
    cellar_length_km    = Column(Float)
    annual_production   = Column(Integer)                               # bottles/year
    # Contact
    address             = Column(String(255))
    phone               = Column(String(40))
    email               = Column(String(120))
    # Metadata
    scraped_at          = Column(DateTime, default=datetime.utcnow)
    updated_at          = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

    products            = relationship("Product", back_populates="winery")
    media               = relationship("Media", back_populates="winery")


# ---------------------------------------------------------------------------
# Product (Cuvée)
# ---------------------------------------------------------------------------

class Product(Base):
    """
    One row per distinct cuvée.  Bottle-size variants live in ProductBottleSize.
    Vintage vs NV is captured via `vintage_year` (None = NV).
    """
    __tablename__ = "product"
    __table_args__ = (
        UniqueConstraint("winery_id", "name", "vintage_year", name="uq_product"),
    )

    id                  = Column(Integer, primary_key=True)
    winery_id           = Column(Integer, ForeignKey("winery.id"), nullable=False)

    # Identity
    name                = Column(String(200), nullable=False)           # "Cuvée Royale Blanc de Blancs"
    slug                = Column(String(200))                           # url-derived key
    category            = Column(String(60))                           # "Prestige", "Non-Vintage", "Vintage", "Rosé"
    product_type        = Column(String(40), default="Champagne")       # Champagne / Crémant / etc.
    is_vintage          = Column(Boolean, default=False)
    vintage_year        = Column(Integer)                               # NULL for NV

    # Blend & viticulture
    base_vintage_year   = Column(Integer)                               # NV base year if disclosed
    dosage_type         = Column(String(40))                            # "Brut", "Extra Brut", "Demi-Sec", "Brut Nature"
    dosage_gl           = Column(Float)                                 # g/L residual sugar
    blend_description   = Column(Text)                                  # "60% PN, 30% CH, 10% PM"
    disgorgement_date   = Column(String(40))                            # "Q1 2024" or exact date string
    aging_months        = Column(Integer)                               # total lees aging
    reserve_wine_pct    = Column(Float)                                 # % reserve wines in blend

    # Technical
    abv                 = Column(Float)                                 # alcohol % by volume
    serving_temp_min_c  = Column(Float)                                 # 8
    serving_temp_max_c  = Column(Float)                                 # 10

    # Commerce
    price_eur           = Column(Float)                                 # standard 75cl retail
    availability        = Column(String(60))                            # "Available", "Limited", "On Allocation"
    sku                 = Column(String(80))

    # Content
    short_description   = Column(Text)                                  # tagline / one-liner
    full_description    = Column(Text)                                  # long marketing copy
    food_pairing        = Column(Text)                                  # "Oysters, grilled fish"
    awards              = Column(Text)                                  # "Gold — Decanter 2023"
    winemaker_notes     = Column(Text)

    # Metadata
    source_url          = Column(String(255))
    scraped_at          = Column(DateTime, default=datetime.utcnow)
    updated_at          = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

    winery              = relationship("Winery", back_populates="products")
    grape_composition   = relationship("GrapeComposition", back_populates="product", cascade="all, delete-orphan")
    tasting_notes       = relationship("TastingNote", back_populates="product", uselist=False, cascade="all, delete-orphan")
    bottle_sizes        = relationship("ProductBottleSize", back_populates="product", cascade="all, delete-orphan")
    media               = relationship("Media", back_populates="product")


# ---------------------------------------------------------------------------
# Grape Composition
# ---------------------------------------------------------------------------

class GrapeComposition(Base):
    """
    Break-out of the blend percentages so they're queryable.
    e.g. Pinot Noir 60%, Chardonnay 30%, Pinot Meunier 10%
    """
    __tablename__ = "grape_composition"
    __table_args__ = (
        UniqueConstraint("product_id", "grape_variety", name="uq_grape_per_product"),
    )

    id              = Column(Integer, primary_key=True)
    product_id      = Column(Integer, ForeignKey("product.id"), nullable=False)
    grape_variety   = Column(String(80), nullable=False)    # "Pinot Noir", "Chardonnay", "Pinot Meunier"
    percentage      = Column(Float)                          # 60.0
    origin_village  = Column(String(120))                   # "Ay, Hautvillers" if disclosed

    product         = relationship("Product", back_populates="grape_composition")


# ---------------------------------------------------------------------------
# Tasting Note
# ---------------------------------------------------------------------------

class TastingNote(Base):
    """
    Structured sensory profile — one-to-one with Product.
    Separates machine-parseable sensory data from marketing prose.
    """
    __tablename__ = "tasting_note"

    id              = Column(Integer, primary_key=True)
    product_id      = Column(Integer, ForeignKey("product.id"), nullable=False, unique=True)

    # Visual
    color           = Column(String(80))        # "pale gold", "salmon pink"
    bubble_fineness = Column(String(80))        # "fine persistent mousse"

    # Nose
    nose_intensity  = Column(String(40))        # "expressive", "delicate"
    nose_primary    = Column(Text)              # "fresh citrus, white peach"
    nose_secondary  = Column(Text)              # "brioche, toasted almonds"
    nose_tertiary   = Column(Text)              # "honey, dried fruits" (aged/prestige)

    # Palate
    palate_attack   = Column(String(80))        # "crisp", "creamy", "rich"
    palate_body     = Column(String(40))        # "light", "medium", "full"
    palate_acidity  = Column(String(40))        # "vibrant", "balanced", "soft"
    palate_finish   = Column(String(120))       # "long mineral finish"
    palate_flavors  = Column(Text)              # free-text flavor descriptors

    # Overall
    balance         = Column(String(80))
    aging_potential = Column(String(80))        # "drink now – 2030"
    raw_text        = Column(Text)              # original unparsed tasting note

    product         = relationship("Product", back_populates="tasting_notes")


# ---------------------------------------------------------------------------
# Bottle Size / Format
# ---------------------------------------------------------------------------

class ProductBottleSize(Base):
    """
    A single cuvée is sold in multiple formats with distinct prices.
    Standard 75cl is also captured here for price normalization.
    """
    __tablename__ = "product_bottle_size"
    __table_args__ = (
        UniqueConstraint("product_id", "size_cl", name="uq_product_size"),
    )

    id          = Column(Integer, primary_key=True)
    product_id  = Column(Integer, ForeignKey("product.id"), nullable=False)
    size_cl     = Column(Float, nullable=False)     # 37.5, 75, 150, 300, 600 …
    size_label  = Column(String(60))                # "Magnum", "Jeroboam", "Methuselah"
    price_eur   = Column(Float)
    sku         = Column(String(80))
    available   = Column(Boolean, default=True)

    product     = relationship("Product", back_populates="bottle_sizes")


# ---------------------------------------------------------------------------
# Media
# ---------------------------------------------------------------------------

class Media(Base):
    """
    Images and videos attached to either the Winery or a specific Product.
    One or both of winery_id / product_id will be set (not enforced at DB level
    to allow winery-level hero assets that aren't product-specific).
    """
    __tablename__ = "media"

    id          = Column(Integer, primary_key=True)
    winery_id   = Column(Integer, ForeignKey("winery.id"), nullable=True)
    product_id  = Column(Integer, ForeignKey("product.id"), nullable=True)

    media_type  = Column(String(20), nullable=False)    # "image" | "video"
    role        = Column(String(40))                    # "hero", "bottle_shot", "vineyard", "label", "thumbnail"
    url         = Column(String(512), nullable=False)
    alt_text    = Column(String(255))
    width_px    = Column(Integer)
    height_px   = Column(Integer)
    mime_type   = Column(String(60))                    # "image/webp", "video/mp4"
    local_path  = Column(String(512))                   # path after download to output/
    sort_order  = Column(Integer, default=0)

    winery      = relationship("Winery", back_populates="media")
    product     = relationship("Product", back_populates="media")


# ---------------------------------------------------------------------------
# DB bootstrap helper
# ---------------------------------------------------------------------------

def init_db(db_path: str = "output/winery.db") -> None:
    engine = create_engine(f"sqlite:///{db_path}", echo=False)
    Base.metadata.create_all(engine)
    return engine

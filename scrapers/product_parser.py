"""
Parses a single Joseph Perrier product page into structured dicts.

Data sources per page:
  1. WooCommerce `data-product_variations` JSON  → bottle sizes, prices, images
  2. Body text labeled sections                  → grapes, tech specs, tasting notes
  3. JSON-LD Yoast schema                        → page title, primary image, description
"""

import json
import re
from dataclasses import dataclass, field
from typing import Optional


# ---------------------------------------------------------------------------
# Known grape varieties (French names as they appear on site)
# ---------------------------------------------------------------------------
GRAPE_VARIETIES = {
    "CHARDONNAY", "PINOT NOIR", "MEUNIER", "PINOT MEUNIER",
    "PINOT BLANC", "ARBANE", "PETIT MESLIER", "PINOT GRIS",
}

# French label → internal key
TECH_LABELS = {
    "CRUS ASSEMBLÉS":         "crus_assembled",
    "VIEILLISSEMENT":         "aging_raw",
    "VINS DE RÉSERVE":        "reserve_wine_raw",
    "VINS DE RÉSERVES":       "reserve_wine_raw",
    "DOSAGE":                 "dosage_raw",
    "MILLÉSIME":              "vintage_raw",
    "TEMPÉRATURE DE SERVICE": "serving_temp_raw",
    "POTENTIEL DE VIEILLISSEMENT": "aging_potential_raw",
}

TASTING_LABELS = {
    "À L'OEIL":           "eye",
    "A L'OEIL":           "eye",
    "AU NEZ":             "nose",
    "EN BOUCHE":          "palate",
    "ACCORDS METS & VINS": "food_pairing",
}

# Bottle size label → cl
SIZE_MAP = {
    "37,5": 37.5, "37.5": 37.5, "demi": 37.5,
    "75":   75.0,
    "150":  150.0, "1,5": 150.0,
    "300":  300.0, "3 l": 300.0, "3l": 300.0,
    "600":  600.0, "6 l": 600.0, "6l": 600.0,
    "450":  450.0, "4,5": 450.0,
    "900":  900.0, "9 l": 900.0,
    "1800": 1800.0, "18 l": 1800.0,
}

SIZE_LABELS = {
    37.5:  "Demi-bouteille",
    75.0:  "Bouteille",
    150.0: "Magnum",
    300.0: "Jeroboam",
    450.0: "Réhoboam",
    600.0: "Mathusalem",
    900.0: "Salmanazar",
    1800.0: "Nébuchadnezzar",
}


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _strip(text: str) -> str:
    return text.strip().strip("\u00a0")


def _parse_cl(label: str) -> Optional[float]:
    """Extract centilitres from a bottle label string."""
    label_lower = label.lower()
    # Explicit cl values
    m = re.search(r"(\d[\d,\.]*)\s*cl", label_lower)
    if m:
        return float(m.group(1).replace(",", "."))
    # Litre values
    m = re.search(r"(\d[\d,\.]*)\s*l(?:\b|$)", label_lower)
    if m:
        litres = float(m.group(1).replace(",", "."))
        return litres * 100
    # Known keywords
    if "demi" in label_lower:
        return 37.5
    if "magnum" in label_lower:
        return 150.0
    if "jéroboam" in label_lower or "jeroboam" in label_lower:
        return 300.0
    if "mathusalem" in label_lower or "methuselah" in label_lower:
        return 600.0
    return None


def _parse_serving_temp(raw: str):
    """Return (min_c, max_c) from '8-10°C' or '8 À 10°C'."""
    nums = re.findall(r"\d+", raw)
    if len(nums) >= 2:
        return float(nums[0]), float(nums[1])
    if len(nums) == 1:
        return float(nums[0]), None
    return None, None


def _parse_dosage_gl(raw: str) -> Optional[float]:
    """Return g/L value from '7g/L', '0g/L', '6 g/L'."""
    m = re.search(r"([\d,\.]+)\s*g", raw, re.IGNORECASE)
    if m:
        return float(m.group(1).replace(",", "."))
    return None


def _parse_aging_months(raw: str) -> Optional[int]:
    """Return months from '36 mois en cave' or '108 mois'."""
    m = re.search(r"(\d+)\s*mois", raw, re.IGNORECASE)
    if m:
        return int(m.group(1))
    return None


def _parse_reserve_pct(raw: str) -> Optional[float]:
    """Return % from 'Environ 20%' or '15%'."""
    m = re.search(r"([\d,\.]+)\s*%", raw)
    if m:
        return float(m.group(1).replace(",", "."))
    return None


# ---------------------------------------------------------------------------
# Main parser
# ---------------------------------------------------------------------------

def parse_product_page(html: str, url: str, body_text: str, wc_variations_json: str) -> dict:
    """
    Returns a flat dict with all product fields, ready to be mapped to the ORM.
    """
    result = {
        "source_url": url,
        "slug": url.rstrip("/").split("/")[-1],
    }

    # ------------------------------------------------------------------ #
    # 1. WooCommerce variations → bottle sizes + prices + hero image
    # ------------------------------------------------------------------ #
    variations = []
    if wc_variations_json:
        try:
            variations = json.loads(wc_variations_json)
        except json.JSONDecodeError:
            pass

    # cl → {size_label, price_eur, available}  — deduplicate by size_cl,
    # keeping base format (no gift box / étui) so the unique constraint holds.
    sizes_by_cl: dict = {}
    hero_image = None

    for var in variations:
        attr = var.get("attributes", {})
        label = attr.get("attribute_conditionnement", "").strip()
        price = var.get("display_price")
        cl = _parse_cl(label)
        img = var.get("image", {})
        img_url = img.get("full_src") or img.get("url")
        if cl and not hero_image and img_url:
            hero_image = {
                "url": img_url,
                "alt": img.get("alt", ""),
                "width_px": img.get("full_src_w"),
                "height_px": img.get("full_src_h"),
            }
        if not cl:
            continue
        label_lower = label.lower()
        is_gift = any(kw in label_lower for kw in ("étui", "etui", "coffret", "avec"))
        if cl not in sizes_by_cl:
            sizes_by_cl[cl] = {
                "size_cl": cl,
                "size_label": SIZE_LABELS.get(cl, label),
                "price_eur": price,
                "available": bool(var.get("is_in_stock", True)),
                "_is_gift": is_gift,
            }
        else:
            # Prefer the base format (no gift box) and its lower price
            existing = sizes_by_cl[cl]
            if existing["_is_gift"] and not is_gift:
                sizes_by_cl[cl] = {
                    "size_cl": cl,
                    "size_label": SIZE_LABELS.get(cl, label),
                    "price_eur": price,
                    "available": bool(var.get("is_in_stock", True)),
                    "_is_gift": False,
                }

    bottle_sizes = [{k: v for k, v in s.items() if k != "_is_gift"}
                    for s in sizes_by_cl.values()]
    result["bottle_sizes"] = bottle_sizes
    result["hero_image"] = hero_image
    # Standard 75cl price
    std = next((b for b in bottle_sizes if b["size_cl"] == 75.0), None)
    result["price_eur"] = std["price_eur"] if std else None

    # ------------------------------------------------------------------ #
    # 2. Parse body text into sections
    # ------------------------------------------------------------------ #
    # Trim everything from "LETTRE D'INFORMATIONS" onward (footer)
    cutoff_patterns = [
        "LETTRE D'INFORMATIONS",
        "NOUS VOUS CONSEILLONS",
        "INSCRIVEZ-VOUS",
    ]
    body = body_text
    for pat in cutoff_patterns:
        idx = body.find(pat)
        if idx != -1:
            body = body[:idx]

    lines = [_strip(l) for l in body.splitlines() if _strip(l)]

    # ---- Product name: derive from slug (reliable) + body text for vintage ----
    slug = result["slug"]
    result["name"] = _slug_to_name(slug)
    # Vintage from slug (e.g. "cuvee-royale-vintage-2018", "josephine-2014")
    vy_match = re.search(r"-(20\d{2}|19\d{2})(?:-|$)", slug)
    if vy_match:
        result["vintage_year"] = int(vy_match.group(1))
        result["is_vintage"] = True
    else:
        result["is_vintage"] = False
        result["vintage_year"] = None

    # Store body-text title lines for debugging
    start_idx = 0
    for i, line in enumerate(lines):
        if line in {"FR", "EN", "PANIER", "0", "MAISON", "CHAMPAGNES", "E-SHOP", "VISITES"}:
            start_idx = i
    content_lines = lines[start_idx + 1:]
    result["name_line1"] = content_lines[0] if content_lines else ""
    result["name_line2"] = content_lines[1] if len(content_lines) > 1 else ""

    # ---- Short description (line after the name pair) ----
    desc_start = start_idx + 3  # skip name lines
    short_desc_lines = []
    for line in lines[desc_start:]:
        if line.upper() == line and len(line) > 3:  # ALL CAPS = new section header
            break
        short_desc_lines.append(line)
    result["short_description"] = " ".join(short_desc_lines).strip()

    # ---- Grape composition ----
    grapes = []
    i = 0
    while i < len(lines):
        line_upper = lines[i].upper()
        if line_upper in GRAPE_VARIETIES:
            # Next line should be the percentage
            if i + 1 < len(lines):
                pct_line = lines[i + 1]
                m = re.match(r"^([\d,\.]+)\s*%", pct_line)
                if m:
                    grapes.append({
                        "grape_variety": lines[i].title(),
                        "percentage": float(m.group(1).replace(",", ".")),
                    })
                    i += 2
                    continue
        i += 1
    result["grape_composition"] = grapes

    # ---- Technical spec blocks (label on one line, value on next) ----
    tech = {}
    i = 0
    while i < len(lines):
        line_upper = lines[i].upper().strip()
        matched_key = None
        for label, key in TECH_LABELS.items():
            if line_upper == label or line_upper.startswith(label):
                matched_key = key
                break
        if matched_key:
            # Collect value lines until next known label or blank
            value_lines = []
            j = i + 1
            while j < len(lines):
                next_upper = lines[j].upper().strip()
                if any(next_upper == lbl or next_upper.startswith(lbl)
                       for lbl in TECH_LABELS):
                    break
                if next_upper in {"TÉLÉCHARGER LA FICHE TECHNIQUE",
                                   "RÉCOMPENSES & NOTES", "NOTES DE DÉGUSTATION"}:
                    break
                value_lines.append(lines[j])
                j += 1
            tech[matched_key] = " ".join(value_lines).strip()
            i = j
        else:
            i += 1

    result["crus_assembled"] = tech.get("crus_assembled")
    result["dosage_raw"] = tech.get("dosage_raw")
    result["aging_raw"] = tech.get("aging_raw")
    result["reserve_wine_raw"] = tech.get("reserve_wine_raw")
    result["serving_temp_raw"] = tech.get("serving_temp_raw")
    result["aging_potential_raw"] = tech.get("aging_potential_raw")

    # Derive vintage year from tech block if not already set
    if not result["vintage_year"] and tech.get("vintage_raw"):
        m = re.search(r"\d{4}", tech["vintage_raw"])
        if m:
            result["vintage_year"] = int(m.group())
            result["is_vintage"] = True

    # Parse numeric derivations
    result["dosage_gl"] = _parse_dosage_gl(tech.get("dosage_raw", ""))
    result["dosage_type"] = _infer_dosage_type(result["dosage_gl"])
    result["aging_months"] = _parse_aging_months(tech.get("aging_raw", ""))
    result["reserve_wine_pct"] = _parse_reserve_pct(tech.get("reserve_wine_raw", ""))
    t_min, t_max = _parse_serving_temp(tech.get("serving_temp_raw", ""))
    result["serving_temp_min_c"] = t_min
    result["serving_temp_max_c"] = t_max

    # ---- Awards ----
    awards_lines = _extract_section(lines, "RÉCOMPENSES & NOTES",
                                    {"NOTES DE DÉGUSTATION", "VOIR TOUTES LES RÉCOMPENSES"})
    # Clean up the "VOIR TOUTES" sentinel if it sneaked in
    awards_lines = [l for l in awards_lines if "VOIR TOUTES" not in l.upper()]
    result["awards"] = _format_awards(awards_lines)

    # ---- Tasting notes ----
    tasting_start = next(
        (i for i, l in enumerate(lines) if "NOTES DE DÉGUSTATION" in l.upper()), None
    )
    tasting = {"eye": None, "nose": None, "palate": None, "food_pairing": None, "raw": None}
    if tasting_start is not None:
        tasting_lines = lines[tasting_start:]
        tasting["raw"] = " | ".join(tasting_lines[:40])
        current_key = None
        current_buf = []
        for line in tasting_lines:
            line_up = line.upper().strip()
            matched = None
            for label, key in TASTING_LABELS.items():
                if line_up == label:
                    matched = key
                    break
            if matched:
                if current_key:
                    tasting[current_key] = " ".join(current_buf).strip()
                current_key = matched
                current_buf = []
            elif current_key:
                # Stop collecting at footer-like lines
                if line_up in {"NOUS VOUS CONSEILLONS", "LETTRE D'INFORMATIONS"} \
                        or line_up.startswith("BOUTEILLE") \
                        or re.match(r"^\d+,\d+\s*€", line_up):
                    tasting[current_key] = " ".join(current_buf).strip()
                    current_key = None
                else:
                    current_buf.append(line)
        if current_key:
            tasting[current_key] = " ".join(current_buf).strip()

    result["tasting_eye"] = tasting["eye"]
    result["tasting_nose"] = tasting["nose"]
    result["tasting_palate"] = tasting["palate"]
    result["food_pairing"] = tasting["food_pairing"]
    result["tasting_raw"] = tasting["raw"]

    # ---- Category inference ----
    result["category"] = _infer_category(result)

    return result


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

_SLUG_NAME_MAP = {
    "cuvee-royale-brut":                  "Cuvée Royale Brut",
    "cuvee-royale-brut-nature":           "Cuvée Royale Brut Nature",
    "cuvee-royale-brut-blanc-de-blancs":  "Cuvée Royale Blanc de Blancs",
    "cuvee-royale-brut-rose":             "Cuvée Royale Brut Rosé",
    "cuvee-royale-vintage-2018":          "Cuvée Royale Vintage 2018",
    "cuvee-royale-demi-sec":              "Cuvée Royale Demi-Sec",
    "cuvee-ciergelot-2020":               "Le Ciergelot 2020",
    "la-cote-a-bras-2016":               "La Côte à Bras 2016",
    "josephine-2014":                     "Joséphine 2014",
    "cuvee-200":                          "Cuvée 200",
}


def _slug_to_name(slug: str) -> str:
    if slug in _SLUG_NAME_MAP:
        return _SLUG_NAME_MAP[slug]
    # Generic: replace hyphens, title-case, fix accents
    return slug.replace("-", " ").title()


def _infer_dosage_type(dosage_gl: Optional[float]) -> Optional[str]:
    if dosage_gl is None:
        return None
    if dosage_gl == 0:
        return "Brut Nature"
    if dosage_gl <= 3:
        return "Extra Brut"
    if dosage_gl <= 12:
        return "Brut"
    if dosage_gl <= 17:
        return "Extra Dry"
    if dosage_gl <= 32:
        return "Sec"
    if dosage_gl <= 50:
        return "Demi-Sec"
    return "Doux"


def _infer_category(r: dict) -> str:
    name = (r.get("name", "") + " " + r.get("slug", "")).lower()
    if "josephine" in name or "joséphine" in name:
        return "Prestige"
    if "200" in name:
        return "Anniversary"
    if "ciergelot" in name or "cote-a-bras" in name or "côte" in name:
        return "Parcellaire"
    if r.get("is_vintage"):
        return "Vintage"
    return "Non-Vintage"


def _extract_section(lines: list, start_label: str, stop_labels: set) -> list:
    collecting = False
    out = []
    for line in lines:
        lu = line.upper().strip()
        if start_label in lu:
            collecting = True
            continue
        if collecting:
            if any(s in lu for s in stop_labels):
                break
            if line.strip():
                out.append(line.strip())
    return out


def _format_awards(lines: list) -> Optional[str]:
    """Collapse multi-line award blocks into 'SOURCE — AWARD YEAR' strings."""
    if not lines:
        return None
    # Group into chunks: source line, then award details
    awards = []
    buf = []
    for line in lines:
        if line.upper() == line and len(line) > 2 and not re.match(r"^\d", line):
            # New source (all-caps name like "DECANTER")
            if buf:
                awards.append(" — ".join(buf))
            buf = [line]
        else:
            buf.append(line)
    if buf:
        awards.append(" — ".join(buf))
    return "; ".join(awards) if awards else None

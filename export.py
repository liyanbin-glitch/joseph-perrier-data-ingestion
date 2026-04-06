"""
Export SQLite data to structured CSV files and a summary Excel workbook.

Outputs (all in output/):
    products.csv           — one row per product, all scalar fields
    grape_composition.csv  — one row per variety per product
    bottle_sizes.csv       — one row per bottle format per product
    tasting_notes.csv      — one row per product
    media.csv              — all media rows
    winery.csv             — winery record
    joseph_perrier.xlsx    — all sheets in one workbook
"""

import pandas as pd
from sqlalchemy import text
from sqlalchemy.orm import Session

from models.schema import init_db

DB_PATH = "output/winery.db"
OUT = "output"


def export_all():
    engine = init_db(DB_PATH)

    # ------------------------------------------------------------------ #
    # Build DataFrames
    # ------------------------------------------------------------------ #
    with engine.connect() as conn:

        winery_df = pd.read_sql(text("SELECT * FROM winery"), conn)

        products_df = pd.read_sql(text("""
            SELECT
                p.id,
                p.name,
                p.category,
                p.product_type,
                p.is_vintage,
                p.vintage_year,
                p.dosage_type,
                p.dosage_gl,
                p.aging_months,
                p.reserve_wine_pct,
                p.serving_temp_min_c,
                p.serving_temp_max_c,
                p.price_eur,
                p.availability,
                p.short_description,
                p.food_pairing,
                p.awards,
                p.winemaker_notes,
                p.source_url,
                p.scraped_at
            FROM product p
            ORDER BY p.id
        """), conn)

        grapes_df = pd.read_sql(text("""
            SELECT
                p.name  AS product_name,
                gc.grape_variety,
                gc.percentage,
                gc.origin_village
            FROM grape_composition gc
            JOIN product p ON p.id = gc.product_id
            ORDER BY p.id, gc.percentage DESC
        """), conn)

        sizes_df = pd.read_sql(text("""
            SELECT
                p.name      AS product_name,
                bs.size_cl,
                bs.size_label,
                bs.price_eur,
                bs.available
            FROM product_bottle_size bs
            JOIN product p ON p.id = bs.product_id
            ORDER BY p.id, bs.size_cl
        """), conn)

        tasting_df = pd.read_sql(text("""
            SELECT
                p.name  AS product_name,
                tn.color,
                tn.bubble_fineness,
                tn.nose_primary,
                tn.palate_attack,
                tn.aging_potential,
                tn.raw_text
            FROM tasting_note tn
            JOIN product p ON p.id = tn.product_id
            ORDER BY p.id
        """), conn)

        media_df = pd.read_sql(text("""
            SELECT
                p.name  AS product_name,
                m.media_type,
                m.role,
                m.url,
                m.alt_text,
                m.width_px,
                m.height_px,
                m.local_path
            FROM media m
            LEFT JOIN product p ON p.id = m.product_id
            ORDER BY p.id, m.sort_order
        """), conn)

    # ------------------------------------------------------------------ #
    # Wide pivot: one row per product, grapes as columns
    # ------------------------------------------------------------------ #
    grape_pivot = (
        grapes_df
        .pivot_table(index="product_name", columns="grape_variety",
                     values="percentage", aggfunc="first")
        .reset_index()
    )
    grape_pivot.columns.name = None

    # Wide pivot: bottle sizes as columns
    size_pivot = (
        sizes_df[["product_name", "size_label", "price_eur"]]
        .pivot_table(index="product_name", columns="size_label",
                     values="price_eur", aggfunc="first")
        .reset_index()
    )
    size_pivot.columns.name = None
    size_pivot.columns = [f"price_{c.lower().replace(' ', '_')}_eur"
                          if c != "product_name" else c
                          for c in size_pivot.columns]

    # Master table: products + grapes + sizes merged
    master_df = (
        products_df
        .merge(grape_pivot, left_on="name", right_on="product_name", how="left")
        .drop(columns=["product_name"], errors="ignore")
        .merge(size_pivot, left_on="name", right_on="product_name", how="left")
        .drop(columns=["product_name"], errors="ignore")
    )

    # ------------------------------------------------------------------ #
    # Write CSVs
    # ------------------------------------------------------------------ #
    files = {
        "winery":            winery_df,
        "products":          master_df,
        "grape_composition": grapes_df,
        "bottle_sizes":      sizes_df,
        "tasting_notes":     tasting_df,
        "media":             media_df,
    }
    for name, df in files.items():
        path = f"{OUT}/{name}.csv"
        df.to_csv(path, index=False, encoding="utf-8-sig")
        print(f"  {path}  ({len(df)} rows, {len(df.columns)} cols)")

    # ------------------------------------------------------------------ #
    # Write Excel workbook
    # ------------------------------------------------------------------ #
    xlsx_path = f"{OUT}/joseph_perrier.xlsx"
    with pd.ExcelWriter(xlsx_path, engine="openpyxl") as writer:
        for sheet_name, df in files.items():
            df.to_excel(writer, sheet_name=sheet_name, index=False)
            ws = writer.sheets[sheet_name]
            # Auto-fit column widths (capped at 60)
            for col_cells in ws.columns:
                max_len = max(
                    (len(str(cell.value)) for cell in col_cells if cell.value),
                    default=8
                )
                ws.column_dimensions[col_cells[0].column_letter].width = min(max_len + 2, 60)
    print(f"  {xlsx_path}")

    # ------------------------------------------------------------------ #
    # Console summary
    # ------------------------------------------------------------------ #
    print("\n=== Product master table preview ===")
    preview_cols = ["name", "category", "dosage_type", "dosage_gl",
                    "price_eur", "aging_months", "vintage_year"]
    print(master_df[[c for c in preview_cols if c in master_df.columns]].to_string(index=False))

    print("\n=== Grape composition ===")
    print(grapes_df.to_string(index=False))

    print("\n=== Bottle sizes & prices ===")
    print(sizes_df.to_string(index=False))


if __name__ == "__main__":
    export_all()

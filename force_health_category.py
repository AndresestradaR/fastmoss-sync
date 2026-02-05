#!/usr/bin/env python3
"""
Force specific products into the Health category.
These 50 products are manually curated and ordered by sales (desc).
They must NOT be duplicated in other categories.

IMPORTANT: Run this BEFORE classify_products.py so these products
are already set to "Health" and won't be reclassified.

Usage:
    python force_health_category.py
"""
import logging
import httpx
import os

from config import SUPABASE_URL, SUPABASE_KEY

logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(message)s')
logger = logging.getLogger(__name__)

# 50 Health products - search terms extracted from titles (order = sales desc)
HEALTH_PRODUCTS = [
    "Toplux Magnesium Compl",
    "NeoCell Collagen Bio-Pep",
    "LEEFAR Her Juicy Feminin",
    "Liquid Chloroph",           # Horbaach
    "Zero Sugar Best Seller Tri",
    "Physician's Choice Fiber",
    "LullaBites Natural Sleep",
    "Toplux Nutrition Collagen",
    "Bella All Natural - Colon",
    "CALM Relaxing Drink Mix",
    "Wellah Creatine",
    "Cata-Kor NAD+ Suppleme",
    "Carlyle Oregano Oil 4000",
    "Physician's Choice Hack",
    "JoySpring Saffron Gummi",
    "15 Day Cleanse - Gut and",
    "Magnesium Complex 100",
    "MaryRuth's Daily Liquid",
    "Physician's Choice Vagin",
    "Neuro Energy Caffeine Gu",
    "Micro Ingredients Astaxan",
    "Blood Sugar Complex 20",
    "Micro Ingredients NMN",
    "LEEFAR Her Juicy Feminin",  # Leefar Nutrition US variant
    "Physician's Choice Gut G",
    "Feminine Balance Gummi",
    "Root Labs Alpha 10-in-1",
    "Nutricost Micronized Crea",
    "FOODOLOGY",
    "Rhodiola Rosea Extract",
    "JoySpring Natural Liquid",
    "Alcedo Smart Scale",
    "Micro Ingredients Multi C",
    "URO - Boric Acid Vaginal",
    "Vitamin B12 Liq",           # 2-PACK NusavaOfficial
    "Bloom Nutrition Creatine",
    "Nutricost N-Acetyl L-Cyst",
    "3 Bottles of Goli Best Sell",
    "Kind Patches Metabolic",
    "Micro Ingredients Oregan",
    "LeeFar Cutting Drink Mix",
    "URO Women's Probiotics",
    "Liquid Chlorophyll Drops",  # Benevolent
    "24-in-1 Advanced Hair Gr",
    "Cata-Kor NAD+ Advance",
    "Nature's Sunshine Lymph",
    "Nello Supercalm",
    "HiPlus Nutrition Oregano",
    "VEV 14-in-1 Magnesium",
    "URO - Metabolism Suppor",
]


def get_all_products():
    """Fetch ALL products from Supabase."""
    headers = {
        "apikey": SUPABASE_KEY,
        "Authorization": f"Bearer {SUPABASE_KEY}",
    }
    response = httpx.get(
        f"{SUPABASE_URL}/rest/v1/fastmoss_products",
        headers=headers,
        params={
            "select": "product_id,title,category_l1,sold_count",
            "limit": 1000,
        },
        timeout=30,
    )
    response.raise_for_status()
    return response.json()


def update_product_category(product_id: str):
    """Update a product's category_l1 to Health in Supabase."""
    headers = {
        "apikey": SUPABASE_KEY,
        "Authorization": f"Bearer {SUPABASE_KEY}",
        "Content-Type": "application/json",
        "Prefer": "return=minimal",
    }
    response = httpx.patch(
        f"{SUPABASE_URL}/rest/v1/fastmoss_products",
        headers=headers,
        params={"product_id": f"eq.{product_id}"},
        json={"category_l1": "Health"},
        timeout=30,
    )
    response.raise_for_status()


def main():
    logger.info("=" * 60)
    logger.info("FORCE HEALTH CATEGORY - 50 Curated Products")
    logger.info("=" * 60)

    if not SUPABASE_URL or not SUPABASE_KEY:
        logger.error("Missing SUPABASE_URL or SUPABASE_KEY in .env")
        return

    # Fetch all products
    products = get_all_products()
    logger.info(f"Total products in DB: {len(products)}")

    # Match each search term to a product
    matched_ids = set()
    matched_products = []
    not_found = []

    for search_term in HEALTH_PRODUCTS:
        search_lower = search_term.lower()
        found = False
        for p in products:
            title = (p.get("title") or "").strip()
            pid = p["product_id"]
            # Skip already matched to avoid duplicates
            if pid in matched_ids:
                continue
            if search_lower in title.lower():
                matched_ids.add(pid)
                matched_products.append({
                    "product_id": pid,
                    "title": title,
                    "sold_count": p.get("sold_count", 0),
                    "current_category": p.get("category_l1", ""),
                })
                found = True
                break
        if not found:
            not_found.append(search_term)

    logger.info(f"\nMatched: {len(matched_products)} / {len(HEALTH_PRODUCTS)}")
    if not_found:
        logger.warning(f"NOT FOUND ({len(not_found)}):")
        for nf in not_found:
            logger.warning(f"  - {nf}")

    # Update all matched products to Health
    updated = 0
    already_health = 0
    failed = 0

    for i, mp in enumerate(matched_products):
        pid = mp["product_id"]
        title = mp["title"][:60]
        current = mp["current_category"]

        if current == "Health":
            already_health += 1
            logger.info(f"  [{i+1}] Already Health: {title}")
            continue

        try:
            update_product_category(pid)
            updated += 1
            logger.info(f"  [{i+1}] {current or 'General'} -> Health: {title}")
        except Exception as e:
            failed += 1
            logger.error(f"  [{i+1}] Failed: {title} - {e}")

    # Summary
    logger.info("")
    logger.info("=" * 60)
    logger.info("SUMMARY")
    logger.info("=" * 60)
    logger.info(f"  Total search terms:    {len(HEALTH_PRODUCTS)}")
    logger.info(f"  Matched in DB:         {len(matched_products)}")
    logger.info(f"  Updated to Health:     {updated}")
    logger.info(f"  Already Health:        {already_health}")
    logger.info(f"  Failed:                {failed}")
    logger.info(f"  Not found in DB:       {len(not_found)}")
    logger.info("=" * 60)


if __name__ == "__main__":
    main()

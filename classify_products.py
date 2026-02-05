#!/usr/bin/env python3
"""
Classify products by keywords in their titles.

This script reads products with category_l1 = "General" or empty,
and classifies them based on keywords in their titles.
"""
import logging
import httpx
from collections import Counter

from config import SUPABASE_URL, SUPABASE_KEY

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s'
)
logger = logging.getLogger(__name__)

# Category classification rules - keywords to match in title (case-insensitive)
CATEGORY_RULES = {
    "Beauty": ["makeup", "lipstick", "mascara", "foundation", "concealer", "eyeshadow", "blush", "primer", "skincare", "serum", "moisturizer", "cleanser", "toner", "sunscreen", "cream", "lotion", "beauty", "cosmetic", "lash", "brow", "nail", "perfume", "fragrance", "hair oil", "shampoo", "conditioner", "hair mask", "derma", "peel", "exfoliat"],
    "Health": ["vitamin", "supplement", "probiotic", "collagen", "protein", "wellness", "health", "medical", "therapy", "massage", "posture", "magnesium", "zinc", "omega", "fiber", "detox", "immune"],
    "Home": ["dresser", "organizer", "storage", "furniture", "shelf", "drawer", "cabinet", "lamp", "pillow", "blanket", "towel", "kitchen", "cook", "clean", "laundry", "garden", "plant", "vase", "candle", "decor", "rug", "curtain", "bedding", "mattress"],
    "Electronics": ["power bank", "charger", "phone", "cable", "speaker", "earbuds", "headphone", "bluetooth", "wireless", "led", "light", "camera", "laptop", "tablet", "usb", "adapter", "fan", "portable"],
    "Fashion": ["dress", "shirt", "pants", "jeans", "jacket", "hoodie", "sweater", "skirt", "blouse", "top", "shorts", "legging", "underwear", "bra", "sock", "shoe", "sneaker", "sandal", "boot", "bag", "purse", "wallet", "jewelry", "necklace", "bracelet", "ring", "earring", "watch", "sunglasses", "hat", "scarf"],
    "Automotive": ["car", "vehicle", "auto", "motor", "tire", "dash", "steering", "seat cover", "freshener"],
    "Baby & Kids": ["baby", "toddler", "infant", "kid", "children", "toy", "diaper", "stroller", "pacifier"],
    "Food": ["coffee", "tea", "snack", "candy", "chocolate", "seasoning", "sauce", "spice", "drink", "water bottle", "food"],
    "Pets": ["dog", "cat", "pet", "collar", "leash", "treat", "aquarium", "fish"],
    "Sports": ["gym", "fitness", "yoga", "exercise", "workout", "sport", "bicycle", "camping", "hiking", "outdoor", "water bottle"],
}


def classify_by_title(title: str) -> str:
    """
    Classify a product by keywords in its title.
    Returns the first matching category or "General" if no match.
    """
    if not title:
        return "General"

    title_lower = title.lower()

    for category, keywords in CATEGORY_RULES.items():
        for keyword in keywords:
            if keyword.lower() in title_lower:
                return category

    return "General"


def get_products_to_classify(limit: int = 500) -> list:
    """Fetch products that have category_l1 = 'General' or empty."""
    headers = {
        "apikey": SUPABASE_KEY,
        "Authorization": f"Bearer {SUPABASE_KEY}",
    }

    response = httpx.get(
        f"{SUPABASE_URL}/rest/v1/fastmoss_products",
        headers=headers,
        params={
            "select": "product_id,title,category_l1",
            "or": "(category_l1.is.null,category_l1.eq.,category_l1.eq.General)",
            "limit": limit,
        },
        timeout=30,
    )
    response.raise_for_status()
    return response.json()


def update_product_category(product_id: str, category: str):
    """Update a product's category_l1 in Supabase."""
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
        json={"category_l1": category},
        timeout=30,
    )
    response.raise_for_status()


def main():
    logger.info("=" * 60)
    logger.info("Product Classification by Title Keywords")
    logger.info("=" * 60)

    if not SUPABASE_URL or not SUPABASE_KEY:
        logger.error("Missing SUPABASE_URL or SUPABASE_KEY")
        return

    # Fetch products to classify
    logger.info("Fetching products with category 'General' or empty...")
    products = get_products_to_classify(limit=500)
    logger.info(f"Found {len(products)} products to classify")

    if not products:
        logger.info("No products need classification!")
        return

    # Classify and update each product
    category_counts = Counter()
    updated = 0
    unchanged = 0
    failed = 0

    for i, product in enumerate(products):
        product_id = product["product_id"]
        title = product.get("title", "")
        current_category = product.get("category_l1", "")

        # Classify by title
        new_category = classify_by_title(title)
        category_counts[new_category] += 1

        # Only update if category changed from General
        if new_category != "General" and current_category in ("General", "", None):
            try:
                update_product_category(product_id, new_category)
                logger.info(f"[{i+1}/{len(products)}] {new_category}: {title[:60]}...")
                updated += 1
            except Exception as e:
                logger.error(f"[{i+1}/{len(products)}] Failed to update {product_id}: {e}")
                failed += 1
        else:
            unchanged += 1
            if (i + 1) % 50 == 0:
                logger.info(f"[{i+1}/{len(products)}] Processing... ({unchanged} unchanged)")

    # Summary
    logger.info("")
    logger.info("=" * 60)
    logger.info("CLASSIFICATION COMPLETE")
    logger.info("=" * 60)
    logger.info(f"Total processed: {len(products)}")
    logger.info(f"Updated to new category: {updated}")
    logger.info(f"Remained as General: {unchanged}")
    logger.info(f"Failed: {failed}")
    logger.info("")
    logger.info("Category Distribution:")
    logger.info("-" * 40)
    for category, count in sorted(category_counts.items(), key=lambda x: -x[1]):
        logger.info(f"  {category:20} : {count:4} products")
    logger.info("=" * 60)


if __name__ == "__main__":
    main()

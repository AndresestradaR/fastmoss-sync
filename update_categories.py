#!/usr/bin/env python3
"""
Update categories for products with empty category_l1.

This script fetches products from Supabase that have empty category_l1,
then tries to get their categories from FastMoss API and updates them.
"""
import logging
import time
import httpx

from config import SUPABASE_URL, SUPABASE_KEY, FASTMOSS_TOKEN

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s'
)
logger = logging.getLogger(__name__)

# FastMoss API endpoints
FASTMOSS_DETAIL_URL = "https://www.fastmoss.com/api/goods/V2/detail"
FASTMOSS_SEARCH_URL = "https://www.fastmoss.com/api/goods/V2/search"


def get_fastmoss_headers():
    """Get headers for FastMoss API requests."""
    cookie = f"fd_tk={FASTMOSS_TOKEN}; region=US; NEXT_LOCALE=es" if FASTMOSS_TOKEN else "region=US; NEXT_LOCALE=es"
    return {
        "Cookie": cookie,
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
        "Accept": "application/json, text/plain, */*",
        "Accept-Language": "es-ES,es;q=0.9,en;q=0.8",
        "Referer": "https://www.fastmoss.com/es/products",
        "Origin": "https://www.fastmoss.com",
    }


def get_first_from_array(value, default=""):
    """Extract first element from array or return value as string."""
    if value is None:
        return default
    if isinstance(value, list):
        return str(value[0]) if value else default
    return str(value)


def get_products_without_category(limit: int = 500) -> list:
    """Fetch products that have empty category_l1."""
    headers = {
        "apikey": SUPABASE_KEY,
        "Authorization": f"Bearer {SUPABASE_KEY}",
    }

    # Fetch products where category_l1 is empty or null
    response = httpx.get(
        f"{SUPABASE_URL}/rest/v1/fastmoss_products",
        headers=headers,
        params={
            "select": "product_id,title,category_l1,category_l2,category_l3",
            "or": "(category_l1.is.null,category_l1.eq.)",
            "limit": limit,
        },
        timeout=30,
    )
    response.raise_for_status()
    return response.json()


def try_get_category_from_detail(product_id: str) -> dict | None:
    """Try to get product category from FastMoss detail endpoint."""
    try:
        response = httpx.get(
            FASTMOSS_DETAIL_URL,
            headers=get_fastmoss_headers(),
            params={"product_id": product_id},
            timeout=15,
        )

        if response.is_success:
            data = response.json()
            product = data.get("data", {})

            if product:
                category_l1 = get_first_from_array(product.get("category_name_l1") or product.get("category_l1"))
                category_l2 = get_first_from_array(product.get("category_name_l2") or product.get("category_l2"))
                category_l3 = get_first_from_array(product.get("category_name_l3") or product.get("category_l3"))

                if category_l1:
                    return {
                        "category_l1": category_l1,
                        "category_l2": category_l2,
                        "category_l3": category_l3,
                    }

        return None
    except Exception as e:
        logger.debug(f"Detail endpoint failed for {product_id}: {e}")
        return None


def try_get_category_from_search(product_id: str) -> dict | None:
    """Try to get product category by searching for product_id."""
    try:
        # Try searching with product_id filter
        response = httpx.get(
            FASTMOSS_SEARCH_URL,
            headers=get_fastmoss_headers(),
            params={
                "product_id": product_id,
                "region": "US",
                "page": 1,
                "pagesize": 10,
            },
            timeout=15,
        )

        if response.is_success:
            data = response.json()
            products = data.get("data", {}).get("product_list", [])

            # Find matching product
            for product in products:
                if str(product.get("product_id")) == str(product_id):
                    category_l1 = get_first_from_array(product.get("category_name_l1") or product.get("category_l1"))
                    category_l2 = get_first_from_array(product.get("category_name_l2") or product.get("category_l2"))
                    category_l3 = get_first_from_array(product.get("category_name_l3") or product.get("category_l3"))

                    if category_l1:
                        return {
                            "category_l1": category_l1,
                            "category_l2": category_l2,
                            "category_l3": category_l3,
                        }

        return None
    except Exception as e:
        logger.debug(f"Search endpoint failed for {product_id}: {e}")
        return None


def update_product_category(product_id: str, categories: dict):
    """Update a product's categories in Supabase."""
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
        json=categories,
        timeout=30,
    )
    response.raise_for_status()


def main():
    logger.info("=" * 60)
    logger.info("Update Categories for Products")
    logger.info("=" * 60)

    if not SUPABASE_URL or not SUPABASE_KEY:
        logger.error("Missing SUPABASE_URL or SUPABASE_KEY")
        return

    if not FASTMOSS_TOKEN:
        logger.warning("FASTMOSS_TOKEN not configured - API requests may fail")

    # Fetch products without categories
    logger.info("Fetching products with empty category_l1...")
    products = get_products_without_category(limit=500)
    logger.info(f"Found {len(products)} products without category")

    if not products:
        logger.info("All products have categories!")
        return

    # Test detail endpoint first
    logger.info("Testing FastMoss detail endpoint...")
    test_result = try_get_category_from_detail(products[0]["product_id"])
    detail_works = test_result is not None
    logger.info(f"Detail endpoint works: {detail_works}")

    # Process each product
    updated = 0
    failed = 0
    set_to_general = 0

    for i, product in enumerate(products):
        product_id = product["product_id"]
        title = product.get("title", "")[:50]

        # Try detail endpoint first (if it works), then search
        categories = None

        if detail_works:
            categories = try_get_category_from_detail(product_id)

        if not categories:
            categories = try_get_category_from_search(product_id)

        if categories:
            try:
                update_product_category(product_id, categories)
                logger.info(f"[{i+1}/{len(products)}] Updated: {product_id} -> {categories['category_l1']}")
                updated += 1
            except Exception as e:
                logger.error(f"[{i+1}/{len(products)}] Failed to update {product_id}: {e}")
                failed += 1
        else:
            # Set to "General" if no category found
            try:
                update_product_category(product_id, {"category_l1": "General"})
                logger.info(f"[{i+1}/{len(products)}] Set to General: {product_id} ({title}...)")
                set_to_general += 1
            except Exception as e:
                logger.error(f"[{i+1}/{len(products)}] Failed to set General for {product_id}: {e}")
                failed += 1

        # Rate limiting
        if (i + 1) % 5 == 0:
            time.sleep(1)

    logger.info("=" * 60)
    logger.info("Update Complete")
    logger.info(f"  Updated with real category: {updated}")
    logger.info(f"  Set to 'General': {set_to_general}")
    logger.info(f"  Failed: {failed}")
    logger.info(f"  Total processed: {len(products)}")
    logger.info("=" * 60)


if __name__ == "__main__":
    main()

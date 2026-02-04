"""Sync logic for FastMoss to Supabase."""
import logging
from datetime import datetime, timezone
from typing import List

from supabase import create_client, Client

from config import (
    SUPABASE_URL,
    SUPABASE_KEY,
    FASTMOSS_REGIONS,
    FASTMOSS_CATEGORIES,
    SYNC_LIMIT_PER_REGION,
    PAGE_SIZE,
)
from fastmoss_client import FastMossClient

logger = logging.getLogger(__name__)


def transform_product(product: dict, region: str) -> dict:
    """Transform FastMoss product to Supabase schema."""
    return {
        "product_id": str(product.get("product_id", "")),
        "region": region,
        "title": product.get("title", ""),
        "img": product.get("img", ""),
        "price": float(product.get("price", 0) or 0),
        "currency": product.get("currency", "USD"),
        "sold_count": int(product.get("sold_count", 0) or 0),
        "day7_sold_count": int(product.get("day7_sold_count", 0) or 0),
        "day28_sold_count": int(product.get("day28_sold_count", 0) or 0),
        "sale_amount": float(product.get("sale_amount", 0) or 0),
        "day7_sale_amount": float(product.get("day7_sale_amount", 0) or 0),
        "category_l1": product.get("category_l1", ""),
        "category_l2": product.get("category_l2", ""),
        "category_l3": product.get("category_l3", ""),
        "category_l1_id": str(product.get("category_l1_id", "")),
        "shop_name": product.get("shop_name", ""),
        "shop_id": str(product.get("shop_id", "")),
        "product_rating": float(product.get("product_rating", 0) or 0),
        "review_count": int(product.get("review_count", 0) or 0),
        "relate_author_count": int(product.get("relate_author_count", 0) or 0),
        "author_order_rate": float(product.get("author_order_rate", 0) or 0),
        "commission_rate": float(product.get("commission_rate", 0) or 0),
        "detail_url": product.get("detail_url", ""),
        "last_synced_at": datetime.now(timezone.utc).isoformat(),
    }


def fetch_products_for_region_category(
    client: FastMossClient,
    region: str,
    category: str,
    limit: int,
) -> List[dict]:
    """Fetch products for a specific region and category."""
    products = []
    page = 1
    pages_needed = (limit + PAGE_SIZE - 1) // PAGE_SIZE  # Ceiling division

    logger.info(f"Fetching {region}/{category}: up to {limit} products ({pages_needed} pages)")

    while len(products) < limit and page <= pages_needed:
        response = client.search_products(
            region=region,
            category_l1=category,
            page=page,
        )

        if response.get("code") != 0:
            logger.error(f"API error for {region}/{category} page {page}")
            break

        page_products = response.get("data", {}).get("list", [])
        if not page_products:
            logger.info(f"No more products for {region}/{category} at page {page}")
            break

        products.extend(page_products)
        total = response.get("data", {}).get("total", 0)
        logger.info(f"  Page {page}: got {len(page_products)} products (total available: {total})")

        page += 1

    # Trim to limit
    return products[:limit]


def upsert_products(supabase: Client, products: List[dict], region: str) -> int:
    """Upsert products to Supabase."""
    if not products:
        return 0

    transformed = [transform_product(p, region) for p in products]

    # Filter out products without product_id
    transformed = [p for p in transformed if p["product_id"]]

    if not transformed:
        return 0

    try:
        # Upsert in batches of 100
        batch_size = 100
        total_upserted = 0

        for i in range(0, len(transformed), batch_size):
            batch = transformed[i:i + batch_size]
            result = supabase.table("fastmoss_products").upsert(
                batch,
                on_conflict="product_id"
            ).execute()
            total_upserted += len(batch)
            logger.info(f"  Upserted batch {i // batch_size + 1}: {len(batch)} products")

        return total_upserted

    except Exception as e:
        logger.error(f"Upsert error: {e}")
        raise


def run_sync():
    """Run the full sync process."""
    logger.info("=" * 60)
    logger.info("FastMoss Sync Started")
    logger.info(f"Regions: {FASTMOSS_REGIONS}")
    logger.info(f"Categories: {FASTMOSS_CATEGORIES}")
    logger.info(f"Limit per region: {SYNC_LIMIT_PER_REGION}")
    logger.info("=" * 60)

    if not SUPABASE_URL or not SUPABASE_KEY:
        logger.error("Missing SUPABASE_URL or SUPABASE_KEY")
        raise ValueError("Supabase credentials not configured")

    supabase = create_client(SUPABASE_URL, SUPABASE_KEY)
    total_synced = 0
    errors = []

    with FastMossClient() as fastmoss:
        for region in FASTMOSS_REGIONS:
            region = region.strip()
            if not region:
                continue

            logger.info(f"\n{'='*40}")
            logger.info(f"Processing region: {region}")
            logger.info(f"{'='*40}")

            region_products = []

            for category in FASTMOSS_CATEGORIES:
                category = category.strip()
                if not category:
                    continue

                try:
                    # Calculate limit per category (distribute evenly)
                    limit_per_category = SYNC_LIMIT_PER_REGION // len(FASTMOSS_CATEGORIES)

                    products = fetch_products_for_region_category(
                        fastmoss,
                        region,
                        category,
                        limit_per_category,
                    )

                    region_products.extend(products)
                    logger.info(f"  Category {category}: fetched {len(products)} products")

                except Exception as e:
                    error_msg = f"Error fetching {region}/{category}: {e}"
                    logger.error(error_msg)
                    errors.append(error_msg)

            # Upsert all products for this region
            if region_products:
                try:
                    upserted = upsert_products(supabase, region_products, region)
                    total_synced += upserted
                    logger.info(f"Region {region}: synced {upserted} products total")
                except Exception as e:
                    error_msg = f"Error upserting {region}: {e}"
                    logger.error(error_msg)
                    errors.append(error_msg)

    logger.info("\n" + "=" * 60)
    logger.info("FastMoss Sync Completed")
    logger.info(f"Total products synced: {total_synced}")
    if errors:
        logger.warning(f"Errors encountered: {len(errors)}")
        for err in errors:
            logger.warning(f"  - {err}")
    logger.info("=" * 60)

    return total_synced, errors

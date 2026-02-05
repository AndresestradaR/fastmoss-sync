#!/usr/bin/env python3
"""
Migrate existing product images to Supabase Storage.

This script fetches all products from Supabase that still have FastMoss CDN URLs
and attempts to download and re-upload them to Supabase Storage.
"""
import logging
import time
import httpx

from config import SUPABASE_URL, SUPABASE_KEY
from sync import (
    SupabaseClient,
    download_image,
    get_image_filename,
    IMAGE_BUCKET,
)

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s'
)
logger = logging.getLogger(__name__)


def get_products_with_external_images(supabase_url: str, supabase_key: str, limit: int = 100) -> list:
    """Fetch products that still have external (non-Supabase) image URLs."""
    headers = {
        "apikey": supabase_key,
        "Authorization": f"Bearer {supabase_key}",
    }

    # Fetch products where img does not contain 'supabase'
    response = httpx.get(
        f"{supabase_url}/rest/v1/fastmoss_products",
        headers=headers,
        params={
            "select": "product_id,img",
            "img": "not.ilike.*supabase*",
            "limit": limit,
        },
        timeout=30,
    )
    response.raise_for_status()
    return response.json()


def update_product_image(supabase_url: str, supabase_key: str, product_id: str, new_img: str):
    """Update a product's img field in Supabase."""
    headers = {
        "apikey": supabase_key,
        "Authorization": f"Bearer {supabase_key}",
        "Content-Type": "application/json",
        "Prefer": "return=minimal",
    }

    response = httpx.patch(
        f"{supabase_url}/rest/v1/fastmoss_products",
        headers=headers,
        params={"product_id": f"eq.{product_id}"},
        json={"img": new_img},
        timeout=30,
    )
    response.raise_for_status()


def main():
    logger.info("=" * 60)
    logger.info("Image Migration to Supabase Storage")
    logger.info("=" * 60)

    if not SUPABASE_URL or not SUPABASE_KEY:
        logger.error("Missing SUPABASE_URL or SUPABASE_KEY")
        return

    supabase = SupabaseClient(SUPABASE_URL, SUPABASE_KEY)

    # Fetch products with external images
    logger.info("Fetching products with external images...")
    products = get_products_with_external_images(SUPABASE_URL, SUPABASE_KEY, limit=500)
    logger.info(f"Found {len(products)} products to migrate")

    if not products:
        logger.info("No products need migration")
        return

    # Process each product
    success = 0
    failed = 0
    skipped = 0

    for i, product in enumerate(products):
        product_id = product.get("product_id")
        original_img = product.get("img", "")

        if not original_img:
            skipped += 1
            continue

        # Download image
        result = download_image(original_img)
        if not result:
            logger.warning(f"[{i+1}/{len(products)}] Failed to download: {product_id}")
            failed += 1
            continue

        image_data, content_type = result

        # Upload to Supabase Storage
        filename = get_image_filename(product_id, original_img)
        new_url = supabase.upload_image(IMAGE_BUCKET, filename, image_data, content_type)

        if new_url:
            # Update product record
            try:
                update_product_image(SUPABASE_URL, SUPABASE_KEY, product_id, new_url)
                logger.info(f"[{i+1}/{len(products)}] Migrated: {product_id}")
                success += 1
            except Exception as e:
                logger.error(f"[{i+1}/{len(products)}] Failed to update DB: {product_id} - {e}")
                failed += 1
        else:
            logger.warning(f"[{i+1}/{len(products)}] Failed to upload: {product_id}")
            failed += 1

        # Rate limiting
        if (i + 1) % 10 == 0:
            time.sleep(1)

    logger.info("=" * 60)
    logger.info("Migration Complete")
    logger.info(f"  Success: {success}")
    logger.info(f"  Failed: {failed}")
    logger.info(f"  Skipped: {skipped}")
    logger.info("=" * 60)


if __name__ == "__main__":
    main()

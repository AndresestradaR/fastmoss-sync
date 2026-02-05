"""Sync logic for FastMoss to Supabase."""
import logging
import hashlib
from datetime import datetime, timezone
from typing import List, Optional
from urllib.parse import urlparse

import httpx

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

# Image bucket name in Supabase Storage
IMAGE_BUCKET = "product-images"


class SupabaseClient:
    """Simple Supabase client using REST API directly."""

    def __init__(self, url: str, key: str):
        self.url = url.rstrip("/")
        self.key = key
        self.headers = {
            "apikey": key,
            "Authorization": f"Bearer {key}",
            "Content-Type": "application/json",
            "Prefer": "return=minimal,resolution=merge-duplicates",
        }

    def upsert(self, table: str, data: list) -> httpx.Response:
        """Upsert data to Supabase table."""
        response = httpx.post(
            f"{self.url}/rest/v1/{table}?on_conflict=product_id",
            headers=self.headers,
            json=data,
            timeout=30,
        )
        if not response.is_success:
            logger.error(f"Supabase error response: {response.text}")
        response.raise_for_status()
        return response

    def upload_image(self, bucket: str, path: str, image_data: bytes, content_type: str = "image/jpeg") -> Optional[str]:
        """
        Upload image to Supabase Storage.

        Returns the public URL if successful, None otherwise.
        """
        upload_headers = {
            "apikey": self.key,
            "Authorization": f"Bearer {self.key}",
            "Content-Type": content_type,
            "x-upsert": "true",  # Overwrite if exists
        }

        try:
            response = httpx.post(
                f"{self.url}/storage/v1/object/{bucket}/{path}",
                headers=upload_headers,
                content=image_data,
                timeout=30,
            )

            if response.is_success:
                # Return public URL
                public_url = f"{self.url}/storage/v1/object/public/{bucket}/{path}"
                return public_url
            else:
                logger.warning(f"Failed to upload image {path}: {response.status_code} - {response.text[:200]}")
                return None

        except Exception as e:
            logger.warning(f"Error uploading image {path}: {e}")
            return None


def download_image(url: str) -> Optional[tuple[bytes, str]]:
    """
    Download image from URL with proper headers for FastMoss CDN.

    Returns tuple of (image_data, content_type) or None if failed.
    """
    if not url:
        return None

    try:
        response = httpx.get(
            url,
            headers={
                "Referer": "https://www.fastmoss.com/",
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
            },
            timeout=15,
            follow_redirects=True,
        )

        if response.is_success:
            content_type = response.headers.get("content-type", "image/jpeg")
            return (response.content, content_type)
        else:
            logger.debug(f"Failed to download image: {response.status_code}")
            return None

    except Exception as e:
        logger.debug(f"Error downloading image from {url[:50]}...: {e}")
        return None


def get_image_filename(product_id: str, original_url: str) -> str:
    """Generate a unique filename for the image based on product_id and URL hash."""
    # Use product_id as primary identifier
    # Add hash of original URL to handle cases where same product might have different images
    url_hash = hashlib.md5(original_url.encode()).hexdigest()[:8]

    # Determine extension from URL
    parsed = urlparse(original_url)
    path = parsed.path.lower()
    if ".png" in path:
        ext = "png"
    elif ".webp" in path:
        ext = "webp"
    elif ".gif" in path:
        ext = "gif"
    else:
        ext = "jpg"

    return f"{product_id}_{url_hash}.{ext}"


def safe_int(value, default=0) -> int:
    """Safely convert value to int."""
    if value is None:
        return default
    try:
        return int(value)
    except (ValueError, TypeError):
        return default


def safe_float(value, default=0.0) -> float:
    """Safely convert value to float."""
    if value is None:
        return default
    try:
        # Handle price strings like "$27.35" or "27.35"
        if isinstance(value, str):
            # Remove currency symbols and commas
            cleaned = value.replace("$", "").replace(",", "").strip()
            return float(cleaned) if cleaned else default
        return float(value)
    except (ValueError, TypeError):
        return default


def safe_str(value, default="") -> str:
    """Safely convert value to string."""
    if value is None:
        return default
    return str(value)


def get_first_from_array(value, default="") -> str:
    """Extract first element from array or return value as string."""
    if value is None:
        return default
    if isinstance(value, list):
        return str(value[0]) if value else default
    return str(value)


def transform_product(product: dict, region: str) -> dict:
    """Transform FastMoss product to Supabase schema."""
    try:
        # Handle commission_rate - API uses "crate" field, comes as "13%" string
        commission_rate = product.get("crate") or product.get("commission_rate") or ""
        if commission_rate is None:
            commission_rate = ""
        commission_rate = str(commission_rate)

        # Handle author count - may be relate_author_count or author_count
        author_count = product.get("relate_author_count") or product.get("author_count") or 0

        # Handle category_id - may be category_id or category_l1_id
        category_l1_id = product.get("category_id") or product.get("category_l1_id") or ""

        # Handle categories - API returns category_name_l1, category_name_l2, category_name_l3 as ARRAYS
        # We need to extract the first element from each array
        category_l1 = get_first_from_array(product.get("category_name_l1") or product.get("category_l1"))
        category_l2 = get_first_from_array(product.get("category_name_l2") or product.get("category_l2"))
        category_l3 = get_first_from_array(product.get("category_name_l3") or product.get("category_l3"))

        transformed = {
            "product_id": safe_str(product.get("product_id")),
            "region": region,
            "title": safe_str(product.get("title")),
            "img": safe_str(product.get("img")),
            "price": safe_float(product.get("price")),
            "currency": safe_str(product.get("currency"), "USD"),
            "sold_count": safe_int(product.get("sold_count")),
            "day7_sold_count": safe_int(product.get("day7_sold_count")),
            "day28_sold_count": safe_int(product.get("day28_sold_count")),
            "sale_amount": safe_float(product.get("sale_amount")),
            "day7_sale_amount": safe_float(product.get("day7_sale_amount")),
            "category_l1": category_l1,
            "category_l2": category_l2,
            "category_l3": category_l3,
            "category_l1_id": safe_str(category_l1_id),
            "shop_name": safe_str(product.get("shop_name")),
            "shop_id": safe_str(product.get("shop_id")),
            "product_rating": safe_float(product.get("product_rating")),
            "review_count": safe_int(product.get("review_count")),
            "relate_author_count": safe_int(author_count),
            "commission_rate": commission_rate,  # Keep as string (e.g., "13%") - table has TEXT type
            "detail_url": safe_str(product.get("detail_url")),
            "last_synced_at": datetime.now(timezone.utc).isoformat(),
        }

        return transformed

    except Exception as e:
        logger.error(f"Error transforming product: {e}")
        logger.error(f"Product data: {product}")
        raise


def fetch_products_for_region_category(
    client: FastMossClient,
    region: str,
    category: str,
    limit: int,
) -> List[dict]:
    """Fetch products for a specific region and category with pagination."""
    import time

    products = []
    page = 1
    products_per_page = 5  # FastMoss Basic plan returns 5 per page
    pages_needed = (limit + products_per_page - 1) // products_per_page  # Ceiling division

    logger.info(f"Fetching {region}/{category}: target {limit} products (~{pages_needed} pages)")

    while len(products) < limit and page <= pages_needed:
        response = client.search_products(
            region=region,
            category_l1=category,
            page=page,
        )

        # Get products from response - field is "product_list", ignore error codes
        page_products = response.get("data", {}).get("product_list", [])

        if not page_products:
            logger.info(f"  Page {page}: no more products available")
            break

        products.extend(page_products)
        total = response.get("data", {}).get("total", 0)
        logger.info(f"  Page {page}/{pages_needed}: +{len(page_products)} products (accumulated: {len(products)}, available: {total})")

        # Log first product keys on first page for debugging
        if page == 1 and page_products:
            logger.debug(f"  Product fields: {list(page_products[0].keys())}")

        # Check if we got less than expected (no more pages)
        if len(page_products) < products_per_page:
            logger.info(f"  Reached end of results at page {page}")
            break

        page += 1

        # Delay between pagination requests to avoid rate limiting
        if page <= pages_needed and len(products) < limit:
            time.sleep(1)

    # Trim to limit
    return products[:limit]


def process_product_image(supabase: SupabaseClient, product: dict) -> str:
    """
    Download product image and upload to Supabase Storage.

    Returns the new Supabase Storage URL, or the original URL if upload fails.
    """
    original_url = product.get("img", "")
    product_id = product.get("product_id", "")

    if not original_url or not product_id:
        return original_url

    # Skip if already a Supabase URL
    if "supabase" in original_url:
        return original_url

    # Download image
    result = download_image(original_url)
    if not result:
        logger.debug(f"Could not download image for product {product_id}")
        return original_url

    image_data, content_type = result

    # Generate filename and upload
    filename = get_image_filename(product_id, original_url)
    new_url = supabase.upload_image(IMAGE_BUCKET, filename, image_data, content_type)

    if new_url:
        logger.debug(f"Uploaded image for {product_id}: {filename}")
        return new_url
    else:
        return original_url


def upsert_products(supabase: SupabaseClient, products: List[dict], region: str) -> int:
    """Upsert products to Supabase."""
    if not products:
        logger.warning(f"No products to upsert for region {region}")
        return 0

    logger.info(f"Transforming {len(products)} products for region {region}")

    # Log raw product sample BEFORE transformation for debugging
    if products:
        sample = products[0]
        logger.info("=" * 50)
        logger.info("RAW PRODUCT SAMPLE (before transform):")
        logger.info(f"  Keys: {list(sample.keys())}")
        logger.info(f"  sold_count: {sample.get('sold_count')}")
        logger.info(f"  day7_sold_count: {sample.get('day7_sold_count')}")
        logger.info(f"  price: {sample.get('price')}")
        logger.info(f"  product_rating: {sample.get('product_rating')}")
        logger.info(f"  relate_author_count: {sample.get('relate_author_count')}")
        logger.info(f"  crate: {sample.get('crate')}")
        logger.info(f"  commission_rate: {sample.get('commission_rate')}")
        logger.info(f"  category_name_l1: {sample.get('category_name_l1')}")
        logger.info(f"  category_l1: {sample.get('category_l1')}")
        logger.info(f"  shop_name: {sample.get('shop_name')}")
        logger.info("=" * 50)

    transformed = []
    for i, p in enumerate(products):
        try:
            t = transform_product(p, region)
            if t["product_id"]:
                transformed.append(t)
            else:
                logger.warning(f"Product {i} has no product_id, skipping")
        except Exception as e:
            logger.error(f"Error transforming product {i}: {e}")
            continue

    if not transformed:
        logger.warning(f"No valid products after transformation for region {region}")
        return 0

    # Process images - download from FastMoss and upload to Supabase Storage
    logger.info(f"Processing images for {len(transformed)} products...")
    images_uploaded = 0
    images_failed = 0

    for product in transformed:
        original_img = product["img"]
        new_img = process_product_image(supabase, product)
        product["img"] = new_img

        if new_img != original_img and "supabase" in new_img:
            images_uploaded += 1
        elif original_img and "supabase" not in new_img:
            images_failed += 1

    logger.info(f"  Images: {images_uploaded} uploaded, {images_failed} failed/skipped")

    logger.info(f"Upserting {len(transformed)} products to Supabase")

    # Log first transformed product for debugging
    if transformed:
        logger.debug(f"Sample transformed product: {transformed[0]}")

    try:
        # Upsert in batches of 100
        batch_size = 100
        total_upserted = 0

        for i in range(0, len(transformed), batch_size):
            batch = transformed[i:i + batch_size]
            try:
                supabase.upsert("fastmoss_products", batch)
                total_upserted += len(batch)
                logger.info(f"  Upserted batch {i // batch_size + 1}: {len(batch)} products")
            except Exception as batch_error:
                logger.error(f"  Error upserting batch {i // batch_size + 1}: {batch_error}")
                # Log first product of failed batch for debugging
                if batch:
                    logger.error(f"  First product in failed batch: {batch[0]}")
                raise

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

    # Verify env vars
    logger.info(f"SUPABASE_URL configured: {bool(SUPABASE_URL)}")
    logger.info(f"SUPABASE_KEY configured: {bool(SUPABASE_KEY)}")

    if not SUPABASE_URL or not SUPABASE_KEY:
        logger.error("Missing SUPABASE_URL or SUPABASE_KEY")
        raise ValueError("Supabase credentials not configured")

    supabase = SupabaseClient(SUPABASE_URL, SUPABASE_KEY)
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

            # Deduplicate by product_id (same product may appear in multiple categories)
            if region_products:
                seen_ids = set()
                unique_products = []
                for p in region_products:
                    pid = p.get("product_id")
                    if pid and pid not in seen_ids:
                        seen_ids.add(pid)
                        unique_products.append(p)
                logger.info(f"Deduplicated: {len(region_products)} -> {len(unique_products)} unique products")
                region_products = unique_products

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
            else:
                logger.warning(f"No products fetched for region {region}")

    logger.info("\n" + "=" * 60)
    logger.info("FastMoss Sync Completed")
    logger.info(f"Total products synced: {total_synced}")
    if errors:
        logger.warning(f"Errors encountered: {len(errors)}")
        for err in errors:
            logger.warning(f"  - {err}")
    logger.info("=" * 60)

    return total_synced, errors

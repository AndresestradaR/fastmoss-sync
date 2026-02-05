#!/usr/bin/env python3
"""
Test script to verify FastMoss API response structure.
Run this to see exactly what fields the API returns.
"""
import json
import logging
from fastmoss_client import FastMossClient

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s'
)
logger = logging.getLogger(__name__)


def main():
    logger.info("=" * 60)
    logger.info("FastMoss API Test Fetch")
    logger.info("=" * 60)

    with FastMossClient() as client:
        # Fetch one page of products from US region, Beauty category (14)
        logger.info("Fetching products from US region, category 14 (Beauty)...")

        response = client.search_products(
            region="US",
            category_l1="14",
            page=1,
        )

        code = response.get("code")
        data = response.get("data", {})
        products = data.get("product_list", [])
        total = data.get("total", 0)

        logger.info(f"Response code: {code}")
        logger.info(f"Total available: {total}")
        logger.info(f"Products returned: {len(products)}")

        if not products:
            logger.error("No products returned! Check your FASTMOSS_TOKEN.")
            return

        # Show first product structure
        first_product = products[0]

        logger.info("")
        logger.info("=" * 60)
        logger.info("FIRST PRODUCT - ALL KEYS:")
        logger.info("=" * 60)
        for key in sorted(first_product.keys()):
            logger.info(f"  {key}")

        logger.info("")
        logger.info("=" * 60)
        logger.info("FIRST PRODUCT - KEY VALUES (metrics fields):")
        logger.info("=" * 60)

        # Numeric fields we care about
        fields_to_check = [
            "product_id",
            "title",
            "price",
            "sold_count",
            "day7_sold_count",
            "day28_sold_count",
            "sale_amount",
            "day7_sale_amount",
            "product_rating",
            "review_count",
            "relate_author_count",
            "author_count",
            "crate",
            "commission_rate",
            "category_name_l1",
            "category_name_l2",
            "category_name_l3",
            "category_l1",
            "category_l2",
            "category_l3",
            "category_id",
            "category_l1_id",
            "shop_name",
            "shop_id",
            "img",
            "detail_url",
        ]

        for field in fields_to_check:
            value = first_product.get(field)
            value_type = type(value).__name__
            # Truncate long strings
            if isinstance(value, str) and len(value) > 80:
                display_value = value[:80] + "..."
            else:
                display_value = value
            logger.info(f"  {field}: {display_value} ({value_type})")

        # Also print full JSON of first product for reference
        logger.info("")
        logger.info("=" * 60)
        logger.info("FIRST PRODUCT - FULL JSON:")
        logger.info("=" * 60)
        print(json.dumps(first_product, indent=2, ensure_ascii=False))


if __name__ == "__main__":
    main()

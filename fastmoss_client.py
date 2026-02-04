"""FastMoss API client for TikTok Shop product data."""
import time
import random
import logging
import json
from typing import Optional
import httpx

from config import REQUEST_DELAY, MAX_RETRIES, PAGE_SIZE, FASTMOSS_TOKEN

logger = logging.getLogger(__name__)

FASTMOSS_API_URL = "https://www.fastmoss.com/api/goods/V2/search"


class FastMossClient:
    """Client for FastMoss API with retry logic and rate limiting."""

    def __init__(self):
        # Build headers to simulate full browser
        if FASTMOSS_TOKEN:
            cookie = f"fd_tk={FASTMOSS_TOKEN}; region=US; NEXT_LOCALE=es"
            logger.info("FastMoss token configured (fd_tk cookie)")
        else:
            cookie = "region=US; NEXT_LOCALE=es"
            logger.warning("FASTMOSS_TOKEN not configured - requests may fail")

        headers = {
            "Cookie": cookie,
            "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
            "Accept": "application/json, text/plain, */*",
            "Accept-Language": "es-ES,es;q=0.9,en;q=0.8",
            "Accept-Encoding": "gzip, deflate, br",
            "Referer": "https://www.fastmoss.com/es/products",
            "Origin": "https://www.fastmoss.com",
            "Sec-Ch-Ua": '"Chromium";v="122", "Not(A:Brand";v="24", "Google Chrome";v="122"',
            "Sec-Ch-Ua-Mobile": "?0",
            "Sec-Ch-Ua-Platform": '"macOS"',
            "Sec-Fetch-Dest": "empty",
            "Sec-Fetch-Mode": "cors",
            "Sec-Fetch-Site": "same-origin",
            "Connection": "keep-alive",
        }

        self.client = httpx.Client(timeout=30.0, headers=headers)
        self.last_request_time = 0

    def _wait_for_rate_limit(self):
        """Ensure minimum delay between requests."""
        elapsed = time.time() - self.last_request_time
        if elapsed < REQUEST_DELAY:
            time.sleep(REQUEST_DELAY - elapsed)
        self.last_request_time = time.time()

    def search_products(
        self,
        region: str,
        category_l1: str,
        page: int = 1,
        pagesize: int = PAGE_SIZE,
        order: str = "2,2",  # Sales 7d descending
        price_min: Optional[float] = None,
        price_max: Optional[float] = None,
        min_sales_7d: Optional[int] = None,
        max_sales_7d: Optional[int] = None,
        min_commission: Optional[float] = None,
    ) -> dict:
        """
        Search products on FastMoss.

        Args:
            region: Country code (US, MX, BR, etc.)
            category_l1: L1 category ID (14=Beauty, 25=Health, etc.)
            page: Page number (1-indexed)
            pagesize: Results per page (max 50)
            order: Sort order (2,2 = sales 7d desc)
            price_min: Minimum price USD
            price_max: Maximum price USD
            min_sales_7d: Minimum 7-day sales
            max_sales_7d: Maximum 7-day sales (-1 for no limit)
            min_commission: Minimum affiliate commission %

        Returns:
            API response dict with 'code' and 'data' keys
        """
        params = {
            "page": page,
            "pagesize": pagesize,
            "order": order,
            "region": region,
            "l1_cid": category_l1,
            "_time": int(time.time() * 1000),
            "cnonce": random.randint(100000, 999999),
        }

        # Optional filters
        if price_min is not None or price_max is not None:
            price_range = f"{price_min or 0},{price_max or -1}"
            params["price_amount"] = price_range

        if min_sales_7d is not None or max_sales_7d is not None:
            sales_range = f"{min_sales_7d or 0},{max_sales_7d or -1}"
            params["day7_sold_count"] = sales_range

        if min_commission is not None:
            params["crate"] = f"{min_commission},-1"

        logger.debug(f"Request params: {params}")

        # Retry logic
        for attempt in range(MAX_RETRIES):
            try:
                self._wait_for_rate_limit()

                response = self.client.get(FASTMOSS_API_URL, params=params)
                response.raise_for_status()

                data = response.json()

                # Log raw response for debugging
                code = data.get("code")
                logger.debug(f"API response code: {code}")

                # Check if we got products
                products = data.get("data", {}).get("list", [])
                total = data.get("data", {}).get("total", 0)

                if products:
                    # Log first product structure for debugging
                    logger.debug(f"First product keys: {list(products[0].keys())}")
                    logger.info(f"API returned {len(products)} products (total: {total})")
                else:
                    logger.warning(f"API returned empty list. Code: {code}, Response keys: {list(data.keys())}")
                    # Log more details if empty
                    if "msg" in data:
                        logger.warning(f"API message: {data.get('msg')}")

                # Return data regardless of code - let caller handle it
                # Some valid responses may have code != 0
                return data

            except httpx.HTTPStatusError as e:
                logger.error(f"HTTP error {e.response.status_code} on attempt {attempt + 1}")
                logger.error(f"Response: {e.response.text[:500]}")
                if attempt < MAX_RETRIES - 1:
                    time.sleep(2 ** attempt)  # Exponential backoff
                else:
                    raise

            except httpx.RequestError as e:
                logger.error(f"Request error on attempt {attempt + 1}: {e}")
                if attempt < MAX_RETRIES - 1:
                    time.sleep(2 ** attempt)
                else:
                    raise

            except json.JSONDecodeError as e:
                logger.error(f"JSON decode error: {e}")
                logger.error(f"Raw response: {response.text[:500]}")
                return {"code": -1, "data": {"list": [], "total": 0}}

        return {"code": -1, "data": {"list": [], "total": 0}}

    def close(self):
        """Close the HTTP client."""
        self.client.close()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

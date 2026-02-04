import asyncio
import logging
from playwright.async_api import async_playwright
from datetime import datetime, timezone
import os
from dotenv import load_dotenv
import httpx

load_dotenv()
logger = logging.getLogger(__name__)

class FastMossScraper:
    def __init__(self):
        self.token = os.getenv("FASTMOSS_TOKEN")
        self.categories = os.getenv("FASTMOSS_CATEGORIES", "14").split(",")
        self.target_per_category = int(os.getenv("SCRAPE_TARGET_PER_CATEGORY", "50"))
        self.supabase_url = os.getenv("SUPABASE_URL")
        self.supabase_key = os.getenv("SUPABASE_KEY")

    async def scrape_category(self, page, category_id: str) -> list:
        """Scrape products from a category page"""
        products = []
        url = f"https://www.fastmoss.com/es/products?region=US&l1_cid={category_id}"

        logger.info(f"Navigating to category {category_id}")
        await page.goto(url, wait_until="networkidle")
        await asyncio.sleep(2)

        # Scroll to load more products
        last_count = 0
        scroll_attempts = 0
        max_scrolls = 20

        while len(products) < self.target_per_category and scroll_attempts < max_scrolls:
            # Extract current products
            product_cards = await page.query_selector_all('[class*="product-card"], [class*="ProductCard"], [class*="goods-item"]')

            if not product_cards:
                # Try alternative selectors
                product_cards = await page.query_selector_all('a[href*="/product/"]')

            logger.info(f"  Found {len(product_cards)} product elements")

            # Scroll down
            await page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
            await asyncio.sleep(1.5)

            scroll_attempts += 1

            if len(product_cards) == last_count:
                logger.info(f"  No new products after scroll {scroll_attempts}")
                break
            last_count = len(product_cards)

        # Extract data from product cards
        products = await self.extract_products_from_page(page)
        logger.info(f"  Extracted {len(products)} products from category {category_id}")

        return products[:self.target_per_category]

    async def extract_products_from_page(self, page) -> list:
        """Extract product data from the current page"""
        # This will need adjustment based on actual FastMoss HTML structure
        products = await page.evaluate('''() => {
            const products = [];
            // Try to find product data in window/page state
            if (window.__NUXT__ && window.__NUXT__.data) {
                // Nuxt apps often store data here
                console.log("Found NUXT data");
            }

            // Extract from DOM
            const cards = document.querySelectorAll('[class*="goods-item"], [class*="product-card"], a[href*="/product/"]');
            cards.forEach(card => {
                try {
                    const link = card.querySelector('a[href*="/product/"]') || card;
                    const href = link?.href || '';
                    const productIdMatch = href.match(/product\\/([0-9]+)/);

                    const img = card.querySelector('img');
                    const titleEl = card.querySelector('[class*="title"], [class*="name"], h3, h4');
                    const priceEl = card.querySelector('[class*="price"]');
                    const soldEl = card.querySelector('[class*="sold"], [class*="sales"]');

                    if (productIdMatch) {
                        products.push({
                            product_id: productIdMatch[1],
                            title: titleEl?.textContent?.trim() || '',
                            img: img?.src || '',
                            price_text: priceEl?.textContent?.trim() || '',
                            sold_text: soldEl?.textContent?.trim() || ''
                        });
                    }
                } catch(e) {}
            });
            return products;
        }''')
        return products

    async def run(self):
        """Main scraping function"""
        logger.info("Starting FastMoss scraper with Playwright")

        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=False)  # headless=False for debugging
            context = await browser.new_context()

            # Set the auth cookie
            await context.add_cookies([{
                "name": "fd_tk",
                "value": self.token,
                "domain": ".fastmoss.com",
                "path": "/"
            }])

            page = await context.new_page()

            all_products = []
            for category_id in self.categories:
                try:
                    products = await self.scrape_category(page, category_id.strip())
                    all_products.extend(products)
                    await asyncio.sleep(2)  # Delay between categories
                except Exception as e:
                    logger.error(f"Error scraping category {category_id}: {e}")

            await browser.close()

        # Deduplicate
        seen = set()
        unique = []
        for p in all_products:
            if p['product_id'] not in seen:
                seen.add(p['product_id'])
                unique.append(p)

        logger.info(f"Total unique products scraped: {len(unique)}")
        return unique

async def main():
    logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(name)s: %(message)s')
    scraper = FastMossScraper()
    products = await scraper.run()
    print(f"Scraped {len(products)} products")
    for p in products[:5]:
        print(f"  - {p['product_id']}: {p['title'][:50]}...")

if __name__ == "__main__":
    asyncio.run(main())

-- FastMoss Products Table
-- Run this in Supabase SQL Editor before deploying the sync service

CREATE TABLE IF NOT EXISTS fastmoss_products (
    id BIGSERIAL PRIMARY KEY,
    product_id TEXT UNIQUE NOT NULL,
    region TEXT NOT NULL,
    title TEXT,
    img TEXT,
    price DECIMAL(10,2),
    currency TEXT DEFAULT 'USD',
    sold_count INTEGER DEFAULT 0,
    day7_sold_count INTEGER DEFAULT 0,
    day28_sold_count INTEGER DEFAULT 0,
    sale_amount DECIMAL(15,2) DEFAULT 0,
    day7_sale_amount DECIMAL(15,2) DEFAULT 0,
    category_l1 TEXT,
    category_l2 TEXT,
    category_l3 TEXT,
    category_l1_id TEXT,
    shop_name TEXT,
    shop_id TEXT,
    product_rating DECIMAL(3,2) DEFAULT 0,
    review_count INTEGER DEFAULT 0,
    relate_author_count INTEGER DEFAULT 0,
    author_order_rate DECIMAL(5,2) DEFAULT 0,
    commission_rate DECIMAL(5,2) DEFAULT 0,
    detail_url TEXT,
    last_synced_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

-- Indexes for common queries
CREATE INDEX IF NOT EXISTS idx_fm_region ON fastmoss_products(region);
CREATE INDEX IF NOT EXISTS idx_fm_category ON fastmoss_products(category_l1_id);
CREATE INDEX IF NOT EXISTS idx_fm_sales7d ON fastmoss_products(day7_sold_count DESC);
CREATE INDEX IF NOT EXISTS idx_fm_price ON fastmoss_products(price);
CREATE INDEX IF NOT EXISTS idx_fm_commission ON fastmoss_products(commission_rate DESC);
CREATE INDEX IF NOT EXISTS idx_fm_last_synced ON fastmoss_products(last_synced_at DESC);

-- Composite index for filtered queries
CREATE INDEX IF NOT EXISTS idx_fm_region_category_sales
ON fastmoss_products(region, category_l1_id, day7_sold_count DESC);

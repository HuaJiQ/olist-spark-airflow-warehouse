
-- 1. 每日销售指标表
CREATE TABLE IF NOT EXISTS ads_daily_sales (
    dt DATE PRIMARY KEY,
    total_order_count BIGINT NOT NULL,
    delivered_order_count BIGINT NOT NULL,
    delivered_item_amount NUMERIC(20, 2),
    delivered_freight_amount NUMERIC(20, 2),
    delivered_aov NUMERIC(20, 2)
);

-- 2. 客户购买画像表
CREATE TABLE IF NOT EXISTS ads_customer_purchase_profile (
    customer_unique_id VARCHAR(32) PRIMARY KEY,
    delivered_order_count BIGINT NOT NULL,
    delivered_item_amount NUMERIC(20, 2),
    is_repeat_customer BOOLEAN NOT NULL
);

-- 3. 历史累计客户复购指标表
CREATE TABLE IF NOT EXISTS ads_customer_repurchase (
    metric_id INTEGER PRIMARY KEY
        CHECK (metric_id = 1),
    purchasing_customer_count BIGINT NOT NULL,
    single_purchase_customer_count BIGINT NOT NULL,
    repeat_customer_count BIGINT NOT NULL,
    repurchase_rate DOUBLE PRECISION
);

-- 4. 商品类别销售指标表
CREATE TABLE IF NOT EXISTS ads_category_sales (
    category_name TEXT PRIMARY KEY,
    item_count BIGINT NOT NULL,
    order_count BIGINT NOT NULL,
    item_amount NUMERIC(20, 2),
    freight_amount NUMERIC(20, 2)
);

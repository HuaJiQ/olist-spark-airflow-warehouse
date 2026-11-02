
-- 记录已成功提交到PostgreSQL的ADS批次
CREATE TABLE IF NOT EXISTS ads_publication_log (
    batch_id VARCHAR(64) PRIMARY KEY,
    published_at TIMESTAMPTZ NOT NULL
        DEFAULT CURRENT_TIMESTAMP,
    daily_sales_rows BIGINT NOT NULL,
    customer_profile_rows BIGINT NOT NULL,
    customer_repurchase_rows BIGINT NOT NULL,
    category_sales_rows BIGINT NOT NULL,
    status VARCHAR(20) NOT NULL
        CHECK (status = 'PUBLISHED')
);

-- 记录当前对外服务的批次
CREATE TABLE IF NOT EXISTS ads_current_batch (
    singleton_id INTEGER PRIMARY KEY
        CHECK (singleton_id = 1),
    batch_id VARCHAR(64) NOT NULL
        REFERENCES ads_publication_log(batch_id),
    updated_at TIMESTAMPTZ NOT NULL
        DEFAULT CURRENT_TIMESTAMP
);

"""Olist source table contracts: filename and required source columns."""
TABLES = {
    "orders": ("olist_orders_dataset.csv", ["order_id", "customer_id", "order_status", "order_purchase_timestamp"]),
    "order_items": ("olist_order_items_dataset.csv", ["order_id", "order_item_id", "product_id", "seller_id", "price", "freight_value"]),
    "order_payments": ("olist_order_payments_dataset.csv", ["order_id", "payment_sequential", "payment_type", "payment_value"]),
    "customers": ("olist_customers_dataset.csv", ["customer_id", "customer_unique_id"]),
    "products": ("olist_products_dataset.csv", ["product_id", "product_category_name"]),
    "sellers": ("olist_sellers_dataset.csv", ["seller_id"]),
    "order_reviews": ("olist_order_reviews_dataset.csv", ["review_id", "order_id"]),
    "geolocation": ("olist_geolocation_dataset.csv", ["geolocation_zip_code_prefix"]),
    "category_translation": ("product_category_name_translation.csv", ["product_category_name", "product_category_name_english"]),
}

import csv
from pathlib import Path
from collections import Counter

ROOT = Path(r"D:\SparkEcommerce\data\raw\olist")


def read_rows(filename):
    path = ROOT / filename

    with path.open(
        "r",
        encoding="utf-8-sig",
        newline=""
    ) as f:
        return list(csv.DictReader(f))


def audit_key(rows, key):
    values = [
        row[key].strip()
        for row in rows
    ]

    non_null = [v for v in values if v]

    print(f"总行数: {len(rows)}")
    print(f"{key}空值: {len(values) - len(non_null)}")
    print(
        f"{key}重复: "
        f"{len(non_null) - len(set(non_null))}"
    )

    return set(non_null)


def check_fk(rows, column, valid_keys):
    values = [
        row[column].strip()
        for row in rows
    ]

    null_count = sum(not v for v in values)

    unmatched = {
        v for v in values
        if v and v not in valid_keys
    }

    print(f"{column}空值行数: {null_count}")
    print(f"{column}未匹配的不同ID数: {len(unmatched)}")


def main():
    customers = read_rows("olist_customers_dataset.csv")
    products = read_rows("olist_products_dataset.csv")
    sellers = read_rows("olist_sellers_dataset.csv")

    orders = read_rows("olist_orders_dataset.csv")
    items = read_rows("olist_order_items_dataset.csv")

    print("\n=== 客户维度 ===")
    customer_keys = audit_key(customers, "customer_id")

    unique_ids = [
        row["customer_unique_id"].strip()
        for row in customers
    ]

    counts = Counter(v for v in unique_ids if v)

    print("customer_unique_id去重数:", len(counts))
    print("customer_unique_id空值:", sum(not v for v in unique_ids))
    print(
        "关联多条客户记录的unique_id数:",
        sum(n > 1 for n in counts.values())
    )
    print("最多关联客户记录数:", max(counts.values()))

    print("\n=== 商品维度 ===")
    product_keys = audit_key(products, "product_id")

    print("\n=== 商家维度 ===")
    seller_keys = audit_key(sellers, "seller_id")

    print("\n=== 外键覆盖检查 ===")

    print("订单 -> 客户")
    check_fk(orders, "customer_id", customer_keys)

    print("订单明细 -> 商品")
    check_fk(items, "product_id", product_keys)

    print("订单明细 -> 商家")
    check_fk(items, "seller_id", seller_keys)


if __name__ == "__main__":
    main()
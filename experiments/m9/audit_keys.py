import csv
from pathlib import Path
from collections import Counter

ROOT = Path(r"D:\SparkEcommerce\data\raw\olist")

FILES = {
    "orders": (
        "olist_orders_dataset.csv",
        ("order_id",)
    ),
    "items": (
        "olist_order_items_dataset.csv",
        ("order_id", "order_item_id")
    ),
    "payments": (
        "olist_order_payments_dataset.csv",
        ("order_id", "payment_sequential")
    ),
}

def audit_table(name, filename, key_columns):
    path = ROOT / filename
    keys = set()
    order_counts = Counter()

    total = 0
    null_keys = 0
    duplicate_keys = 0

    with path.open("r", encoding="utf-8-sig", newline="") as f:
        reader = csv.DictReader(f)

        for row in reader:
            total += 1

            key = tuple(row[col].strip() for col in key_columns)

            if any(not value for value in key):
                null_keys += 1
            elif key in keys:
                duplicate_keys += 1
            else:
                keys.add(key)

            order_id = row["order_id"].strip()
            if order_id:
                order_counts[order_id] += 1

    print(f"\n=== {name} ===")
    print(f"总行数: {total}")
    print(f"主键空值行数: {null_keys}")
    print(f"重复候选主键行数: {duplicate_keys}")
    print(f"不同订单数: {len(order_counts)}")
    print(f"一笔订单最多记录数: {max(order_counts.values())}")

    return order_counts


def main():
    counts = {}

    for name, (filename, key_columns) in FILES.items():
        counts[name] = audit_table(
            name, filename, key_columns
        )

    orders = counts["orders"]
    items = counts["items"]
    payments = counts["payments"]

    print("\n=== 关联基数检查 ===")

    for name, detail in [
        ("items", items),
        ("payments", payments),
    ]:
        missing = set(detail) - set(orders)
        multi = sum(n > 1 for n in detail.values())

        print(f"{name} 多记录订单数: {multi}")
        print(f"{name} 找不到对应订单数: {len(missing)}")

    common = set(items) & set(payments)

    join_rows = sum(
        items[oid] * payments[oid]
        for oid in common
    )

    print("\n=== Join膨胀检查 ===")
    print(f"同时有商品和支付的订单数: {len(common)}")
    print(f"商品与支付直接Join产生的行数: {join_rows}")
    print(
        "这些订单原有商品明细行数:",
        sum(items[oid] for oid in common)
    )

    risky = [
        (oid, items[oid], payments[oid])
        for oid in common
        if items[oid] > 1 and payments[oid] > 1
    ]

    print(f"商品和支付均为多条的订单数: {len(risky)}")

    for oid, ni, np in sorted(
        risky, key=lambda x: x[1] * x[2], reverse=True
    )[:5]:
        print(
            f"订单 {oid}: "
            f"商品{ni}条 × 支付{np}条 = {ni*np}行"
        )


if __name__ == "__main__":
    main()
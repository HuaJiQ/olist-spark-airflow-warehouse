
# M10 销售主题域设计

## 1. 建设目标

基于 Olist 电商数据集，构建日销售主题数据集市，
统一订单量、商品销售额、运费及支付金额等核心指标。

## 2. 数据来源

- DWD.fact_order
- DWD.fact_order_item
- DWD.fact_payment

## 3. 统计粒度

一天一行，以订单购买日期 purchase_date 作为统计日期。

## 4. 指标定义

| 指标 | 定义 |
|---|---|
| order_count | 当日订单去重数量 |
| item_order_count | 当日存在商品明细的订单数 |
| payment_order_count | 当日存在支付记录的订单数 |
| item_line_count | 当日商品明细行数 |
| item_amount | 当日商品金额之和 |
| freight_amount | 当日运费之和 |
| payment_amount | 当日支付金额之和 |
| delivered_order_count | 当日已交付订单数 |
| canceled_order_count | 当日已取消订单数 |
| avg_item_amount_per_order | 商品金额 / 有商品订单数 |
| avg_payment_amount_per_order | 支付金额 / 有支付订单数 |

## 5. 建模原则

1. 商品明细先按订单聚合。
2. 支付明细先按订单聚合。
3. 再与订单主表关联。
4. 避免多事实表直接关联产生数据膨胀。
5. 使用订单购买日期进行日统计。
6. 金额使用 Decimal 类型，避免浮点精度误差。

## 6. 数据质量要求

- 每个统计日期只能有一行。
- 日期不能为空。
- 订单数量必须为非负整数。
- 金额不能出现意外的负数。
- 商品金额、运费、支付金额必须与DWD来源数据核对。
- 订单总数必须与订单主表按日期聚合的结果一致。
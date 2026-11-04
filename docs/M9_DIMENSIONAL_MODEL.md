# M9 维度建模与订单财务汇总设计

## 1. 模块概述

M9在已有Olist电商数仓项目的基础上，进一步建设订单、商品明细和支付事实表，以及客户、商品和商家维度表，并形成订单粒度的财务汇总模型。

本模块重点解决以下问题：

1. 明确事实表与维度表的业务粒度和关联主键。
2. 对原始字段进行类型标准化和基础质量检查。
3. 避免订单商品明细与支付记录直接关联导致的多对多Join膨胀。
4. 实现商品金额、运费金额和支付金额的跨层守恒检查。
5. 对缺失业务记录和金额不一致情况保留审计证据。

M9输出与此前M1—M8使用的数据目录隔离，避免影响原有ADS构建及发布链路。

## 2. 数据分层与目录

### 2.1 ODS层

ODS层保存由Olist公开数据集导入的基础数据，以Parquet格式存储。

主要来源包括：

- orders
- order_items
- order_payments
- customers
- products
- sellers
- category_translation

### 2.2 DWD层

M9 DWD输出路径：

`data/m9/dwd/`

包含：

| 数据表 | 业务粒度 | 业务主键 | 记录数 |
|---|---|---|---:|
| fact_order | 一笔订单 | order_id | 99,441 |
| fact_order_item | 一条订单商品明细 | order_id + order_item_id | 112,650 |
| fact_payment | 一条订单支付记录 | order_id + payment_sequential | 103,886 |
| dim_customer | 一条客户记录 | customer_id | 99,441 |
| dim_product | 一个商品 | product_id | 32,951 |
| dim_seller | 一个商家 | seller_id | 3,095 |

以上主键采用Olist源数据中的业务标识，当前版本未引入数仓代理键。

### 2.3 DWS层

M9 DWS输出路径：

`data/m9/dws/order_financial_summary`

粒度：一笔订单一行。

记录数：99,441。

以`fact_order`作为主表，将商品明细和支付明细分别按`order_id`汇总后，通过LEFT JOIN建立订单级财务汇总。

## 3. 核心数据模型

### 3.1 订单事实表 fact_order

主键：`order_id`

主要属性：

- order_id
- customer_id
- order_status
- purchase_ts
- approved_ts
- carrier_ts
- delivered_ts
- estimated_delivery_ts
- purchase_date

时间字段由源数据中的字符串转换为Timestamp，purchase_date为Date类型。

### 3.2 商品明细事实表 fact_order_item

联合主键：`order_id + order_item_id`

主要属性：

- order_id
- order_item_id
- product_id
- seller_id
- shipping_limit_ts
- price
- freight_value

price和freight_value采用Decimal(18,2)类型，避免使用浮点数进行财务金额累计。

### 3.3 支付事实表 fact_payment

联合主键：`order_id + payment_sequential`

主要属性：

- order_id
- payment_sequential
- payment_type
- payment_installments
- payment_value

payment_value采用Decimal(18,2)类型。

需要区分支付记录数、支付方式数及分期数。一个订单可以有多条支付记录，也可以同时采用多种支付方式。

### 3.4 客户维度 dim_customer

主键：`customer_id`

主要属性：

- customer_id
- customer_unique_id
- customer_zip_code_prefix
- customer_city
- customer_state

其中：

- customer_id：客户记录标识，用于订单和客户维度关联。
- customer_unique_id：识别跨订单的实际客户身份。

当前客户维度以customer_id为粒度，不能直接把其99,441行视为99,441个独立客户。

customer_unique_id去重后共有96,096个。

### 3.5 商品维度 dim_product

主键：`product_id`

主要属性：

- product_id
- product_category_name
- product_category_name_english
- product_name_length
- product_description_length
- product_photos_qty
- product_weight_g
- product_length_cm
- product_height_cm
- product_width_cm

商品维度关联品类翻译表，同时保留原始葡萄牙语分类。

源数据中的`product_name_lenght`、`product_description_lenght`等拼写在DWD中进行了规范化。

已发现610条商品缺少品类，13条商品未匹配到英文品类。这些记录未被删除。

### 3.6 商家维度 dim_seller

主键：`seller_id`

主要属性：

- seller_id
- seller_zip_code_prefix
- seller_city
- seller_state

邮编字段保留字符串类型，避免前导零丢失。

## 4. 事实表与维度表关联关系

主要关联关系：

- fact_order.customer_id → dim_customer.customer_id
- fact_order_item.order_id → fact_order.order_id
- fact_payment.order_id → fact_order.order_id
- fact_order_item.product_id → dim_product.product_id
- fact_order_item.seller_id → dim_seller.seller_id

所有五组关联关系已经通过外键覆盖检查。

本模块中，fact_order_item和fact_payment均为事实表，且同一个order_id在两张表中都可能出现多次。

因此不能在未统一粒度之前直接Join并计算订单金额。

## 5. 多对多Join风险与解决方案

### 5.1 问题

历史审计发现：

- 商品明细112,650行。
- 支付记录103,886行。
- 参与商品明细和支付明细Inner Join的商品明细为112,647行。
- 直接按order_id关联后得到117,601行。

相较参与关联的商品明细，净增加4,954行。

例如某笔订单有3条商品明细、2条支付记录，直接关联将生成6行，可能导致金额重复累计。

### 5.2 解决方案

采用“先聚合、再Join”的设计：

1. fact_order_item按order_id聚合商品金额、运费及明细数量。
2. fact_payment按order_id聚合支付金额及支付记录数量。
3. 以fact_order为主表，通过LEFT JOIN关联两张订单级聚合结果。

各输入聚合表在order_id粒度上唯一，从而避免多对多关联造成行数膨胀。

## 6. DWS订单财务汇总指标

主要指标：

| 字段 | 业务含义 |
|---|---|
| item_count | 订单商品明细记录数 |
| item_amount | 订单商品明细price之和 |
| freight_amount | 订单商品明细freight_value之和 |
| payment_count | 订单支付记录数 |
| payment_amount | 订单payment_value之和 |
| has_items | 是否存在商品明细 |
| has_payment | 是否存在支付记录 |

本版本保留缺失指标的NULL值，不将不存在的支付记录自动解释为支付金额0。

### 6.1 金额定义

订单商品金额：

`item_amount = SUM(price)`

订单运费：

`freight_amount = SUM(freight_value)`

订单支付金额：

`payment_amount = SUM(payment_value)`

完整订单的对账差额：

`difference = payment_amount - item_amount - freight_amount`

该差额只表示不同来源的金额口径不一致，不能直接解释为退款、优惠券或利润。

## 7. 数据质量验收

统一验收程序：

`src/m9/validate_m9.py`

当前固定Olist全量快照的验收结果：

- 32项P0规则通过。
- 六张DWD表的记录数和主键质量符合预期。
- 五组关联关系的外键覆盖检查通过。
- DWS订单总数为99,441，order_id唯一。
- 商品金额、运费金额、支付金额的跨层汇总结果完全一致。

财务对账结果：

| 指标 | DWD汇总 | DWS汇总 |
|---|---:|---:|
| 商品金额（BRL） | 13,591,643.70 | 13,591,643.70 |
| 运费金额（BRL） | 2,251,909.54 | 2,251,909.54 |
| 支付金额（BRL） | 16,008,872.12 | 16,008,872.12 |

## 8. 业务数据边界

实际数据中存在：

- 775笔订单缺少商品明细。
- 1笔订单缺少支付记录。
- 303笔商品和支付记录均存在的订单，其金额差异超过设定的0.01 BRL容差。
- 610条商品缺少品类。
- 13条商品未匹配英文品类。

这些现象已在专项审计中分析，当前不能全部归类为源系统错误。

统一质量验收区分P0阻断规则与P1/P2观察问题，避免将允许存在的源数据缺失全部作为构建失败条件。

## 9. 当前实现范围与后续计划

已实现：

- 基础事实表与维度表构建。
- 字段类型规范化。
- 订单粒度财务汇总。
- 跨事实表Join风险审计。
- 金额守恒检查。
- 订单完整性与生命周期专项分析。
- 统一质量验收。

尚未在M9实现：

- 维度历史版本管理（SCD Type 2）。
- 增量ETL与变更数据处理。
- 生产环境数据质量告警与持久化。
- 基于真实大规模集群的性能基准测试。
- 将M9模型完整接入此前的Airflow生产发布流程。

后续将在M10建设复杂业务指标，M11探索增量处理，并逐步补充性能优化与自动化测试。

本模块目前基于本地Docker与Spark运行，属于可复现的学习项目，不代表已经验证PB级生产处理能力。

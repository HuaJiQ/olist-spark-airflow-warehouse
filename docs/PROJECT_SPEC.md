# 基于 Spark 的电商离线数仓与数据质量治理平台

版本：v0.2（2026-10-29）  |  状态：需求基线，尚未完成端到端验收

## 1. 项目目标与边界

基于 Olist 公开历史电商数据，开发可重复部署、可重跑、可审计的批处理数据链路：九张 CSV → ODS Parquet → DWD 事实/维度 → DWS 汇总 → ADS 指标 → PostgreSQL 服务。通过 Airflow 编排、质量测试与 Spark 性能实验形成求职作品。项目不代表真实生产部署或真实在线流数据。

**用户场景**：运营查看订单趋势/销售指标；履约团队查看交付情况；数据开发人员能监控批次、检测异常、重跑任务。

**非目标**：第一阶段不引入 Kafka/Flink、分布式 Hadoop 集群、dbt，也不冒充实时增量。避免把模拟负载当成真实生产数据。

## 2. 技术约束

- 开发机 Windows，工作目录 `D:\SparkEcommerce`；Docker Desktop 已可使用。
- 使用官方 Docker Spark 镜像 `spark:4.2.0-python3`，初期 `local[2]`，在容器中执行 PySpark；真实数据存放于 `data/raw/olist`。
- ODS / DWD / DWS / ADS 初期以 Parquet 文件落地，显式定义路径、Schema、覆盖与批次语义。
- 在 SQL 服务层引入 PostgreSQL；在端到端数据链路可运行后再接入 Airflow。
- 数据原始文件不可被 ETL 修改；不将真实 CSV、派生数据或密钥提交 Git。

## 3. 原始数据源

Olist Brazilian E-Commerce Public Dataset (Kaggle): https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce

源表九张：`orders`、`order_items`、`order_payments`、`customers`、`products`、`sellers`、`order_reviews`、`geolocation`、`category_translation`。

已在用户本地实测（2026-10-29）：
- `orders` = 99,441 行，`order_id` 去重后 99,441。
- `order_items` = 112,650 行，具有多条明细的订单 = 9,803，最多 21 条。
- `order_payments` = 103,886 行，具有多条支付记录的订单 = 2,961，最多 29 条。
- 其他六表行数未在当前项目对话中实测，禁止将网上参考行数当作验收事实。

## 4. 数据模型与核心业务规则

| 逻辑表 | 粒度 | 候选键 | 说明 |
| --- | --- | --- | --- |
| DWD 订单 | 每个 `order_id` 一条 | `order_id` | 状态、下单/批准/配送/签收等时间 |
| DWD 订单明细 | 每个订单商品明细行一条 | (`order_id`,`order_item_id`) | `price`、`freight_value` 单独计量 |
| DWD 支付 | 每个支付顺序号一条 | (`order_id`,`payment_sequential`) | 支付金额与支付方式，不直接同明细做多对多 Join |
| 客户维度 | 每个 `customer_id` 一条 | `customer_id` | 用 `customer_unique_id` 分析跨订单客户行为，需核查语义 |
| 商品维度 | 每个 `product_id` 一条 | `product_id` | 与类别翻译表关联时验证基数 |

- 各候选键是否唯一、非空必须由数据质量测试确认；不能由名称推断。
- 金额统一以 Decimal 表示，记录金额字段不可解析、为空的行数。`item_total = sum(price)`，`freight_total = sum(freight_value)`，`payment_total = sum(payment_value)`，分别先按 `order_id` 聚合再关联。不能直接在商品明细×支付明细 Join 后对金额求和。
- **GMV 暂不写死为 payment_total**：先制定是否含运费、是否过滤取消订单、支付状态和退款处理等明确口径，并说明 Olist 数据可能不支持完整退款链路。
- 用户复购率必须说明主体键、时间窗口、分母；物流时效仅对相应时间字段有效的订单计算。

## 5. 数据分层

**ODS**：保留原始列和值（字符串），九表完整接入为 Parquet；记录源路径、列、行数、关键键空值；M1 采用全量覆盖快照。

**DWD**：显式类型、时间转换、唯一性及关联检查；订单/明细/支付独立事实表和必要维度。避免无说明的去重及丢行。

**DWS**：按订单、客户、日期、商品主题汇总，保留可追溯的指标中间值。

**ADS**：订单数量趋势、商品销售、支付方式、交付时效等，经业务口径批准的指标发布至 PostgreSQL。

## 6. 里程碑及验收（开发顺序，不是绝对日历）

| 阶段 | 交付物 | 必须通过的验收 |
| --- | --- | --- |
| M1 九表接入 | `src/ingestion/`、ODS Parquet、JSON报告、README | 九源表均识别；列存在；输出可重新读取；行数对账；订单ID唯一非空；两次运行行数稳定；缺文件明确失败 |
| M2 DWD建模 | 事实/维度表、模型文档、质量规则 | 唯一性、空值、金额转换、外键覆盖、Join不膨胀 |
| M3 业务指标 | DWS/ADS、指标口径、SQL | 数量与金额可追溯，抽样和全量对账一致 |
| M4 服务层 | PostgreSQL schema、导入流程 | 发布后SQL查询正确；重复发布不产生重复记录 |
| M5 历史回放 | 业务日期批次、审计表、重跑策略 | 重跑同批次结果稳定；模拟异常批次隔离；不冒充实时流 |
| M6 调度 | Airflow DAG / Compose | 依赖关系、自动重试、失败可见、成功可核验 |
| M7 质量治理 | 自动化单元/集成测试、异常样例 | 关键规则触发能阻断错误发布并形成报告 |
| M8 优化与交付 | Spark UI/执行计划对照、最终README | 相同输入/指标基线对照，正确性一致，完整部署说明 |

## 7. 当前M1执行契约

输入：`data/raw/olist/*.csv`，九个约定文件名，表名和必需字段来自 `olist_manifest.py`。

输出：`data/ods/<table>/` 下的Parquet目录，`reports/m1_ods_quality.json`。

操作：只读原始文件；验证九个输入存在再开始；保存源列为字符串；对订单主键做非空/重复检查；每张表统计行数。重复运行采取全量重建。`PASS`仅代表已实现的质量规则通过。

**已识别的改进需求**：现有M1原型直接逐表覆盖并在最后写报告，中途失败可能留下部分新ODS与旧ODS混合；不具备原子发布与完整的读回对账。M1 首轮可先运行诊断，但在声称工程级验收通过之前，必须补上输出读回校验，并设计临时输出目录和批次发布策略。禁止把这一版本说成具备生产级原子性。

## 8. 协作与完成定义

每个任务必须包含：需求、输入输出、最小验收、运行命令、测试证据、错误修复记录。学生独立完成关键代码，导师负责解释原理和审查；不以仅能运行的单脚本视为完整模块。

开源参考：
- https://github.com/abaqasif-aa/olist_data_pipeline_end
- https://github.com/NovaStrix/olist-pipeline-data-eng
- https://github.com/ENikopoulos/Olist-E-Commerce-Pipeline

借鉴架构思路，保留自身实现、来源与差异记录；不宣称参与了上述开源仓库。

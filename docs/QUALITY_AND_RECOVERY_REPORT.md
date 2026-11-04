# M8-02｜Olist 电商数仓：数据质量与故障恢复验证报告

> 文档版本：v1.0｜整理日期：2026-11-03｜项目：`D:\SparkEcommerce`  
> 数据来源：本项目 M1—M7 已提供的本地命令输出、Manifest、Airflow 日志及 Git 提交记录。  
> **证据等级说明：**「已观测」表示曾看到实际命令结果；「Mock 测试」表示用模拟依赖验证 Python 分支；「代码审查」仅证明当前实现中存在该逻辑；「待验证」表示没有足够的可复现运行证据。本报告不把这些等级混为一谈。

## 1. 执行摘要

项目基于 Olist 公开电商数据，在本地使用 Spark 构建 ODS → DWD → DWS → ADS 数据仓库；Airflow 编排 ADS 批次计算、质量校验及发布，PostgreSQL 提供四张 ADS 服务表。项目已观测到九任务 DAG 全部成功，并完成当前批次状态、服务表行数、发布历史、Manifest 来源追踪及发布恢复 Mock 测试。

**本阶段结论：** 数据量与发布一致性有实际输出支持；代码具有事务级互斥、原子替换及幂等恢复分支；4/4 项自动化 Mock 测试通过。尚未在隔离数据库执行本轮真实故障注入、锁竞争测试与全自动端到端回归，因此不声称这些边界已完全验收。

## 2. 数据资产与口径核验

### 2.1 原始数据（ODS输入，已观测）

| 原始数据 | 行数 | 证据 / 说明 |
|---|---:|---|
| orders | 99,441 | CSV装载及订单键去重结果 |
| order_items | 112,650 | CSV装载记录 |
| order_payments | 103,886 | CSV装载记录 |
| customers | 99,441 | CSV装载记录 |
| products | 32,951 | CSV装载记录 |
| sellers | 3,095 | CSV装载记录 |

订单唯一性核验：`orders` 总行数 99,441，唯一 `order_id` 数 99,441，重复差值为 **0**（已观测）。该结论限于订单ID唯一性，不等于所有表均无重复或缺失。

**历史数据问题：** `order_reviews` CSV 读取曾遇到包含逗号、双引号和空行等非规则记录，使用 `FAILFAST` 时触发 `MALFORMED_RECORD_IN_PARSING` / `MALFORMED_CSV_RECORD`。这一问题说明源数据接入需要明确 CSV 转义、引号、解析模式和异常行处理策略；现有对话没有提供该表最终逐条修复率，因此此处不记为“全部异常清零”。

### 2.2 数仓汇总与ADS（已观测）

| 数据对象 | 行数 | 备注 |
|---|---:|---|
| DWS `order_summary` | 99,441 | 订单粒度汇总 |
| ADS `daily_sales` | 634 | 日销售指标 |
| ADS `customer_purchase_profile` | 93,358 | 客户购买画像 |
| ADS `customer_repurchase` | 1 | 复购汇总结果 |
| ADS `category_sales` | 74 | 品类销售指标 |

另有此前计算指标：交付商品金额约 **13,221,498.11 BRL**、复购客户数 **2,801**。这些属于本项目既定口径，不能直接等同于平台 GMV 或自然业务报表中的“总营收”；正式对外展示前应在 SQL 口径文件中补充订单状态、退款、运费、日期等过滤与定义。

### 2.3 当前服务层行数核对（已观测）

2026-11-03，本地 `ads_current_batch` 指向 `m6_full_cb29be2ec233b189`；该批次 Manifest 为 `PUBLISHED`，四张表均 `quality_pass: true`。PostgreSQL 实际 `COUNT(*)` 和对应 Manifest 记录相等：

| 表名 | PostgreSQL `COUNT(*)` | Manifest `row_count` | 结果 |
|---|---:|---:|---|
| `ads_daily_sales` | 634 | 634 | 一致 |
| `ads_customer_purchase_profile` | 93,358 | 93,358 | 一致 |
| `ads_customer_repurchase` | 1 | 1 | 一致 |
| `ads_category_sales` | 74 | 74 | 一致 |

> 行数一致是必要检查，但不能单独证明金额、主键、空值和所有业务口径完全正确；这部分还依赖 `validate_batch.py` 与 `check_database_metrics()` 的规则。报告不虚构未见到的详细规则清单。

## 3. 质量门禁与批次生命周期

### 3.1 批次产物隔离

每个新批次将 ADS Parquet 写入 `data/batches/<batch_id>/ads/<table>/`，避免多个批次直接共享同一组待发布目录。Manifest 记录表路径、行数、质量检查结果、检查时间及批次状态。

典型链路：

```text
CREATED → RUNNING → VALIDATED → PUBLISHED
                    ↘ FAILED（按代码允许的失败转换）
```

质量门禁分别通过 `src/pipeline/validate_batch.py` 与 `src/pipeline/check_publish_ready.py` 实现；未满足发布条件的批次不能进入正式发布流程。

### 3.2 DAG编排（历史已观测 9/9 成功）

```text
prepare_batch → build_daily_sales → build_customer_ads
 → build_category_sales → record_all_ads → validate_batch
 → check_publish_ready → publish_batch → verify_publication
```

2026-11-03 已观测到 `olist_m6_ads_full` 一次九任务全通过的真实调度；发布批次为 `m6_full_cb29be2ec233b189`。另有两次成功/失败的 DAG Run 历史状态记录，不应把某次失败任务误写成“生产事故”。

### 3.3 Manifest 来源链路（M7实际任务验证）

通过 `airflow tasks test olist_m6_ads_full prepare_batch ...`，实际创建批次 `m6_full_6c226d1f2ad8644f`。Manifest 记录：

```json
{
  "batch_id": "m6_full_6c226d1f2ad8644f",
  "source": {
    "system": "airflow",
    "dag_id": "olist_m6_ads_full",
    "run_id": "__airflow_temporary_run_2026-11-03T13:02:43.463701+00:00__"
  }
}
```

任务完成 `prepare_batch` 后该批次状态为 `RUNNING`；因下游任务被有意省略，人工设置为 `FAILED` 并写入测试终止原因，`source` 仍被保留。**这里 `FAILED` 是测试清理动作，不表示 Airflow `prepare_batch` 执行失败**。单任务测试的临时 Run ID 也不能作为正常九任务 DAG 全链路运行的替代证据。

## 4. PostgreSQL 发布一致性及幂等保护

### 4.1 事务结构（代码审查）

`src/pipeline/publish_batch.py` 的受控发布流程包括：

1. 进入 `psycopg.connect(...)` 连接上下文；
2. 获取 `pg_advisory_xact_lock(2026110201)` 事务级建议锁；
3. 查询 `ads_current_batch` 与 `ads_publication_log`；
4. 对未发布且已 `VALIDATED` 的批次，在同一事务中 `TRUNCATE` 四张ADS表、`COPY`数据并核验指标；
5. 插入发布日志，并更新当前批次指针；
6. 成功退出事务上下文后更新本地 Manifest 状态。

同一锁键只对**遵循该约定的发布程序**提供互斥，不能阻止绕开这条发布路径直接操作表的其他会话。并且互斥锁不能保证“先生成批次必先发布”的业务版本顺序。

### 4.2 发布历史与当前指针（已观测）

本地只读 SQL 结果：

- `ads_current_batch.batch_id = m6_full_cb29be2ec233b189`；
- `ads_publication_log` 共有 **4条**，均显示 `PUBLISHED`：`batch_isolation_002`、`batch_rollback_test_001`、`m6_full_9a33e0342b88ccf9`、`m6_full_cb29be2ec233b189`；
- 已发布历史批次中，当前批次以外的 **3条**符合「历史批次重新发布应拒绝」的数据库判断条件；当前批次符合「允许识别为重复请求/恢复」条件。

这些只读结果验证了判断所需数据库状态，**不等于执行了真实重复写入测试**。

### 4.3 故障边界（设计/代码审查）

| 故障场景 | 设计行为 | 证据等级 |
|---|---|---|
| 批次状态为 `RUNNING` | 拒绝发布 | Mock 测试通过 |
| 当前批次重复执行 | 检测到已有发布日志，不再次 `TRUNCATE` | Mock 测试通过 |
| 历史已发布批次重试 | 当前指针不同，拒绝旧批次覆盖 | Mock 测试通过 |
| PostgreSQL提交成功但Manifest未更新 | 识别已发布、补写 `PUBLISHED` | Mock 测试通过 |
| `COPY`中途出错 | 借助事务回滚保护上一版快照 | 代码审查；本轮未执行隔离DB故障注入 |
| 两个发布实例并发 | 共享建议锁的程序应串行进入临界区 | 代码审查；本轮未执行并发实测 |
| 较旧但从未发布的 `VALIDATED` 批次后来发布 | 当前逻辑未证明会拒绝该场景 | 待增补业务版本顺序规则 |

## 5. 自动化测试：4/4通过（Mock，不触及真实DB）

测试文件：`tests/pipeline/test_publish_recovery.py`；提交 `a818d68`。执行命令：

```powershell
.\.venv-serving\Scripts\python.exe -m unittest `
    discover -s tests/pipeline -p "test_publish_recovery.py" -v
```

2026-11-03 已观测输出：`Ran 4 tests in 0.018s`、`OK`。

| 测试方法 | 模拟条件 | 核心断言 | 结果 |
|---|---|---|---|
| `test_reject_running_batch` | Manifest `RUNNING` | 抛出“Batch is not ready for publication” | PASS |
| `test_republish_current_batch` | Manifest `PUBLISHED`；发布日志与当前指针均为该批次 | 不执行 `TRUNCATE`；不再更新Manifest | PASS |
| `test_reject_historical_republish` | 历史A已发布；当前指针为B | 抛出拒绝异常；不执行 `TRUNCATE` | PASS |
| `test_recover_after_database_commit` | 数据库已发布A，Manifest仍为`VALIDATED` | 调用 `update_status(A, PUBLISHED)`；不重复 `TRUNCATE` 或插入发布日志 | PASS |

测试使用 `unittest.mock.patch` 隔离 `psycopg.connect`、数据装载及密码配置，已确认不再出现密码提示。**断言Mock调用并不等同于真实持久化、事务竞争或数据文件读写验证。**

## 6. 批次健康扫描：异常分层与Airflow关联

脚本：`src/pipeline/detect_stale_batches.py`；提交 `dc6b579`。扫描器只读运行，不自动把超时批次判为失败。

```powershell
.\.venv-serving\Scripts\python.exe `
    .\src\pipeline\detect_stale_batches.py `
    --threshold-minutes 60

.\.venv-serving\Scripts\python.exe `
    .\src\pipeline\detect_stale_batches.py `
    --threshold-minutes 1 --check-airflow
```

历史观测样例（阈值为1分钟，当时扫描12批次）：

| 项目 | 数量 |
|---|---:|
| Scanned batches | 12 |
| Active warnings | 6 |
| Historical info | 2 |
| No warnings | 4 |

示例分类：

- `SOURCE_UNKNOWN`：旧版Manifest没有来源字段，不能推断其Airflow运行情况；
- `DAG_RUN_NOT_FOUND`：有`run_id`，但在当前可查询的运行列表中没有精确匹配，不等于证明它从未存在；
- `DAG_RUN_FAILED`：匹配到真实状态为 `failed` 的运行记录；
- `PATH_MISMATCH` / `BATCH_OUTPUT_MISSING`：历史路径与预期批次隔离目录不一致、产物目录缺失；
- `STALE_CANDIDATE`：`CREATED` 或 `RUNNING` 超过时间阈值，**只是待调查候选**。

用于验证 `DAG_RUN_FAILED` 的 `m7_failed_run_probe_001` 是**手工制造并引用真实失败Run ID**的测试Manifest，不应误写为“Airflow实际生成的失败批次”。

当前 `--check-airflow` 通过主机Docker CLI查询容器，属于本地实现；迁移到独立主机或容器内部，需要改成适用的API访问机制。

## 7. 一组可复现的只读检查命令

从 `D:\SparkEcommerce` 根目录运行。检查命令不执行 `--apply`，不覆盖数据库。

**7.1 数据库当前批次和发布数量**

```powershell
docker exec olist-postgres psql -U olist -d olist_warehouse -c "
SELECT batch_id FROM ads_current_batch WHERE singleton_id = 1;
SELECT COUNT(*) AS publication_count FROM ads_publication_log;
"
```

**7.2 四表行数**

```powershell
docker exec olist-postgres psql -U olist -d olist_warehouse -c "
SELECT 'daily_sales' AS table_name, COUNT(*) AS row_count FROM ads_daily_sales
UNION ALL SELECT 'customer_purchase_profile', COUNT(*) FROM ads_customer_purchase_profile
UNION ALL SELECT 'customer_repurchase', COUNT(*) FROM ads_customer_repurchase
UNION ALL SELECT 'category_sales', COUNT(*) FROM ads_category_sales;
"
```

**7.3 当前批次Manifest（示例批次ID来自本轮已观测记录）**

```powershell
.\.venv-serving\Scripts\python.exe .\src\pipeline\batch_manager.py show `
    --batch-id m6_full_cb29be2ec233b189
```

**7.4 Airflow状态与单元测试**

```powershell
docker exec olist-airflow airflow dags list-import-errors --local
docker exec olist-airflow airflow tasks list olist_m6_ads_full
.\.venv-serving\Scripts\python.exe -m unittest discover -s tests/pipeline -p "test_publish_recovery.py" -v
```

**7.5 配置与版本**

```powershell
docker compose config --quiet
git status --short
git log -4 --oneline
```

> 这些命令将显示执行时的实时状态；上文的具体数值是2026-11-03的历史基准，不保证未来继续相同。

## 8. 证据目录与版本追溯

| Git提交 | 已知含义 |
|---|---|
| `9f231c9` | M5批次管理、质量门禁、事务发布及故障恢复代码 |
| `186df03` | M6 Airflow全链路ADS发布 |
| `dc6b579` | M7批次来源与只读异常检测 |
| `a818d68` | M7发布恢复4项Mock单元测试 |
| `deb988b` | M7 PostgreSQL与Airflow共享Docker网络持久化配置 |

**建议新增的可审计材料：** `docs/evidence/` 保存经脱敏的测试命令和输出、Airflow成功运行截图、Manifest样例、数据库四表COUNT及一致性SQL输出；不提交密码、Airflow私有配置、真实连接密钥或巨大原始数据集。

## 9. 已知局限与后续行动

1. **真实事务故障注入缺少本轮独立可审计日志。** 应在独立测试库验证`COPY`失败回滚和提交成功但本地状态更新失败的恢复；不在当前服务库实验。
2. **并发竞争与批次新旧顺序是两个不同问题。** 建议锁可串行化遵守协议的事务，但待补并发压测与业务时间版本防回退规则。
3. **数据质量规则清单尚需从代码逐条提取。** 后续应将非空、唯一键、关联完整性、销售口径、异常行策略等按“规则—SQL/代码—阈值—结果”独立列出，避免凭经验杜撰。
4. **新来源字段尚无完整九任务成功Run的回归记录。** 已验证真实`prepare_batch`单任务写入来源；以前的完整九任务成功运行发生在该字段加入之前。
5. **部署范围有限。** 本项目为Windows + Docker单机Spark工程，不是PB级集群，也尚未证明线上SLA或实际高并发服务能力。

## 10. 可用于面试的事实表述

> 基于Olist公开电商数据构建Spark离线数仓，完成ODS/DWD/DWS/ADS分层与四张ADS指标表；在Airflow中编排九任务批处理链路，使用Manifest记录批次质量、状态和调度来源；通过PostgreSQL事务级建议锁、原子替换及发布日志实现受控发布和幂等恢复，并以4项Mock自动化测试覆盖异常状态拒绝、历史版本保护和提交后状态修复。项目规模为公开数据集的本地工程实践，重点验证数据正确性与发布可靠性。

**不能替代为：** “搭建PB级分布式数仓”“解决生产级并发发布”“实现零故障自动恢复”“经过大规模压测”；这些没有相应证据支持。

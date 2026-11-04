# Olist 数仓架构与发布链路

## A. 逻辑架构

```mermaid
flowchart LR
  CSV[(Olist CSV)] --> ODS[ODS Parquet]
  ODS --> DWD[DWD 明细层]
  DWD --> DWS[DWS 订单汇总]
  DWS --> ADS[批次 ADS Parquet]
  AIR[Airflow DAG] --> RUN[Spark Runner]
  RUN --> ADS
  AIR --> MF[Manifest 状态与来源]
  ADS --> QC[质量校验 / 发布就绪]
  MF --> QC
  QC --> TX[PostgreSQL 事务发布]
  TX --> SERVE[(四张 ADS 服务表)]
  TX --> LOG[(发布日志)]
  TX --> PTR[(当前批次指针)]
  CHECK[只读批次扫描器] --> MF
  CHECK -.按需查询.-> AIR
```

## B. 九任务编排

```mermaid
flowchart TD
    A[prepare_batch] --> B[build_daily_sales]
    B --> C[build_customer_ads]
    C --> D[build_category_sales]
    D --> E[record_all_ads]
    E --> F[validate_batch]
    F --> G[check_publish_ready]
    G --> H[publish_batch]
    H --> I[verify_publication]
```

上述为项目中已经验证的任务依赖顺序，**不是**并发执行图。

## C. 发布事务与恢复路径

```mermaid
flowchart TD
  START[VALIDATED 或 PUBLISHED Manifest] --> LOCK[PostgreSQL 事务建议锁]
  LOCK --> Q{发布日志中已有该 batch_id?}
  Q -- 否 --> V{Manifest为VALIDATED?}
  V -- 否 --> REJECT[拒绝发布]
  V -- 是 --> COPY[同一事务: TRUNCATE / COPY 四表]
  COPY --> METRIC[指标检查]
  METRIC --> WRITE[写发布日志 + 更新当前批次指针]
  WRITE --> COMMIT[提交事务]
  Q -- 是 --> CUR{仍是当前批次?}
  CUR -- 否 --> REJECT
  CUR -- 是 --> COMMIT
  COMMIT --> FIX{Manifest仍为VALIDATED?}
  FIX -- 是 --> UPDATE[更新Manifest为PUBLISHED]
  FIX -- 否 --> END[完成]
  UPDATE --> END
```

**注意：** 本图是对现有 `publish_batch.py` 逻辑的概括。事务锁只协调使用同一锁键的发布路径；数据库提交后到Manifest更新前仍存在短暂的跨系统状态不一致窗口，依靠后续重试恢复。

## D. Docker网络

```mermaid
flowchart LR
  subgraph A[olist-airflow_default]
    AF[olist-airflow]
    SR[olist-spark-runner]
    PG[olist-postgres]
  end
  subgraph B[sparkecommerce_default]
    PG2[PostgreSQL 服务的另一网络连接]
  end
  PG --- PG2
```

根目录 `compose.yaml` 的 PostgreSQL 服务使用 `default` + `airflow_shared` 网络；顶层 `airflow_shared` 以 `external: true` 指向 `olist-airflow_default`。网络由 Airflow Compose 创建，因此部署顺序需要确保网络先存在。PostgreSQL的数据由独立命名卷 `sparkecommerce_postgres_data` 保存。

## E. 验收证据索引

| 证据 | 已观察结果 |
|---|---|
| `airflow tasks list olist_m6_ads_full` | 九个任务均存在 |
| 完整 M6 DAG 运行 | 九任务完成，历史执行记录 `success` |
| `ads_current_batch` | `m6_full_cb29be2ec233b189`（2026-11-03验证） |
| `ads_publication_log` | 四条记录（2026-11-03验证） |
| `unittest discover` | 四项 Mock 测试通过 |
| `prepare_batch`任务测试 | 新建含实际 Airflow 临时 Run ID 的 Manifest |
| Docker网络连通 | `POSTGRES NETWORK: PASS` |

本文件不包含源数据文件或用户本地密钥。

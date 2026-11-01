# Olist Spark 电商离线数仓（M1 starter）

这是**第一个可运行里程碑**，不是已完成的端到端数仓。项目目标见 [`docs/PROJECT_SPEC.md`](docs/PROJECT_SPEC.md)。

## 前置条件
Windows PowerShell、Docker Desktop、已下载并解压的 Olist 9 张官方 CSV。使用已有的 `spark:4.2.0-python3` 镜像。

将本包内容合并到 `D:\SparkEcommerce`；**不要删除已有项目文件**。请将原始 Olist CSV 放到 `data/raw/olist`，文件名须与官方数据集一致。`data/raw/olist/.gitkeep` 仅用于保留空目录。

## 运行 M1
在 PowerShell：

```powershell
cd D:\SparkEcommerce
docker run --rm --name olist-m1 --memory 4g --cpus 2 `
  --mount "type=bind,source=$((Get-Location).Path),target=/workspace" `
  --entrypoint /opt/spark/bin/spark-submit `
  spark:4.2.0-python3 `
  --master "local[2]" `
  /workspace/src/ingestion/run_ods.py
```

应见 `M1 STATUS: PASS`（前提是源文件真实、数据符合检查）。输出：`data/ods/<table>/` 的 9 份 Parquet 数据集，以及 `reports/m1_ods_quality.json`。如文件缺失会在写入前报错。

## 验收与重跑
1. 查看 JSON 报告中 `tables.orders.rows` 是否为 `99441`（对于标准 Olist 数据）；其他表以报告为准。
2. 再执行一次命令，比较 9 个表的行数是否不变；Parquet 的物理文件名与大小可能变化，这不是行数错误。
3. 将报告里的问题记录到 `docs/`，不可伪造 PASS。

## 注意
M1 使用 `mode("overwrite")` **覆盖** `data/ods` 内同名表的旧数据。不要把重要的手工修改放在 `data/ods`。M1 不会删除原始 CSV。中途作业失败时，可能只写好部分 ODS 表；后续会增加发布原子性。M1 不做外键/多表对账与业务级金额核算。

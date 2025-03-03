# **Kestra Flow: 06_gcp_taxi_scheduled**
本 Flow 主要完成：
1. **下载 NYC Taxi 数据**
2. **上传数据至 Google Cloud Storage (GCS)**
3. **在 BigQuery 中创建外部表**
4. **去重合并数据**
5. **按计划调度**

---

## **1. Inputs & Variables**
### **1.1 Inputs（用户输入）**
```yaml
inputs:
  - id: taxi
    type: SELECT
    displayName: Select taxi type
    values: [yellow, green]
    defaults: green
```
| **Input**  | **作用** |
|------------|---------|
| `taxi`     | 选择 taxi 数据类型（yellow 或 green）|

---

### **1.2 Variables（变量定义）**
```yaml
variables:
  file: "{{inputs.taxi}}_tripdata_{{trigger.date | date('yyyy-MM')}}.csv"
  gcs_file: "gs://{{kv('GCP_BUCKET_NAME')}}/{{vars.file}}"
  table: "{{kv('GCP_DATASET')}}.{{inputs.taxi}}_tripdata_{{trigger.date | date('yyyy_MM')}}"
  data: "{{outputs.extract.outputFiles[inputs.taxi ~ '_tripdata_' ~ (trigger.date | date('yyyy-MM')) ~ '.csv']}}"
```

| **变量**  | **示例值** | **作用** |
|-----------|-----------|---------|
| `file`    | `yellow_tripdata_2019-01.csv` | 本地 CSV 文件名 |
| `gcs_file` | `gs://bucket/yellow_tripdata_2019-01.csv` | GCS 存储路径 |
| `table` | `gcp_dataset.yellow_tripdata_2019_01` | BigQuery 外部表名称 |
| `data` | `outputs.extract.outputFiles["yellow_tripdata_2019-01.csv"]` | 提取后的 CSV 数据文件 |

📌 **重点解析**
- `outputs.extract.outputFiles[...]` **指向 extract 任务的输出文件**，即 `yellow_tripdata_2019-01.csv`
- `gcs_file` 是数据上传到 GCS 后的存储路径
- `table` 定义 **BigQuery 外部表**，使用 `YYYY_MM` 命名

---

## **2. Tasks & Steps**
### **2.1 给任务打标签**
```yaml
- id: set_label
  type: io.kestra.plugin.core.execution.Labels
  labels:
    file: "{{render(vars.file)}}"
    taxi: "{{inputs.taxi}}"
```
📌 **作用**
- 方便后续追踪任务执行情况
- 例如：
  ```yaml
  file: yellow_tripdata_2019-01.csv
  taxi: yellow
  ```

---

### **2.2 下载数据**
```yaml
- id: extract
  type: io.kestra.plugin.scripts.shell.Commands
  outputFiles:
    - "*.csv"
  taskRunner:
    type: io.kestra.plugin.core.runner.Process
  commands:
    - wget -qO- https://github.com/DataTalksClub/nyc-tlc-data/releases/download/{{inputs.taxi}}/{{render(vars.file)}}.gz | gunzip > {{render(vars.file)}}
```
📌 **作用**
- 使用 `wget` **下载压缩数据**
- `gunzip` **解压缩 CSV 文件**
- `outputFiles: "*.csv"` **告诉 Kestra 任务的输出是 CSV**

📌 **示例**
```bash
wget -qO- https://github.com/DataTalksClub/nyc-tlc-data/releases/download/yellow/yellow_tripdata_2019-01.csv.gz | gunzip > yellow_tripdata_2019-01.csv
```
➡️ **最终输出文件**：`yellow_tripdata_2019-01.csv`

---

### **2.3 上传数据到 GCS**
```yaml
- id: upload_to_gcs
  type: io.kestra.plugin.gcp.gcs.Upload
  from: "{{render(vars.data)}}"
  to: "{{render(vars.gcs_file)}}"
```
📌 **作用**
- 把 `data` 变量中的文件上传至 **Google Cloud Storage (GCS)**
- `from`: 本地存储的 `yellow_tripdata_2019-01.csv`
- `to`: `gs://bucket/yellow_tripdata_2019-01.csv`

---

### **2.4 在 BigQuery 中创建外部表**
```yaml
- id: bq_yellow_table_ext
  type: io.kestra.plugin.gcp.bigquery.Query
  sql: |
    CREATE OR REPLACE EXTERNAL TABLE `{{kv('GCP_PROJECT_ID')}}.{{render(vars.table)}}_ext`
    OPTIONS (
        format = 'CSV',
        uris = ['{{render(vars.gcs_file)}}'],
        skip_leading_rows = 1,
        ignore_unknown_values = TRUE
    );
```
📌 **作用**
- **创建 BigQuery 外部表**（`yellow_tripdata_2019_01_ext`）
- **数据源**：`gs://bucket/yellow_tripdata_2019-01.csv`
- **文件格式**：CSV
- **忽略第一行标题**
- **不解析未知字段**

📌 **示例**
```sql
CREATE OR REPLACE EXTERNAL TABLE `gcp_project_id.gcp_dataset.yellow_tripdata_2019_01_ext`
OPTIONS (
    format = 'CSV',
    uris = ['gs://bucket/yellow_tripdata_2019-01.csv'],
    skip_leading_rows = 1,
    ignore_unknown_values = TRUE
);
```

---

### **2.5 生成唯一 ID 并去重**
```yaml
- id: bq_yellow_table_tmp
  type: io.kestra.plugin.gcp.bigquery.Query
  sql: |
    CREATE OR REPLACE TABLE `{{kv('GCP_PROJECT_ID')}}.{{render(vars.table)}}`
    AS
    SELECT
      MD5(CONCAT(
        COALESCE(CAST(VendorID AS STRING), ""),
        COALESCE(CAST(tpep_pickup_datetime AS STRING), ""),
        COALESCE(CAST(tpep_dropoff_datetime AS STRING), ""),
        COALESCE(CAST(PULocationID AS STRING), ""),
        COALESCE(CAST(DOLocationID AS STRING), "")
      )) AS unique_row_id,
      "{{render(vars.file)}}" AS filename,
      *
    FROM `{{kv('GCP_PROJECT_ID')}}.{{render(vars.table)}}_ext`;
```
📌 **作用**
- **生成 `unique_row_id`**，避免重复数据
- **添加 `filename` 字段**，记录数据来源
- **从外部表加载数据**

---

### **2.6 清理临时文件**
```yaml
- id: purge_files
  type: io.kestra.plugin.core.storage.PurgeCurrentExecutionFiles
  description: To avoid cluttering your storage, we will remove the downloaded files
```
📌 **作用**
- **删除 Kestra 任务执行时的本地 CSV 文件**
- **不影响 GCS 和 BigQuery**

---

## **3. Flow 调度**
```yaml
triggers:
  - id: green_schedule
    type: io.kestra.plugin.core.trigger.Schedule
    cron: "0 9 1 * *"
    inputs:
      taxi: green

  - id: yellow_schedule
    type: io.kestra.plugin.core.trigger.Schedule
    cron: "0 10 1 * *"
    inputs:
      taxi: yellow
```
📌 **作用**
- **定时触发 Flow**
- **每月 1 日 09:00 处理 Green Taxi 数据**
- **每月 1 日 10:00 处理 Yellow Taxi 数据**

---

## **4. 总结**
| **任务**  | **作用** |
|------------|---------|
| `extract` | 下载 & 解压数据 |
| `upload_to_gcs` | 上传数据到 GCS |
| `bq_yellow_table_ext` | 在 BigQuery 创建外部表 |
| `bq_yellow_table_tmp` | 生成 `unique_row_id`，去重并添加 `filename` |
| `purge_files` | 清理本地 CSV |

➡️  **最终数据存储在**：
1. **GCS** (`gs://bucket/yellow_tripdata_2019-01.csv`)
2. **BigQuery 外部表** (`gcp_dataset.yellow_tripdata_2019_01_ext`)
3. **BigQuery 去重表** (`gcp_dataset.yellow_tripdata_2019_01`)

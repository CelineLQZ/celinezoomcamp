# **Kestra Flow: 06_gcp_taxi_scheduled**

## **Overview**
This Kestra Flow automates the process of extracting, storing, and managing NYC taxi trip data. It downloads taxi trip CSV files, uploads them to Google Cloud Storage (GCS), creates external tables in BigQuery, and merges new data while avoiding duplicates.

## **Flow Steps**
### **1. Inputs & Variables**
#### **Inputs**
- `taxi`: Select taxi type (`yellow`, `green`)

#### **Variables**
- `file`: Taxi data file name
  ```yaml
  file: "{{inputs.taxi}}_tripdata_{{trigger.date | date('yyyy-MM')}}.csv"
  ```
  **Example**: `yellow_tripdata_2019-01.csv`

- `gcs_file`: GCS bucket storage path
  ```yaml
  gcs_file: "gs://{{kv('GCP_BUCKET_NAME')}}/{{vars.file}}"
  ```
  **Example**: `gs://bucket/yellow_tripdata_2019-01.csv`

- `table`: BigQuery dataset table name
  ```yaml
  table: "{{kv('GCP_DATASET')}}.{{inputs.taxi}}_tripdata_{{trigger.date | date('yyyy_MM')}}"
  ```
  **Example**: `gcp_dataset.yellow_tripdata_2019_01`

- `data`: Extracted file reference
  ```yaml
  data: "{{outputs.extract.outputFiles[inputs.taxi ~ '_tripdata_' ~ (trigger.date | date('yyyy-MM')) ~ '.csv']]}}"
  ```
  **Example**: `outputs.extract.outputFiles["yellow_tripdata_2019-01.csv"]`

### **2. Tasks & Steps**
#### **2.1 Labeling the Execution**
```yaml
- id: set_label
  type: io.kestra.plugin.core.execution.Labels
  labels:
    file: "{{render(vars.file)}}"
    taxi: "{{inputs.taxi}}"
```
**Purpose**: Assigns metadata labels for tracking execution.

#### **2.2 Data Extraction**
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
**Purpose**: Downloads taxi trip data and extracts it.

#### **2.3 Uploading Extracted Data to GCS**
```yaml
- id: upload_to_gcs
  type: io.kestra.plugin.gcp.gcs.Upload
  from: "{{render(vars.data)}}"
  to: "{{render(vars.gcs_file)}}"
```
**Purpose**: Uploads extracted data to Google Cloud Storage.

#### **2.4 BigQuery Table Management**
##### **Yellow Taxi Data Handling**
- **Create Main Table** (`yellow_tripdata`)
- **Create External Table** (`yellow_tripdata_YYYY_MM_ext`)
- **Create Temporary Table** (`yellow_tripdata_YYYY_MM`)
- **Merge Data into Main Table**
```yaml
- id: if_yellow_taxi
  type: io.kestra.plugin.core.flow.If
  condition: "{{inputs.taxi == 'yellow'}}"
  then:
    - id: bq_yellow_tripdata
      type: io.kestra.plugin.gcp.bigquery.Query
      sql: |
        CREATE TABLE IF NOT EXISTS `{{kv('GCP_PROJECT_ID')}}.{{kv('GCP_DATASET')}}.yellow_tripdata` (...)
```

##### **Green Taxi Data Handling**
- **Create External Table** (`green_tripdata_YYYY_MM_ext`)
- **Create Temporary Table** (`green_tripdata_YYYY_MM`)
- **Merge Data into Main Table**
```yaml
- id: if_green_taxi
  type: io.kestra.plugin.core.flow.If
  condition: "{{inputs.taxi == 'green'}}"
  then:
    - id: bq_green_tripdata
      type: io.kestra.plugin.gcp.bigquery.Query
      sql: |
        CREATE TABLE IF NOT EXISTS `{{kv('GCP_PROJECT_ID')}}.{{kv('GCP_DATASET')}}.green_tripdata` (...)
```

#### **2.5 File Cleanup**
```yaml
- id: purge_files
  type: io.kestra.plugin.core.storage.PurgeCurrentExecutionFiles
  description: "To avoid cluttering your storage, we will remove the downloaded files."
```
**Purpose**: Removes local CSV files to free up storage.

#### **2.6 Scheduling Flow Execution**
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
**Purpose**: Automates monthly data ingestion.

---

## **Flow Components Summary**
| Component                | Description                                      |
|--------------------------|--------------------------------------------------|
| `set_label`              | Assigns labels to execution                    |
| `extract`                | Downloads and extracts CSV files                |
| `upload_to_gcs`         | Uploads extracted data to GCS                   |
| `if_yellow_taxi`        | Handles Yellow taxi data processing             |
| `if_green_taxi`         | Handles Green taxi data processing              |
| `purge_files`           | Cleans up extracted files after processing      |
| `green_schedule`        | Triggers Green taxi data ingestion (Monthly)    |
| `yellow_schedule`       | Triggers Yellow taxi data ingestion (Monthly)   |

---

## **File & Table Summary**
| Name                         | Location                            | Final State         |
|------------------------------|-------------------------------------|---------------------|
| `file` (CSV)                 | Local storage                      | Deleted after use  |
| `gcs_file` (CSV in GCS)      | `gs://bucket/filename.csv`         | Stored in GCS      |
| `table_ext` (External Table) | `gcp_dataset.yellow_tripdata_YYYY_MM_ext` | Kept as reference |
| `table_tmp` (Temp Table)     | `gcp_dataset.yellow_tripdata_YYYY_MM`     | Merged & deleted  |
| `table` (Merged Table)       | `gcp_dataset.yellow_tripdata`       | Final data storage |

This structured summary provides a **concise**, **organized**, and **easy-to-reference** overview of the Kestra Flow for handling NYC taxi data. 🚀



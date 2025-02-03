# **week2 homework answer**

This guide outlines how to execute Kestra workflows and query data in **GCP Buckets** and **PostgreSQL (pgAdmin)**.

To answer the questions below, execute the following Kestra workflows in order:
1. **Run `02_postgres_taxi_scheduled.yaml`** → Backfill data for:
   - **Yellow Taxi:** From **January 1, 2019** to **December 31, 2021**  
   - **Green Taxi:** From **January 1, 2019** to **December 31, 2021**

2. **Run `04_gcp_kv.yaml`** → Sets GCP-related variables.

3. **Run `05_gcp_setup.yaml`** → Creates GCS Buckets and BigQuery datasets.

4. **Run `06_gcp_taxi.yaml`** with parameters:
   - `taxi=yellow`
   - `year=2020`
   - `month=12`


---

## **Questions and How to Answer Them**

### **Question 1: File Size of `yellow_tripdata_2020-12.csv`**
- After executing **Kestra workflows**, navigate to **Google Cloud Storage (GCS) Buckets**.
- Locate `yellow_tripdata_2020-12.csv` and check its **file size**.

---

### **Question 2: Rendered Value in Kestra**
- Refer to **`02_postgres_taxi_scheduled.yaml`** to determine how variables are rendered in Kestra.

---

### **Question 3: Number of Rows (Yellow Taxi, 2020)**
- Open **pgAdmin** and run the following SQL query to count rows for **yellow taxi trips in 2020**:
```sql
SELECT COUNT(*) AS total_rows
FROM public.yellow_tripdata
WHERE filename LIKE 'yellow_tripdata_2020-%';
```

---

### **Question 4: Number of Rows (Green Taxi, 2020)**
- Open **pgAdmin** and run the following SQL query to count rows for **green taxi trips in 2020**:
```sql
SELECT COUNT(*) AS total_rows
FROM public.green_tripdata
WHERE filename LIKE 'green_tripdata_2020-%';
```

---

### **Question 5: Number of Rows (Yellow Taxi, March 2021)**
- Open **pgAdmin** and run the following SQL query to count rows for **yellow taxi trips in March 2021**:
```sql
SELECT COUNT(*) AS total_rows
FROM public.yellow_tripdata
WHERE filename = 'yellow_tripdata_2021-03.csv';
```


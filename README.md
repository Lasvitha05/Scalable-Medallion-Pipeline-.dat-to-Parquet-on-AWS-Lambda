# Scalable-Medallion-Pipeline-.dat-to-Parquet-on-AWS-Lambda

##  Project Overview
ProcureFlow is an end-to-end data engineering pipeline designed to handle the ingestion, transformation, and optimization of high-volume retail transaction data. This project simulates a real-world enterprise scenario where legacy **`.dat`** files are converted into analytics-ready **Snappy-compressed `.Parquet`** files using a **Serverless Medallion Architecture**.

### Key Highlights:
* **Scale:** Successfully processed **10GB+** of synthetic retail data.
* **Architecture:** Implemented a three-tier "Medallion" structure (Bronze, Silver, Gold).
* **Tech Stack:** Python, Docker, AWS S3, AWS Lambda, AWS Athena, and IAM.
* **Optimization:** Reduced storage footprint by **~80%** using columnar storage formats.

---

## Architecture
The pipeline follows a modular, event-driven design:
1. **Bronze (Landing):** Raw legacy `.dat` files generated via a local orchestrator.
2. **Silver (Raw):** Files converted to Parquet format to enable schema enforcement.
3. **Gold (Clean):** Data processed by a **Dockerized AWS Lambda** for deduplication, null handling, and type casting.
4. **Analytics:** Fully queryable via **AWS Athena** for business intelligence.

<img width="2212" height="958" alt="image" src="https://github.com/user-attachments/assets/dfce507f-252b-45d1-9136-225bc29adbfe" />


---

##  Challenges & Engineering Solutions

### 1. Solving the "Memory Wall" (OutOfMemory Error)
#### **PROBLEM:** During the 10GB stress test, the Lambda function crashed repeatedly. I discovered that a 10MB compressed Parquet file expands significantly in RAM when loaded into a Pandas DataFrame, exceeding the initial 256MB limit.
#### **SOLUTION:** I vertically scaled the infrastructure by increasing Lambda memory to **1024MB** and optimized the memory footprint during the transformation phase.

### 2. Strategic Micro-Batching
#### **PROBLEM:** Uploading massive files in a single stream caused network timeouts and local system instability.
#### **SOLUTION:** I built a **Cycling Orchestrator** that processed data in stable, iterative batches. This ensured the pipeline remained resilient and could run autonomously for extended periods.

---

## Setup & Execution

### 1. Containerization
The **Cleaner Service** is packaged as a Docker container to ensure environment parity and manage heavy data libraries.
* **Base Image:** Python 3.9-slim
* **Dependencies:** `pandas`, `pyarrow`, `boto3`, and `s3fs`.
* **Deployment:** The image is pushed to **AWS ECR** and deployed as a Lambda Function.

### 2. Infrastructure & Automation
To achieve a fully serverless, event-driven flow, the following AWS configurations are required:
* **S3 Event Triggers:** Configured on the `raw_data/` prefix to invoke the Lambda function automatically upon `.parquet` file creation.
* **IAM Roles:** A custom execution role with `s3:GetObject` and `s3:PutObject` permissions.
* **Lambda Tuning:** Function memory set to **1024MB** to accommodate in-memory data uncompression.

### 3. Running the Pipeline
Ensure your AWS credentials are configured locally, then execute the master orchestrator:
```bash
python3 orchestrator.py
```

---


## Analytics Examples (AWS Athena)
The final Gold layer is optimized for SQL.
#### Data Availability in Gold Layer:
<img width="1420" height="624" alt="image" src="https://github.com/user-attachments/assets/c6178bf6-3b7a-44e9-a624-86ea1d3410e3" />

<img width="2840" height="1248" alt="image" src="https://github.com/user-attachments/assets/254a8579-a919-4123-a959-8008bd1edc72" />


#### Example query for revenue analysis:
```sql
SELECT 
    product_category, 
    SUM(total_amount) as total_revenue 
FROM procureflow_db.cleaned_transactions
GROUP BY 1 ORDER BY 2 DESC;
```
<img width="1420" height="675" alt="image" src="https://github.com/user-attachments/assets/a4121c38-532c-45bf-825d-d12a1d3cc9cf" />

# CDR Analytics Pipeline

This project is a batch analytics pipeline for telecom Call Detail Records (CDRs).

It uses:
- Apache Spark for distributed processing
- Hadoop HDFS for storage
- Apache Airflow for orchestration
- Docker Compose to run the full stack locally

## What the project does

The pipeline reads a large CSV file of telecom records and runs analytics jobs on it.

Each run:
- reads the input data
- processes it with Spark
- writes the result to HDFS
- writes a `_MANIFEST.json` file with run details

## Dataset

The data is generated automatically from [data/generate.py](/abs/path/c:/Users/user/Desktop/Dev/cdr-analytics-pipeline/data/generate.py).

Important facts:
- It creates `2,000,000` records
- It includes a high-volume caller named `WHALE_CALLER_001`
- It includes a small number of abnormal long-duration calls

This helps demonstrate both analytics logic and data skew handling.

## Jobs

### `top_callers`
Finds the top 100 callers by total spend.

Special point:
- uses salting to reduce data skew from the whale caller

### `tower_heatmap`
Counts calls by `tower_id` and hour of day.

Special point:
- helps show tower usage patterns

### `anomalous_calls`
Detects unusual calls based on caller-level duration behavior.

Special point:
- uses a custom partitioner so records for the same caller stay together

### `revenue_recon`
Calculates total revenue from the full dataset.

Special point:
- simple business validation metric

## Architecture

Flow:
1. Data is generated into `data/cdr_data.csv`
2. Airflow triggers a Spark job
3. Spark reads the CSV file
4. Spark writes results to HDFS
5. Output is stored by job name and run ID

## Project structure

- `data/` - generated input data and generator scripts
- `jobs/` - PySpark analytics jobs
- `dags/` - Airflow DAGs
- `output/` - mounted output directory used with HDFS
- `run_pipeline.sh` - Bash launcher
- `run_pipeline.ps1` - PowerShell launcher for Windows

## Prerequisites

- Docker Desktop
- At least 8 GB RAM available to Docker

## Start the project

```bash
docker compose up -d --build
```

Wait until all containers are healthy.

## Airflow login

Open:
- `http://localhost:8082`

Login with:
- username: `admin`
- password: `admin`

## Run a job

### On Windows PowerShell

```powershell
.\run_pipeline.ps1 top_callers
```

### On Bash

```bash
./run_pipeline.sh top_callers
```

Supported job names:
- `top_callers`
- `tower_heatmap`
- `anomalous_calls`
- `revenue_recon`

## Monitoring URLs

- Airflow: `http://localhost:8082`
- Spark Master UI: `http://localhost:8081`
- Hadoop Namenode UI: `http://localhost:9870`

## Check output

List all output folders:

```bash
docker exec namenode hdfs dfs -ls -R /output
```

View top callers output folders:

```bash
docker exec namenode hdfs dfs -ls /output/top_callers_by_spend
```

View a manifest:

```bash
docker exec namenode hdfs dfs -cat /output/top_callers_by_spend/<run_id>/_MANIFEST.json
```

## Notes

- The launcher automatically unpauses the DAG before triggering it.
- The Airflow DAG uses the Airflow run ID when no custom `run_id` is passed.
- First runs can take a little time because Spark needs to start the application and executor.

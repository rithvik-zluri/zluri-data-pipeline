# Zluri Data Pipelines

This repository contains the ETL pipelines for **Zluri**, including agents, roles, groups, budgets, cards, and transactions. Pipelines are orchestrated using **Prefect** and run on **Spark**.  

## Table of Contents

- [Prerequisites](#prerequisites)
- [Installation](#installation)
- [Project Structure](#project-structure)
- [Running Pipelines](#running-pipelines)
  - [Single Day Pipeline](#single-day-pipeline)
  - [Backfill Pipeline](#backfill-pipeline)
- [Optional Pipelines](#optional-pipelines)
- [Prefect UI](#prefect-ui)
- [Tests](#tests)
- [Notes](#notes)

---

## Prerequisites

Before starting, ensure you have:

- Python 3.10+
- Java 8+ (required by Spark)
- PostgreSQL or your preferred DB (configured in `src/db/connection.py`)
- Git
- Prefect 2.x (`pip install prefect`)

---

## Installation

Clone the repository:

```bash
git clone <REPO_URL>
cd <REPO_NAME>
```

Create a Python virtual eenvironment:

```
python -m venv venv
source venv/bin/activate 
```

### Create Database and User

Run the following commands in **PostgreSQL** to create the database and user with the necessary privileges.

```sql
-- Connect to PostgreSQL as a superuser (e.g., postgres)
psql -U postgres

-- Create a new database
CREATE DATABASE rithvik_zluri_pipeline_db;

-- Create a new user with password
CREATE USER rithvik_zluri_pipeline_user WITH PASSWORD 'rithvik_zluri_pipeline_pass';

-- Grant all privileges on the database to the user
GRANT ALL PRIVILEGES ON DATABASE rithvik_zluri_pipeline_db TO rithvik_zluri_pipeline_user;

-- Optional: exit psql
\q
```
## Database Connection Properties

You can use the following JDBC URL and properties to connect to the PostgreSQL database.

```python
# JDBC URL for the database
jdbc_url = "jdbc:postgresql://localhost:5432/rithvik_zluri_pipeline_db"

# Database connection properties
db_properties = {
    "user": "rithvik_zluri_pipeline_user",
    "password": "rithvik_zluri_pipeline_pass",
    "driver": "org.postgresql.Driver",
}
```
Usage Notes

Make sure PostgreSQL is running locally on port 5432.

The database rithvik_zluri_pipeline_db should exist, and the user rithvik_zluri_pipeline_user should have access.

### Using S3 Data Path

If your ETL reads data from S3, set the base path as an environment variable:
```
export DATA_BASE_PATH=s3a://zluri-data-assignment/assignment-jan-2026
```

Notes:

This should be run before running the Prefect flow.

All pipelines will read data from this path by default.

### Using Sample Data (Optional)

If you want to run the pipelines on sample/local data instead of S3, you can set the base path to a local folder containing CSV/Parquet files:
```
export DATA_BASE_PATH=sample_data
```

Replace path to sample_data with the path on your machine.

This is useful for testing and development without needing access to S3.

And before running the **prefect_flow.py** 

Run this file **src/db/migrations001_create_tables.sql** once using
```
psql -d rithvik_zluri_pipeline_db -f 001_create_tables.sql
```


# Running Pipelines

Pipelines are orchestrated using **Prefect**. You can run either a **single-day pipeline** or a **backfill** for multiple days.

---

## Single Day Pipeline

Run the ETL for a specific day:

```bash
python -m src.orchestration.prefect_flow. --day day1
```
- day1 corresponds to the logical day identifier used in your ETL.

- Mandatory pipelines: agents, groups, transactions.

- Optional pipelines: roles, agent_roles, budgets, cards. These run only if data is available.

## Backfill Pipeline

Run ETL for multiple days at once:

```bash
python src/orchestration/prefect_flow.py --backfill day1 day2 day3
```

## Optional Pipelines

Pipelines like `roles`, `agent_roles`, `budgets`, and `cards` are optional.

- If their data is missing, they will be skipped and their corresponding upserts will not run.  
- This ensures that **mandatory pipelines** (`agents`, `groups`, `transactions`) still run successfully.

---

## Prefect UI

Prefect provides a dashboard to monitor pipeline runs.

Start the Prefect server:

```bash
prefect server start
```
In another terminal:

Set API URL
```
prefect config set PREFECT_API_URL=http://127.0.0.1:4200/api
```
And Run

```bash
python -m src.orchestration.prefect_flow. --day day1
```

Open the UI in your browser:

http://127.0.0.1:4200


In the Prefect UI, you can:

- View flow runs and task status

- Retry failed tasks

- See logs and outputs for each task

- Trigger new runs manually

### To visualize logs in the terminal:
```
prefect logs <FLOW_RUN_ID>
```
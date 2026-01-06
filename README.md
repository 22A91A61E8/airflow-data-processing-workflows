# airflow-data-processing-workflows

Complete data processing workflows using Apache Airflow and Docker with 5 DAGs demonstrating ETL pipelines, transformations, conditional logic, and error handling

## Overview

This project demonstrates a complete Apache Airflow setup with Docker, featuring 5 production-ready DAGs that showcase various data engineering patterns:

- CSV to PostgreSQL data ingestion
- Data transformation pipelines
- PostgreSQL to Parquet export
- Conditional workflow execution
- Notification workflows with error handling

## Prerequisites

- Docker and Docker Compose installed
- Python 3.8+ (for local development)
- Git
- At least 4GB RAM available for Docker

## Project Structure

```
airflow-data-processing-workflows/
├── dags/
│   ├── dag1_csv_to_postgres.py
│   ├── dag2_data_transformation.py
│   ├── dag3_postgres_to_parquet.py
│   ├── dag4_conditional_workflow.py
│   └── dag5_notification_workflow.py
├── tests/
│   ├── test_dag1.py
│   ├── test_dag2.py
│   └── test_utils.py
├── data/
│   └── input.csv
├── output/
├── plugins/
├── logs/
├── docker-compose.yml
├── requirements.txt
└── README.md
```

## Setup Instructions

### 1. Clone the Repository

```bash
git clone https://github.com/22A91A61E8/airflow-data-processing-workflows.git
cd airflow-data-processing-workflows
```

### 2. Initialize Airflow Database

```bash
docker-compose up airflow-init
```

### 3. Start Airflow Services

```bash
docker-compose up -d
```

This will start:
- Airflow Webserver (port 8080)
- Airflow Scheduler
- PostgreSQL Database (port 5432)

### 4. Access Airflow UI

Open your browser and navigate to: `http://localhost:8080`

**Default Credentials:**
- Username: `airflow`
- Password: `airflow`

### 5. Verify DAGs

All 5 DAGs should be visible in the Airflow UI:
- csv_to_postgres_ingestion
- data_transformation_pipeline
- postgres_to_parquet_export
- conditional_workflow_pipeline
- notification_workflow

## DAG Execution Guide

### DAG 1: CSV to PostgreSQL Ingestion
**DAG ID:** `csv_to_postgres_ingestion`
**Schedule:** Daily (@daily)

**Purpose:** Reads employee data from CSV and loads it into PostgreSQL

**Tasks:**
1. `create_table` - Creates employees table if not exists
2. `load_csv_to_postgres` - Loads data from data/input.csv

**Manual Trigger:**
```bash
docker-compose exec airflow-webserver airflow dags trigger csv_to_postgres_ingestion
```

### DAG 2: Data Transformation Pipeline
**DAG ID:** `data_transformation_pipeline`
**Schedule:** Daily (@daily)

**Purpose:** Applies transformations to employee data

**Tasks:**
1. `extract_data` - Extracts data from PostgreSQL
2. `transform_data` - Applies business logic transformations
3. `load_transformed_data` - Loads transformed data back to PostgreSQL

**Manual Trigger:**
```bash
docker-compose exec airflow-webserver airflow dags trigger data_transformation_pipeline
```

### DAG 3: PostgreSQL to Parquet Export
**DAG ID:** `postgres_to_parquet_export`
**Schedule:** Weekly (@weekly)

**Purpose:** Exports PostgreSQL data to Parquet format for analytics

**Tasks:**
1. `export_to_parquet` - Exports employees table to output/employees.parquet
2. `verify_export` - Verifies the export was successful

**Manual Trigger:**
```bash
docker-compose exec airflow-webserver airflow dags trigger postgres_to_parquet_export
```

### DAG 4: Conditional Workflow Pipeline
**DAG ID:** `conditional_workflow_pipeline`
**Schedule:** Daily (@daily)

**Purpose:** Demonstrates conditional branching based on data quality checks

**Tasks:**
1. `check_data_quality` - Checks if data meets quality thresholds
2. `process_good_data` - Processes data if quality check passes
3. `handle_bad_data` - Handles data if quality check fails
4. `send_notification` - Sends notification about processing result

**Manual Trigger:**
```bash
docker-compose exec airflow-webserver airflow dags trigger conditional_workflow_pipeline
```

### DAG 5: Notification Workflow
**DAG ID:** `notification_workflow`
**Schedule:** Daily (@daily)

**Purpose:** Demonstrates error handling and notification patterns

**Tasks:**
1. `start_task` - Initializes workflow
2. `risky_task` - Simulates a task that might fail
3. `success_notification` - Sends success notification
4. `failure_notification` - Sends failure notification (trigger rule: one_failed)

**Manual Trigger:**
```bash
docker-compose exec airflow-webserver airflow dags trigger notification_workflow
```

## Running Tests

### Install Test Dependencies

```bash
pip install -r requirements.txt
```

### Run All Tests

```bash
pytest tests/ -v
```

### Run Specific Test Files

```bash
# Test DAG 1
pytest tests/test_dag1.py -v

# Test DAG 2
pytest tests/test_dag2.py -v

# Test utilities
pytest tests/test_utils.py -v
```

## Troubleshooting

### Issue: DAGs not appearing in UI

**Solution:**
1. Check if DAG files are in the correct directory:
   ```bash
   ls -la dags/
   ```
2. Check scheduler logs:
   ```bash
   docker-compose logs airflow-scheduler
   ```
3. Verify DAG syntax:
   ```bash
   docker-compose exec airflow-webserver python /opt/airflow/dags/dag1_csv_to_postgres.py
   ```

### Issue: Database connection failed

**Solution:**
1. Verify PostgreSQL is running:
   ```bash
   docker-compose ps
   ```
2. Check connection string in docker-compose.yml
3. Restart services:
   ```bash
   docker-compose down
   docker-compose up -d
   ```

### Issue: Permission denied on directories

**Solution:**
Ensure proper permissions:
```bash
chmod -R 777 logs/ output/ plugins/
```

### Issue: Tasks failing with import errors

**Solution:**
1. Check requirements.txt includes all necessary packages
2. Rebuild containers:
   ```bash
   docker-compose down
   docker-compose build --no-cache
   docker-compose up -d
   ```

### Issue: Out of memory errors

**Solution:**
1. Increase Docker memory allocation (Settings > Resources)
2. Reduce number of worker processes in docker-compose.yml
3. Monitor resource usage:
   ```bash
   docker stats
   ```

### Issue: CSV file not found

**Solution:**
Verify input file exists:
```bash
ls -la data/input.csv
```

If missing, ensure data/input.csv contains employee data with columns:
- id, name, age, city, salary, join_date

## Monitoring and Logs

### View Service Logs

```bash
# All services
docker-compose logs -f

# Specific service
docker-compose logs -f airflow-scheduler
docker-compose logs -f airflow-webserver
docker-compose logs -f postgres
```

### Access Task Logs

Task logs are available in:
- Airflow UI: Click on task > View Log
- File system: `logs/` directory

## Stopping and Cleaning Up

### Stop Services

```bash
docker-compose down
```

### Remove All Data (including database)

```bash
docker-compose down -v
```

### Clean Output Files

```bash
rm -rf output/* logs/*
```

## Configuration

### Environment Variables

Key environment variables in docker-compose.yml:

- `AIRFLOW__CORE__EXECUTOR`: LocalExecutor
- `AIRFLOW__DATABASE__SQL_ALCHEMY_CONN`: PostgreSQL connection string
- `AIRFLOW__CORE__DAGS_ARE_PAUSED_AT_CREATION`: true
- `AIRFLOW__CORE__LOAD_EXAMPLES`: false

### Modifying DAG Schedules

Edit the `schedule_interval` parameter in each DAG file:

```python
dag = DAG(
    'dag_id',
    schedule_interval='@daily',  # Change this
    ...
)
```

Common schedules:
- `@once`: Run once
- `@hourly`: Every hour
- `@daily`: Daily at midnight
- `@weekly`: Weekly on Sunday
- `@monthly`: First day of month
- `'0 */4 * * *'`: Every 4 hours (cron expression)

## Performance Optimization

1. **Parallelism**: Adjust `parallelism` and `max_active_runs_per_dag` in airflow.cfg
2. **Connection Pooling**: Configure pool size for PostgreSQL connections
3. **Task Concurrency**: Set `max_active_tasks_per_dag` appropriately
4. **Resource Allocation**: Allocate sufficient memory and CPU to Docker

## Security Best Practices

1. Change default Airflow credentials in production
2. Use environment variables for sensitive data
3. Implement RBAC for multi-user environments
4. Regularly update Airflow and dependencies
5. Restrict network access to Airflow ports

## Contributing

Feel free to submit issues and enhancement requests!

## License

This project is open source and available for educational purposes.

## Author

22A91A61E8

## Acknowledgments

- Apache Airflow Documentation
- Docker Documentation
- Python Community

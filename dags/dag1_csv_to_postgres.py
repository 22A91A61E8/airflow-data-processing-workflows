# dags/dag1_csv_to_postgres.py
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime
import pandas as pd
import logging

logger = logging.getLogger(__name__)

def create_employee_table():
    """
    Creates the raw_employee_data table in PostgreSQL if it doesn't exist.
    """
    try:
        hook = PostgresHook(postgres_conn_id='postgres_default')
        conn = hook.get_conn()
        cursor = conn.cursor()
        
        create_table_query = """
        CREATE TABLE IF NOT EXISTS raw_employee_data (
            id INTEGER PRIMARY KEY,
            name VARCHAR(255),
            age INTEGER,
            city VARCHAR(100),
            salary FLOAT,
            join_date DATE
        );
        """
        
        cursor.execute(create_table_query)
        conn.commit()
        cursor.close()
        conn.close()
        logger.info("Table raw_employee_data created successfully or already exists")
        
    except Exception as e:
        logger.error(f"Error creating table: {str(e)}")
        raise

def truncate_employee_table():
    """
    Truncates the raw_employee_data table to ensure idempotency.
    """
    try:
        hook = PostgresHook(postgres_conn_id='postgres_default')
        conn = hook.get_conn()
        cursor = conn.cursor()
        
        cursor.execute("TRUNCATE TABLE raw_employee_data;")
        conn.commit()
        cursor.close()
        conn.close()
        logger.info("Table raw_employee_data truncated successfully")
        
    except Exception as e:
        logger.error(f"Error truncating table: {str(e)}")
        raise

def load_csv_data():
    """
    Loads data from /opt/airflow/data/input.csv into raw_employee_data table.
    Returns the number of rows inserted.
    """
    try:
        # Read CSV file
        df = pd.read_csv('/opt/airflow/data/input.csv')
        rows_count = len(df)
        
        # Get connection using PostgresHook
        hook = PostgresHook(postgres_conn_id='postgres_default')
        conn = hook.get_conn()
        
        # Insert data using pandas to_sql
        df.to_sql('raw_employee_data', conn, if_exists='append', index=False)
        
        conn.close()
        logger.info(f"Successfully loaded {rows_count} rows into raw_employee_data")
        return rows_count
        
    except Exception as e:
        logger.error(f"Error loading CSV data: {str(e)}")
        raise

# Define DAG
dag = DAG(
    dag_id='csv_to_postgres_ingestion',
    start_date=datetime(2024, 1, 1),
    schedule_interval='@daily',
    catchup=False,
    description='Ingest employee data from CSV to PostgreSQL',
    tags=['etl', 'csv', 'postgres']
)

# Define tasks
create_table_task = PythonOperator(
    task_id='create_table_if_not_exists',
    python_callable=create_employee_table,
    dag=dag
)

truncate_table_task = PythonOperator(
    task_id='truncate_table',
    python_callable=truncate_employee_table,
    dag=dag
)

load_csv_task = PythonOperator(
    task_id='load_csv_to_postgres',
    python_callable=load_csv_data,
    dag=dag
)

# Set task dependencies
create_table_task >> truncate_table_task >> load_csv_task

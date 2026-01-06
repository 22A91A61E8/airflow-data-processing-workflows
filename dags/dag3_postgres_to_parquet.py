# dags/dag3_postgres_to_parquet.py
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime
import pandas as pd
import os
import logging

logger = logging.getLogger(__name__)

def check_table_exists(table_name='transformed_employee_data'):
    """
    Checks if the specified table exists and contains data.
    """
    try:
        hook = PostgresHook(postgres_conn_id='postgres_default')
        conn = hook.get_conn()
        cursor = conn.cursor()
        
        # Check if table exists
        cursor.execute(f"""
            SELECT EXISTS (
                SELECT FROM information_schema.tables 
                WHERE table_name = '{table_name}'
            );
        """)
        table_exists = cursor.fetchone()[0]
        
        if not table_exists:
            raise Exception(f"Table {table_name} does not exist")
        
        # Check if table has data
        cursor.execute(f"SELECT COUNT(*) FROM {table_name};")
        row_count = cursor.fetchone()[0]
        
        if row_count == 0:
            raise Exception(f"Table {table_name} exists but has no data")
        
        cursor.close()
        conn.close()
        logger.info(f"Table {table_name} exists and has {row_count} rows")
        return True
        
    except Exception as e:
        logger.error(f"Error checking table: {str(e)}")
        raise

def export_table_to_parquet(**context):
    """
    Exports a PostgreSQL table to Parquet format.
    """
    try:
        table_name = 'transformed_employee_data'
        execution_date = context['ds']  # YYYY-MM-DD format
        output_path = f"/opt/airflow/output/employee_data_{execution_date}.parquet"
        
        # Read table data
        hook = PostgresHook(postgres_conn_id='postgres_default')
        conn = hook.get_conn()
        df = pd.read_sql(f"SELECT * FROM {table_name}", conn)
        conn.close()
        
        row_count = len(df)
        
        # Write to Parquet with snappy compression
        df.to_parquet(output_path, engine='pyarrow', compression='snappy', index=False)
        
        # Get file size
        file_size = os.path.getsize(output_path)
        
        result = {
            'file_path': output_path,
            'row_count': row_count,
            'file_size_bytes': file_size
        }
        
        logger.info(f"Successfully exported {row_count} rows to {output_path} ({file_size} bytes)")
        return result
        
    except Exception as e:
        logger.error(f"Error exporting to Parquet: {str(e)}")
        raise

def validate_parquet(**context):
    """
    Validates that the Parquet file is readable and has correct schema.
    """
    try:
        execution_date = context['ds']
        file_path = f"/opt/airflow/output/employee_data_{execution_date}.parquet"
        
        # Read Parquet file
        df = pd.read_parquet(file_path)
        
        # Verify expected columns exist
        expected_columns = ['id', 'name', 'age', 'city', 'salary', 'join_date', 
                          'full_info', 'age_group', 'salary_category', 'year_joined']
        
        for col in expected_columns:
            if col not in df.columns:
                raise Exception(f"Expected column '{col}' not found in Parquet file")
        
        # Verify row count
        if len(df) == 0:
            raise Exception("Parquet file has no rows")
        
        logger.info(f"Parquet file validation successful: {len(df)} rows, {len(df.columns)} columns")
        return True
        
    except Exception as e:
        logger.error(f"Error validating Parquet file: {str(e)}")
        raise

# Define DAG
dag = DAG(
    dag_id='postgres_to_parquet_export',
    start_date=datetime(2024, 1, 1),
    schedule_interval='@weekly',
    catchup=False,
    description='Export transformed data to Parquet format',
    tags=['export', 'parquet', 'analytics']
)

# Define tasks
check_table_task = PythonOperator(
    task_id='check_source_table_exists',
    python_callable=check_table_exists,
    dag=dag
)

export_task = PythonOperator(
    task_id='export_to_parquet',
    python_callable=export_table_to_parquet,
    dag=dag
)

validate_task = PythonOperator(
    task_id='validate_parquet_file',
    python_callable=validate_parquet,
    dag=dag
)

# Set dependencies
check_table_task >> export_task >> validate_task

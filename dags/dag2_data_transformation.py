# dags/dag2_data_transformation.py
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime
import pandas as pd
import logging

logger = logging.getLogger(__name__)

def create_transformed_table():
    """
    Creates the transformed_employee_data table with additional columns.
    """
    try:
        hook = PostgresHook(postgres_conn_id='postgres_default')
        conn = hook.get_conn()
        cursor = conn.cursor()
        
        create_table_query = """
        CREATE TABLE IF NOT EXISTS transformed_employee_data (
            id INTEGER PRIMARY KEY,
            name VARCHAR(255),
            age INTEGER,
            city VARCHAR(100),
            salary FLOAT,
            join_date DATE,
            full_info VARCHAR(500),
            age_group VARCHAR(20),
            salary_category VARCHAR(20),
            year_joined INTEGER
        );
        """
        
        cursor.execute(create_table_query)
        conn.commit()
        cursor.close()
        conn.close()
        logger.info("Table transformed_employee_data created successfully or already exists")
        
    except Exception as e:
        logger.error(f"Error creating transformed table: {str(e)}")
        raise

def transform_data():
    """
    Reads from raw_employee_data, applies transformations, loads to transformed_employee_data.
    Returns dictionary with processing statistics.
    """
    try:
        hook = PostgresHook(postgres_conn_id='postgres_default')
        conn = hook.get_conn()
        
        # Read data from raw table
        df = pd.read_sql("SELECT * FROM raw_employee_data", conn)
        rows_processed = len(df)
        
        # Apply transformations
        df['full_info'] = df['name'] + ' - ' + df['city']
        
        # Age group categorization
        df['age_group'] = df['age'].apply(
            lambda x: 'Young' if x < 30 else ('Mid' if x < 50 else 'Senior')
        )
        
        # Salary categorization
        df['salary_category'] = df['salary'].apply(
            lambda x: 'Low' if x < 50000 else ('Medium' if x < 80000 else 'High')
        )
        
        # Extract year from join_date
        df['year_joined'] = pd.to_datetime(df['join_date']).dt.year
        
        # Truncate transformed table first
        cursor = conn.cursor()
        cursor.execute("TRUNCATE TABLE transformed_employee_data;")
        conn.commit()
        cursor.close()
        
        # Insert transformed data
        df.to_sql('transformed_employee_data', conn, if_exists='append', index=False)
        
        conn.close()
        
        result = {
            'rows_processed': rows_processed,
            'rows_inserted': rows_processed
        }
        
        logger.info(f"Data transformation completed: {result}")
        return result
        
    except Exception as e:
        logger.error(f"Error transforming data: {str(e)}")
        raise

# Define DAG
dag = DAG(
    dag_id='data_transformation_pipeline',
    start_date=datetime(2024, 1, 1),
    schedule_interval='@daily',
    catchup=False,
    description='Transform raw employee data with derived columns',
    tags=['etl', 'transformation', 'postgres']
)

# Define tasks
create_transformed_table_task = PythonOperator(
    task_id='create_transformed_table',
    python_callable=create_transformed_table,
    dag=dag
)

transform_and_load_task = PythonOperator(
    task_id='transform_and_load',
    python_callable=transform_data,
    dag=dag
)

# Set dependencies
create_transformed_table_task >> transform_and_load_task

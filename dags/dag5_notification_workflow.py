# dags/dag5_notification_workflow.py
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from datetime import datetime
import logging

logger = logging.getLogger(__name__)

def send_success_notification(context):
    """
    Callback function executed on task success.
    """
    task_instance = context['task_instance']
    result = {
        'notification_type': 'success',
        'status': 'sent',
        'message': f"Task {task_instance.task_id} succeeded",
        'timestamp': datetime.now().isoformat()
    }
    logger.info(f"Success notification: {result}")
    return result

def send_failure_notification(context):
    """
    Callback function executed on task failure.
    """
    task_instance = context['task_instance']
    exception = context.get('exception', 'Unknown error')
    result = {
        'notification_type': 'failure',
        'status': 'sent',
        'message': f"Task {task_instance.task_id} failed",
        'error': str(exception),
        'timestamp': datetime.now().isoformat()
    }
    logger.error(f"Failure notification: {result}")
    return result

def risky_operation(**context):
    """
    Simulates an operation that may fail based on execution date.
    Fails if the day of month is divisible by 5.
    """
    execution_date = context['execution_date']
    day_of_month = execution_date.day
    
    logger.info(f"Executing risky operation on day {day_of_month}")
    
    if day_of_month % 5 == 0:
        result = {
            'status': 'failed',
            'execution_date': execution_date.isoformat(),
            'success': False
        }
        logger.error(f"Risky operation failed: {result}")
        raise Exception(f"Operation failed on day {day_of_month} (divisible by 5)")
    
    result = {
        'status': 'success',
        'execution_date': execution_date.isoformat(),
        'success': True
    }
    logger.info(f"Risky operation succeeded: {result}")
    return result

def cleanup_task():
    """
    Cleanup task that always executes regardless of success/failure.
    """
    result = {
        'cleanup_status': 'completed',
        'timestamp': datetime.now().isoformat()
    }
    logger.info(f"Cleanup completed: {result}")
    return result

# Define DAG
dag = DAG(
    dag_id='notification_workflow',
    start_date=datetime(2024, 1, 1),
    schedule_interval='@daily',
    catchup=False,
    description='Workflow with error handling and notifications',
    tags=['notification', 'error-handling', 'callbacks']
)

# Start task
start_task = EmptyOperator(
    task_id='start_task',
    dag=dag
)

# Risky operation with callbacks
risky_task = PythonOperator(
    task_id='risky_operation',
    python_callable=risky_operation,
    on_success_callback=send_success_notification,
    on_failure_callback=send_failure_notification,
    dag=dag
)

# Success notification task
success_notification_task = EmptyOperator(
    task_id='success_notification',
    trigger_rule='all_success',
    dag=dag
)

# Failure notification task
failure_notification_task = EmptyOperator(
    task_id='failure_notification',
    trigger_rule='all_failed',
    dag=dag
)

# Always execute cleanup
always_execute_task = PythonOperator(
    task_id='always_execute',
    python_callable=cleanup_task,
    trigger_rule='all_done',
    dag=dag
)

# Set dependencies
start_task >> risky_task >> [success_notification_task, failure_notification_task]
[success_notification_task, failure_notification_task] >> always_execute_task

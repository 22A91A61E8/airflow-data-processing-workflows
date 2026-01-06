# dags/dag4_conditional_workflow.py
from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.empty import EmptyOperator
from datetime import datetime
import logging

logger = logging.getLogger(__name__)

def determine_branch(**context):
    """
    Determines which branch to execute based on day of week.
    Monday-Wednesday: weekday_processing
    Thursday-Friday: end_of_week_processing
    Saturday-Sunday: weekend_processing
    """
    execution_date = context['execution_date']
    day_of_week = execution_date.weekday()  # 0=Monday, 6=Sunday
    
    logger.info(f"Execution date: {execution_date}, Day of week: {day_of_week}")
    
    if day_of_week <= 2:  # Monday to Wednesday
        return 'weekday_processing'
    elif day_of_week <= 4:  # Thursday to Friday
        return 'end_of_week_processing'
    else:  # Saturday to Sunday
        return 'weekend_processing'

def weekday_process():
    """
    Processes weekday-specific logic.
    """
    result = {
        'day_name': datetime.now().strftime('%A'),
        'task_type': 'weekday',
        'record_count': 0
    }
    logger.info(f"Weekday processing completed: {result}")
    return result

def end_of_week_process():
    """
    Processes end-of-week specific logic.
    """
    result = {
        'day_name': datetime.now().strftime('%A'),
        'task_type': 'end_of_week',
        'weekly_summary': 'Week summary generated'
    }
    logger.info(f"End of week processing completed: {result}")
    return result

def weekend_process():
    """
    Processes weekend-specific logic.
    """
    result = {
        'day_name': datetime.now().strftime('%A'),
        'task_type': 'weekend',
        'cleanup_status': 'Weekend cleanup completed'
    }
    logger.info(f"Weekend processing completed: {result}")
    return result

# Define DAG
dag = DAG(
    dag_id='conditional_workflow_pipeline',
    start_date=datetime(2024, 1, 1),
    schedule_interval='@daily',
    catchup=False,
    description='Conditional workflow based on day of week',
    tags=['conditional', 'branching', 'workflow']
)

# Start task
start_task = EmptyOperator(
    task_id='start',
    dag=dag
)

# Branching task
branch_task = BranchPythonOperator(
    task_id='branch_by_day',
    python_callable=determine_branch,
    dag=dag
)

# Weekday branch tasks
weekday_task = PythonOperator(
    task_id='weekday_processing',
    python_callable=weekday_process,
    dag=dag
)

weekday_summary_task = EmptyOperator(
    task_id='weekday_summary',
    dag=dag
)

# End of week branch tasks
end_of_week_task = PythonOperator(
    task_id='end_of_week_processing',
    python_callable=end_of_week_process,
    dag=dag
)

end_of_week_report_task = EmptyOperator(
    task_id='end_of_week_report',
    dag=dag
)

# Weekend branch tasks
weekend_task = PythonOperator(
    task_id='weekend_processing',
    python_callable=weekend_process,
    dag=dag
)

weekend_cleanup_task = EmptyOperator(
    task_id='weekend_cleanup',
    dag=dag
)

# End task with trigger rule to run after any branch
end_task = EmptyOperator(
    task_id='end',
    trigger_rule='none_failed_min_one_success',
    dag=dag
)

# Set up dependencies
start_task >> branch_task

# Weekday branch
branch_task >> weekday_task >> weekday_summary_task >> end_task

# End of week branch
branch_task >> end_of_week_task >> end_of_week_report_task >> end_task

# Weekend branch
branch_task >> weekend_task >> weekend_cleanup_task >> end_task

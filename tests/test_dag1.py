# tests/test_dag1.py
from airflow.models import DagBag
import pytest

def test_dag1_loaded():
    """
    Verifies that DAG 1 can be loaded without errors.
    """
    dagbag = DagBag(dag_folder='dags/', include_examples=False)
    assert 'csv_to_postgres_ingestion' in dagbag.dags
    assert len(dagbag.import_errors) == 0

def test_dag1_structure():
    """
    Verifies DAG 1 has the correct number of tasks.
    """
    dagbag = DagBag(dag_folder='dags/', include_examples=False)
    dag = dagbag.dags['csv_to_postgres_ingestion']
    assert len(dag.tasks) == 3

def test_dag1_task_dependencies():
    """
    Verifies task dependencies are correctly configured.
    """
    dagbag = DagBag(dag_folder='dags/', include_examples=False)
    dag = dagbag.dags['csv_to_postgres_ingestion']
    
    create_task = dag.get_task('create_table_if_not_exists')
    truncate_task = dag.get_task('truncate_table')
    load_task = dag.get_task('load_csv_to_postgres')
    
    assert truncate_task in create_task.downstream_list
    assert load_task in truncate_task.downstream_list

def test_dag1_no_cycles():
    """
    Verifies DAG has no cycles.
    """
    dagbag = DagBag(dag_folder='dags/', include_examples=False)
    dag = dagbag.dags['csv_to_postgres_ingestion']
    dag.test_cycle()

def test_dag1_schedule():
    """
    Verifies the schedule interval is correct.
    """
    dagbag = DagBag(dag_folder='dags/', include_examples=False)
    dag = dagbag.dags['csv_to_postgres_ingestion']
    assert dag.schedule_interval == '@daily'

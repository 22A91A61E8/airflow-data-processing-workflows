# tests/test_dag2.py
from airflow.models import DagBag
import pytest

def test_dag2_loaded():
    dagbag = DagBag(dag_folder='dags/', include_examples=False)
    assert 'data_transformation_pipeline' in dagbag.dags
    assert len(dagbag.import_errors) == 0

def test_dag2_structure():
    dagbag = DagBag(dag_folder='dags/', include_examples=False)
    dag = dagbag.dags['data_transformation_pipeline']
    assert len(dag.tasks) == 2

def test_dag2_task_dependencies():
    dagbag = DagBag(dag_folder='dags/', include_examples=False)
    dag = dagbag.dags['data_transformation_pipeline']
    
    create_task = dag.get_task('create_transformed_table')
    transform_task = dag.get_task('transform_and_load')
    
    assert transform_task in create_task.downstream_list

def test_dag2_schedule():
    dagbag = DagBag(dag_folder='dags/', include_examples=False)
    dag = dagbag.dags['data_transformation_pipeline']
    assert dag.schedule_interval == '@daily'

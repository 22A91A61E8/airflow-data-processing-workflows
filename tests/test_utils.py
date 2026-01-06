# tests/test_utils.py
from airflow.models import DagBag
import os

def test_all_dags_load():
    """Verifies all 5 DAGs can be loaded without errors."""
    dagbag = DagBag(dag_folder='dags/', include_examples=False)
    
    expected_dags = [
        'csv_to_postgres_ingestion',
        'data_transformation_pipeline',
        'postgres_to_parquet_export',
        'conditional_workflow_pipeline',
        'notification_workflow'
    ]
    
    for dag_id in expected_dags:
        assert dag_id in dagbag.dags, f"DAG {dag_id} not found"
    
    assert len(dagbag.import_errors) == 0

def test_no_import_errors():
    """Verifies there are no import errors in any DAG file."""
    dagbag = DagBag(dag_folder='dags/', include_examples=False)
    assert len(dagbag.import_errors) == 0

def test_all_dag_ids_unique():
    """Verifies all DAG IDs are unique."""
    dagbag = DagBag(dag_folder='dags/', include_examples=False)
    dag_ids = [dag.dag_id for dag in dagbag.dags.values()]
    assert len(dag_ids) == len(set(dag_ids))

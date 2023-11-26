import pytest
from airflow.models import DagBag


def test_dag_loaded(dag_folder):

    dagbag = DagBag(
        dag_folder=dag_folder
    )

    assert dagbag.import_errors == {}
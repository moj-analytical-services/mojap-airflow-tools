import pytest
import mojap_airflow_tools

from airflow.contrib.operators.kubernetes_pod_operator import KubernetesPodOperator
from mojap_airflow_tools.operators import basic_kubernetes_pod_operator

def test_no_errors():
    
    k = basic_kubernetes_pod_operator(
        task_id="task1",
        dag=None,
        role="a_role",
        repo_name = None,
        release = None,
        full_image_name = "blah:v1.0.0",
    )

    assert isinstance(k, KubernetesPodOperator)

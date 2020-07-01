import pytest
from datetime import datetime

from airflow import DAG
from airflow.contrib.operators.kubernetes_pod_operator import KubernetesPodOperator

from mojap_airflow_tools.constants import ecr_base_path
from mojap_airflow_tools.operators import basic_kubernetes_pod_operator

# from mock import Mock
# mocked_dag = Mock()
# attrs = {"id.return_value": "test_dag"}
# mocked_dag.configure_mock(**attrs)


@pytest.mark.parametrize(
    "repo,tag,full",
    [("my-repo", "v0.0.0", None), (None, None, f"{ecr_base_path}my-repo:v0.0.0"),],
)
def test_no_errors(repo, tag, full):

    test_dag = DAG(
        "test-dag",
        default_args={},
        description="testing",
        start_date=datetime(2020, 1, 1),
        schedule_interval=None,
    )
    k = basic_kubernetes_pod_operator(
        task_id="task1",
        dag=test_dag,
        role="a_role",
        repo_name=repo,
        release=tag,
        full_image_name=full,
    )
    assert isinstance(k, KubernetesPodOperator)

    assert k.task_id == "task1"
    assert k.name == "task1"
    assert k.image == f"{ecr_base_path}my-repo:v0.0.0"
    assert k.namespace == "airflow"
    assert k.annotations == {"iam.amazonaws.com/role": "a_role"}

    expected_env = {
        "AWS_METADATA_SERVICE_TIMEOUT": "60",
        "AWS_METADATA_SERVICE_NUM_ATTEMPTS": "5",
        "AWS_DEFAULT_REGION": "eu-west-1",
    }
    assert k.env_vars == expected_env


def test_sandboxed():
    test_dag = DAG(
        "test-dag",
        default_args={},
        description="testing",
        start_date=datetime(2020, 1, 1),
        schedule_interval=None,
    )

    k = basic_kubernetes_pod_operator(
        task_id="task1",
        dag=test_dag,
        role="alpha_user_test",
        repo_name="my_repo",
        release="v0.0.0",
        sandboxed=True,
    )
    assert k.image == f"{ecr_base_path}my-repo:v0.0.0"
    assert k.annotations == {"iam.amazonaws.com/role": "alpha_user_test"}
    assert k.namespace == "user-test"


def test_error():
    test_dag = DAG(
        "test-dag",
        default_args={},
        description="testing",
        start_date=datetime(2020, 1, 1),
        schedule_interval=None,
    )

    with pytest.raises(ValueError):
        basic_kubernetes_pod_operator(
            task_id="task1",
            dag=test_dag,
            role="a_role",
            repo_name=None,
            release=None,
            full_image_name=None,
        )

    with pytest.raises(ValueError):
        basic_kubernetes_pod_operator(
            task_id="task1",
            dag=test_dag,
            role="a_role",
            repo_name="something",
            release="something",
            full_image_name="something",
        )

    with pytest.raises(ValueError):
        basic_kubernetes_pod_operator(
            task_id="invalid_underscore",
            dag=test_dag,
            role="a_role",
            repo_name="something",
            release="something",
        )

    with pytest.raises(ValueError):
        basic_kubernetes_pod_operator(
            task_id="invalid_underscore",
            dag=test_dag,
            role="a_role",
            repo_name="something",
            release="something",
        )

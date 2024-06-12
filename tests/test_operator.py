from datetime import datetime

import pytest
from airflow import DAG
from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import (
    KubernetesPodOperator,
)
from kubernetes.client.models import V1EnvVar
from mojap_airflow_tools.constants import ecr_base_path
from mojap_airflow_tools.operators import basic_kubernetes_pod_operator


@pytest.mark.parametrize(
    "repo,tag,full",
    [
        ("my-repo", "v0.0.0", None),
        (None, None, f"{ecr_base_path}my-repo:v0.0.0"),
    ],
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
    assert k.is_delete_operator_pod is True
    assert k.cluster_context == "aws"
    assert k.config_file == "/usr/local/airflow/dags/.kube/config"
    assert k.in_cluster is False
    assert k.get_logs is True
    assert k.security_context == {
        "allowPrivilegeEscalation": False,
        "runAsNonRoot": True,
        "privileged": False,
    }

    expected_env = [
        V1EnvVar(
            name="AWS_METADATA_SERVICE_TIMEOUT",
            value="60",
            value_from=None,
        ),
        V1EnvVar(
            name="AWS_METADATA_SERVICE_NUM_ATTEMPTS",
            value="5",
            value_from=None,
        ),
        V1EnvVar(
            name="AWS_DEFAULT_REGION",
            value="eu-west-1",
            value_from=None,
        ),
    ]
    assert k.env_vars == expected_env


def test_env_vars():
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
        repo_name="my_repo",
        release="v0.0.0",
        env_vars={"AWS_DEFAULT_REGION": "us-east-1", "EXTRA": "test"},
    )

    expected_env = [
        V1EnvVar(
            name="AWS_DEFAULT_REGION",
            value="us-east-1",
            value_from=None,
        ),
        V1EnvVar(
            name="EXTRA",
            value="test",
            value_from=None,
        ),
        V1EnvVar(
            name="AWS_METADATA_SERVICE_TIMEOUT",
            value="60",
            value_from=None,
        ),
        V1EnvVar(
            name="AWS_METADATA_SERVICE_NUM_ATTEMPTS",
            value="5",
            value_from=None,
        ),
    ]

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
    assert k.annotations == {}
    assert k.namespace == "user-test"
    assert k.is_delete_operator_pod is False
    assert k.cluster_context is None
    assert k.config_file is None
    assert k.in_cluster is True


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


def test_kwargs():
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
        role="a_test",
        repo_name="my_repo",
        release="v0.0.0",
        service_account_name="test_service_account",
    )

    assert k.service_account_name == "test_service_account"

def test_irsa():
    test_dag = DAG(
        "irsa-dag",
        default_args={},
        description="testing irsa",
        start_date=datetime(2020, 1, 1),
        schedule_interval=None,
    )
    k = basic_kubernetes_pod_operator(
        task_id="task1",
        dag=test_dag,
        role="a_test",
        repo_name="my_repo",
        release="v0.0.0",
        irsa=True
    )

    assert k.annotations == {"eks.amazonaws.com/role-arn": "a_test"}

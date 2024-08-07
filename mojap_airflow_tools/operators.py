import re
from typing import Optional

from airflow import DAG
from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import (
    KubernetesPodOperator,
)

from mojap_airflow_tools.constants import ECR_TEMPLATE, DEFAULT_ACC_NUMBER


def basic_kubernetes_pod_operator(
    task_id: str,
    dag: DAG,
    role: str,
    repo_name: Optional[str] = None,
    release: Optional[str] = None,
    full_image_name: Optional[str] = None,
    run_as_user: Optional[str] = None,
    env_vars: Optional[dict] = None,
    account_number: Optional[str] = None,
    sandboxed: Optional[bool] = False,
    service_account_name: Optional[str] = "default",
    environment: Optional[str] = None,
    **kwargs,
) -> KubernetesPodOperator:
    """
    A simple wrapper function for using the KubernetesPodOperator on the
    analytical platform (either on the deployed airflow or sandboxed version).
    Parameters
    ----------
    task_id: Name of the task (same as KubernetesPodOperator).

    dag: To assign the task to (same as KubernetesPodOperator).
        The name of the repository that the docker image you are running is.

    role: str
        The IAM role that will be used to run the docker image. This is usually
        your iam role (if running on the airflow sandbox). Otherwise this will
        be the role that is created alongside the image.

    repo_name:
        The name of the repository that the docker image you are running is
        built off of. Note that '_' in your repo name are replaced with '-'
        to match the naming convention of your docker image. Not this function
        assumes the docker image is saved in our private ECR docker repository.

    release:
        The name of the github release for your repository that your
        docker image was built from.

    full_image_name:
        If you are using a public docker image or one that wasn't automatically
        built from a guthub repo than use this parameter instead of the repo_name
        and release parameters. Expects the full path and name (including tag)
        of the docker image. This function will throw an error if this and the
        other three (repo_name, release, and account number) parameters are all
        not None.

    run_as_user:
        Supplies a username for the 'runAsUser' argument of the security context.
        If left blank, we assume the user is providing a user as part of the image,
        otherwise the image will not run in our containers.

    env_vars:
        The environment variables you want to pass to your docker image
        (same as KubernetesPodOperator). This function adds default values
        to your environment 'AWS_DEFAULT_REGION', 'AWS_METADATA_SERVICE_TIMEOUT'
        and 'AWS_METADATA_SERVICE_NUM_ATTEMPTS'. But can be overwritten if
        these keys exist in your env_vars parameter.

    account_number:
        the account number to pull the ECR image from, if providing one from ECR.
        This defaults to the data engineering account if not set.

    sandboxed:
        Set to True if running on your airflow sandbox environment and
        False (default) if running on deployed. If set to True this Operator
        will assume that you are running the pod in your own namespace
        'user-<github-username>'. Otherwise the namespace is set to airflow.

    service_account_name:
        This argument can optionally be entered to enable IRSA instead of Kube2IAM
        to give IAM permissions to containers running on the kubernetes cluster.
        The value should be the same as the ROLE, but using kebab-case.

    environment:
        If service_account_name is populated, an environment value should also be
        set as 'dev' or 'prod', depending on the airflow environment the DAG is
        running on.

    Returns
    -------
    KubernetesPodOperator
        A KubernetesPodOperator with set parameters.
    """

    # Check inputs
    allowed_chars = "[a-z0-9.-]"
    if not bool(re.search(f"^{allowed_chars}*$", task_id)):
        raise ValueError(
            f"Input task_id ({task_id}) only allows these characters: {allowed_chars}"
        )

    nullr = repo_name is None
    nulli = release is None
    nullfin = full_image_name is None
    if nullr and nulli:
        if nullfin:
            raise ValueError(
                "Please provide a (repo_name and release, and optional account_number) "
                "or full_image_name"
            )

    elif (not nullr) and (not nulli) and (not nullfin):
        msg = (
            "You cannot provide all three parameters. Please provide a (repo_name "
            "and release, and optional account_number) or full_image_name only."
        )
        raise ValueError(msg)

    if nullfin:
        repo_name = repo_name.replace("_", "-")
        acc_num = account_number if account_number else DEFAULT_ACC_NUMBER
        full_image_name = ECR_TEMPLATE.format(
            account_number=acc_num, repo_name=repo_name, release=release
        )

    if "_" in task_id or " " in task_id:
        raise ValueError(
            f"Characters '_' and ' ' are not allowed in task_id name given ({task_id})."
        )

    # Define stand envs to be passed to pod operator
    if env_vars is None:
        env_vars = dict()

    std_envs = {
        "AWS_METADATA_SERVICE_TIMEOUT": "60",
        "AWS_METADATA_SERVICE_NUM_ATTEMPTS": "5",
        "AWS_DEFAULT_REGION": "eu-west-1",
    }
    for k, v in std_envs.items():
        if k not in env_vars:
            env_vars[k] = v

    security_context = {
        "allowPrivilegeEscalation": False,
        "runAsNonRoot": True,
        "privileged": False,
    }

    cluster_context = {
        "dev" : "analytical-platform-compute-test",
        "prod" : "analytical-platform-compute-production"
    }

    if run_as_user is not None:
        security_context["runAsUser"] = run_as_user

    if sandboxed:
        user = role.replace("alpha_user_", "", 1).replace("_", "-").lower()
        namespace = f"user-{user}"
        kube_op = KubernetesPodOperator(
            dag=dag,
            namespace=namespace,
            image=full_image_name,
            env_vars=env_vars,
            labels={"app": dag.dag_id},
            name=task_id,
            in_cluster=True,
            is_delete_operator_pod=False,
            task_id=task_id,
            get_logs=True,
            service_account_name=f"{user}-jupyter",
            **kwargs,
        )
    elif service_account_name != "default":
        if environment not in ["dev", "prod"]:
            raise ValueError(
                    "if service_account_name argument is populated," 
                    f"environment should be 'dev' or 'prod'. ({environment=})"
                )
        kube_op = KubernetesPodOperator(
            dag=dag,
            namespace="airflow",
            image=full_image_name,
            env_vars=env_vars,
            labels={"app": dag.dag_id},
            name=task_id,
            in_cluster=False,
            is_delete_operator_pod=True,
            cluster_context=cluster_context[environment],
            config_file="/usr/local/airflow/dags/.kube/config",
            task_id=task_id,
            get_logs=True,
            security_context=security_context,
            service_account_name=service_account_name,
            **kwargs,
        )

    else:
        kube_op = KubernetesPodOperator(
            dag=dag,
            namespace="airflow",
            image=full_image_name,
            env_vars=env_vars,
            labels={"app": dag.dag_id},
            name=task_id,
            in_cluster=False,
            is_delete_operator_pod=True,
            cluster_context="aws",
            config_file="/usr/local/airflow/dags/.kube/config",
            task_id=task_id,
            get_logs=True,
            annotations={"iam.amazonaws.com/role": role},
            security_context=security_context,
            **kwargs,
        )

    return kube_op


BasicKubernetesPodOperator = basic_kubernetes_pod_operator

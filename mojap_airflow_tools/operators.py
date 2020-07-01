import re

from airflow import DAG
from airflow.contrib.operators.kubernetes_pod_operator import KubernetesPodOperator

from mojap_airflow_tools.constants import ecr_base_path


def basic_kubernetes_pod_operator(
    task_id: str,
    dag: DAG,
    role: str,
    repo_name: str = None,
    release: str = None,
    full_image_name: str = None,
    env_vars: dict = {},
    sandboxed: bool = False,
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
        other two parameters are all not None.

    env_vars:
        The environment variables you want to pass to your docker image
        (same as KubernetesPodOperator). This function adds default values
        to your environment 'AWS_DEFAULT_REGION', 'AWS_METADATA_SERVICE_TIMEOUT'
        and 'AWS_METADATA_SERVICE_NUM_ATTEMPTS'. But can be overwritten if
        these keys exist in your env_vars parameter.

    sandboxed:
        Set to True if running on your airflow sandbox environment and
        False (default) if running on deployed. If set to True this Operator
        will assume that you are running the pod in your own namespace
        'user-<github-username>'. Otherwise the namespace is set to airflow.

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
                "Please provide a (repo_name and release) or full_image_name"
            )

    elif (not nullr) and (not nulli) and (not nullfin):
        msg = (
            "You cannot provide all three parameters. "
            "Please provide a (repo_name and release) or full_image_name only."
        )
        raise ValueError(msg)

    if nullfin:
        repo_name = repo_name.replace("_", "-")
        full_image_name = f"{ecr_base_path}{repo_name}:{release}"

    if "_" in task_id or " " in task_id:
        raise ValueError(
            f"Characters '_' and ' ' are not allowed in task_id name given ({task_id})."
        )

    # Define stand envs to be passed to pod operator
    std_envs = {
        "AWS_METADATA_SERVICE_TIMEOUT": "60",
        "AWS_METADATA_SERVICE_NUM_ATTEMPTS": "5",
        "AWS_DEFAULT_REGION": "eu-west-1",
    }
    for k, v in std_envs.items():
        if k not in env_vars:
            env_vars[k] = v

    if sandboxed:
        user = role.replace("alpha_user_", "", 1)
        user = user.replace("_", "-")
        namespace = f"user-{user}"
    else:
        namespace = "airflow"

    kube_op = KubernetesPodOperator(
        dag=dag,
        namespace=namespace,
        image=full_image_name,
        env_vars=env_vars,
        labels={"app": dag.dag_id},
        name=task_id,
        in_cluster=True,
        task_id=task_id,
        get_logs=True,
        annotations={"iam.amazonaws.com/role": role},
    )

    return kube_op

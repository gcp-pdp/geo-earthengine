from __future__ import print_function

import os
from datetime import timedelta

from airflow import DAG, configuration
from airflow.contrib.operators.kubernetes_pod_operator import KubernetesPodOperator

from geoetl_airflow.utils.k8s_utils import build_pod_spec, build_secret_volume


def build_export_npp_dag(
    dag_id,
    output_bucket,
    export_start_date,
    export_end_date=None,
    notification_emails=None,
    export_schedule_interval=None,
    export_max_active_runs=None,
    export_concurrency=None,
    export_parallel_jobs=1,
    export_retries=5,
    export_retry_delay=300,
    export_retry_exponential_backoff=False,
    export_max_retry_delay=300,
    export_overwrite=False,
    export_secret="service-account",
    image_name="gcr.io/gcp-pdp-weather-dev/geo-exporter",
    image_version="1.0.0",
    image_pull_policy="Always",
    namespace="default",
    resources=None,
    node_selector="default-pool",
    included_images=None,
    excluded_images=None,
    output_path_prefix="export",
):
    default_dag_args = {
        "depends_on_past": False,
        "start_date": export_start_date,
        "end_date": export_end_date,
        "email_on_failure": True,
        "email_on_retry": True,
        "retries": export_retries,
        "retry_delay": timedelta(seconds=export_retry_delay),
        "retry_exponential_backoff": export_retry_exponential_backoff,
        "max_retry_delay": timedelta(seconds=export_max_retry_delay),
    }

    if notification_emails and len(notification_emails) > 0:
        default_dag_args["email"] = [
            email.strip() for email in notification_emails.split(",")
        ]

    if export_max_active_runs is None:
        export_max_active_runs = configuration.conf.getint(
            "core", "max_active_runs_per_dag"
        )

    dag = DAG(
        dag_id,
        schedule_interval=export_schedule_interval,
        default_args=default_dag_args,
        max_active_runs=export_max_active_runs,
        concurrency=export_concurrency,
        is_paused_upon_creation=True,
    )

    secret_volume = build_secret_volume(export_secret)

    data_dir = "/usr/share/gcs/data"

    task_id = "export_annual_npp"
    name = task_id.replace("_", "-")
    cmd = "export_to_gcs.sh -f {type} -d {date} {include} {exclude} -p {path} {overwrite}".format(
        type="annual_npp",
        date='{{ execution_date.strftime("%Y-%m-%d") }}',
        include=f"-i {included_images}" if included_images else "",
        exclude=f"-e {excluded_images}" if excluded_images else "",
        overwrite="-r" if export_overwrite else "",
        path=os.path.join(
            output_path_prefix,
            "annual_npp",
        ),
    )
    operator = KubernetesPodOperator(
        task_id=task_id,
        name=name,
        namespace=namespace,
        image="{name}:{version}".format(name=image_name, version=image_version),
        cmds=["/bin/bash", "-c", cmd],
        secrets=[secret_volume],
        startup_timeout_seconds=120,
        env_vars={
            "GOOGLE_APPLICATION_CREDENTIALS": "/var/secrets/google/service-account.json",
            "DATA_DIR": data_dir,
        },
        image_pull_policy=image_pull_policy,
        resources=resources,
        is_delete_operator_pod=True,
        full_pod_spec=build_pod_spec(
            name=name, bucket=output_bucket, data_dir=data_dir
        ),
        node_selectors={"cloud.google.com/gke-nodepool": node_selector},
        dag=dag,
    )
    return dag

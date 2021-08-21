from __future__ import print_function

import os.path
import uuid
from datetime import timedelta

from airflow import DAG, configuration, AirflowException
from airflow.contrib.kubernetes.secret import Secret
from airflow.contrib.operators.kubernetes_pod_operator import KubernetesPodOperator
import kubernetes.client.models as k8s


def build_export_dag(
        dag_id,
        export_type,
        output_bucket,
        export_start_date,
        export_end_date=None,
        notification_emails=None,
        export_schedule_interval='0 0 * * *',
        export_max_active_runs=None,
        image_name='gcr.io/gcp-pdp-weather-dev/geo-exporter',
        image_version='0.1.0',
        image_pull_policy='Always',
        namespace='default',
        resources=None,
        node_selector='default-pool',
        excluded_images='',
        output_path_prefix='export'
):
    """Build Export DAG"""

    default_dag_args = {
        "depends_on_past": False,
        "start_date": export_start_date,
        "end_date": export_end_date,
        "email_on_failure": True,
        "email_on_retry": True,
        "retries": 3,
        "retry_delay": timedelta(minutes=5)
    }

    if notification_emails and len(notification_emails) > 0:
        default_dag_args['email'] = [email.strip() for email in notification_emails.split(',')]

    if export_max_active_runs is None:
        export_max_active_runs = configuration.conf.getint('core', 'max_active_runs_per_dag')

    dag = DAG(
        dag_id,
        schedule_interval=export_schedule_interval,
        default_args=default_dag_args,
        max_active_runs=export_max_active_runs
    )

    secret_volume = Secret(
        deploy_type='volume',
        deploy_target='/var/secrets/google',
        secret='service-account',
        key='service-account.json'
    )

    extra_args = ''
    export_path = [output_path_prefix, export_type]

    if export_type == 'gfs':
        date = '{{ execution_date.strftime("%Y-%m-%d") }}'
        date_time = '{{ (execution_date - macros.timedelta(hours=4)).strftime("%Y-%m-%dT%H:00:00") }}'
        export_path.append('date={date}'.format(date=date))
        extra_args = '-d {date}'.format(date=date_time)
    elif export_type == 'world_pop':
        year = '{{ execution_date.strftime("%Y") }}'
        export_path.append('year={year}'.format(year=year))
        extra_args = '-y {year}'.format(year=year)

    cmd = 'export_to_gcs.sh -f {type} {extra_args} -o {bucket} -e {exclude} -p {path}' \
        .format(type=export_type,
                extra_args=extra_args,
                bucket=output_bucket,
                exclude=excluded_images,
                path=os.path.join(*export_path))

    task_id = 'export-{export_type}'.format(export_type=export_type).replace('_', '-')
    data_dir = '/usr/share/gcs/data'
    export_operator = KubernetesPodOperator(
        task_id=task_id,
        name=task_id,
        namespace=namespace,
        image="{name}:{version}".format(name=image_name, version=image_version),
        cmds=["/bin/bash", "-c", cmd],
        secrets=[secret_volume],
        startup_timeout_seconds=120,
        env_vars={
            'GOOGLE_APPLICATION_CREDENTIALS': '/var/secrets/google/service-account.json',
            'DATA_DIR': data_dir,
        },
        image_pull_policy=image_pull_policy,
        resources=resources,
        is_delete_operator_pod=True,
        full_pod_spec=build_pod_spec(name=task_id, bucket=output_bucket, data_dir=data_dir),
        node_selectors={'cloud.google.com/gke-nodepool': node_selector},
        dag=dag
    )

    return dag


def build_pod_spec(name, bucket, data_dir):
    metadata = k8s.V1ObjectMeta(
        name=make_unique_pod_name(name),
    )
    container = k8s.V1Container(
        name=name,
        lifecycle=k8s.V1Lifecycle(
            post_start=k8s.V1Handler(
                _exec=k8s.V1ExecAction(command=["gcsfuse", "--log-file", "/var/log/gcs_fuse.log", "--temp-dir", "/tmp", "--debug_gcs", bucket, data_dir])
            ),
            pre_stop=k8s.V1Handler(
                _exec=k8s.V1ExecAction(command=["fusermount", "-u", data_dir])
            ),
        ),
        security_context=k8s.V1SecurityContext(
            privileged=True,
            capabilities=k8s.V1Capabilities(add=['SYS_ADMIN'])
        ),
    )
    pod = k8s.V1Pod(metadata=metadata, spec=k8s.V1PodSpec(containers=[container]))

    return pod


def make_unique_pod_name(name):
    safe_uuid = uuid.uuid4().hex
    safe_pod_id = name + "-" + safe_uuid

    return safe_pod_id

from __future__ import print_function

import uuid
from abc import abstractmethod, ABC
from datetime import timedelta

import kubernetes.client.models as k8s
from airflow import DAG, configuration
from airflow.contrib.kubernetes.secret import Secret
from airflow.contrib.operators.kubernetes_pod_operator import KubernetesPodOperator


class BaseExporter(ABC):
    def __init__(
        self,
        dag_id,
        export_type,
        output_bucket,
        export_start_date,
        export_end_date=None,
        notification_emails=None,
        export_schedule_interval="0 0 * * *",
        export_max_active_runs=None,
        export_concurrency=None,
        export_parallel_jobs=1,
        export_retries=5,
        export_retry_delay=300,
        export_retry_exponential_backoff=False,
        export_max_retry_delay=300,
        export_overwrite=False,
        image_name="gcr.io/gcp-pdp-weather-dev/geo-exporter",
        image_version="1.0.0",
        image_pull_policy="Always",
        namespace="default",
        resources=None,
        node_selector="default-pool",
        excluded_images=None,
        output_path_prefix="export",
    ):
        self.dag_id = dag_id
        self.export_type = export_type
        self.output_bucket = output_bucket
        self.export_start_date = export_start_date
        self.export_end_date = export_end_date
        self.notification_emails = notification_emails
        self.export_schedule_interval = export_schedule_interval
        self.export_max_active_runs = export_max_active_runs
        self.export_retries = export_retries
        self.export_retry_delay = export_retry_delay
        self.export_retry_exponential_backoff = export_retry_exponential_backoff
        self.export_max_retry_delay = export_max_retry_delay
        self.image_name = image_name
        self.image_version = image_version
        self.image_pull_policy = image_pull_policy
        self.namespace = namespace
        self.resources = resources
        self.node_selector = node_selector
        self.excluded_images = excluded_images
        self.output_path_prefix = output_path_prefix
        self.export_concurrency = export_concurrency
        self.export_parallel_jobs = export_parallel_jobs
        self.export_overwrite = export_overwrite

    def build_dag(self):
        default_dag_args = {
            "depends_on_past": False,
            "start_date": self.export_start_date,
            "end_date": self.export_end_date,
            "email_on_failure": True,
            "email_on_retry": True,
            "retries": self.export_retries,
            "retry_delay": timedelta(seconds=self.export_retry_delay),
            "retry_exponential_backoff": self.export_retry_exponential_backoff,
            "max_retry_delay": timedelta(seconds=self.export_max_retry_delay),
        }

        if self.notification_emails and len(self.notification_emails) > 0:
            default_dag_args["email"] = [
                email.strip() for email in self.notification_emails.split(",")
            ]

        if self.export_max_active_runs is None:
            self.export_max_active_runs = configuration.conf.getint(
                "core", "max_active_runs_per_dag"
            )

        dag = DAG(
            self.dag_id,
            schedule_interval=self.export_schedule_interval,
            default_args=default_dag_args,
            max_active_runs=self.export_max_active_runs,
            concurrency=self.export_concurrency,
            is_paused_upon_creation=True,
        )

        secret_volume = Secret(
            deploy_type="volume",
            deploy_target="/var/secrets/google",
            secret="service-account",
            key="service-account.json",
        )

        data_dir = "/usr/share/gcs/data"

        task_id = f"export_{self.export_type}"
        name = task_id.replace("_", "-")
        operator = KubernetesPodOperator(
            task_id=task_id,
            name=name,
            namespace=self.namespace,
            image="{name}:{version}".format(
                name=self.image_name, version=self.image_version
            ),
            cmds=["/bin/bash", "-c", self.build_cmd()],
            secrets=[secret_volume],
            startup_timeout_seconds=120,
            env_vars={
                "GOOGLE_APPLICATION_CREDENTIALS": "/var/secrets/google/service-account.json",
                "DATA_DIR": data_dir,
            },
            image_pull_policy=self.image_pull_policy,
            resources=self.resources,
            is_delete_operator_pod=True,
            full_pod_spec=self.build_pod_spec(
                name=name, bucket=self.output_bucket, data_dir=data_dir
            ),
            node_selectors={"cloud.google.com/gke-nodepool": self.node_selector},
            dag=dag,
        )
        return dag

    @abstractmethod
    def build_cmd(self):
        pass

    def build_pod_spec(self, name, bucket, data_dir):
        metadata = k8s.V1ObjectMeta(
            name=self.make_unique_pod_name(name),
        )
        container = k8s.V1Container(
            name=name,
            lifecycle=k8s.V1Lifecycle(
                post_start=k8s.V1Handler(
                    _exec=k8s.V1ExecAction(
                        command=[
                            "gcsfuse",
                            "--log-file",
                            "/var/log/gcs_fuse.log",
                            "--temp-dir",
                            "/tmp",
                            "--debug_gcs",
                            bucket,
                            data_dir,
                        ]
                    )
                ),
                pre_stop=k8s.V1Handler(
                    _exec=k8s.V1ExecAction(command=["fusermount", "-u", data_dir])
                ),
            ),
            security_context=k8s.V1SecurityContext(
                privileged=True, capabilities=k8s.V1Capabilities(add=["SYS_ADMIN"])
            ),
        )
        pod = k8s.V1Pod(metadata=metadata, spec=k8s.V1PodSpec(containers=[container]))
        return pod

    @staticmethod
    def make_unique_pod_name(name):
        safe_uuid = uuid.uuid4().hex
        safe_pod_id = name + "-" + safe_uuid

        return safe_pod_id

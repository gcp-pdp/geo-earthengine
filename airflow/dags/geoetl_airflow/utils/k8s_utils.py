import uuid
import kubernetes.client.models as k8s
from airflow.contrib.kubernetes.secret import Secret


def build_pod_spec(name, bucket, data_dir):
    metadata = k8s.V1ObjectMeta(
        name=make_unique_pod_name(name),
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


def make_unique_pod_name(name):
    safe_uuid = uuid.uuid4().hex
    safe_pod_id = name + "-" + safe_uuid
    return safe_pod_id


def build_secret_volume(secret, key="service-account.json"):
    return Secret(
        deploy_type="volume",
        deploy_target="/var/secrets/google",
        secret=secret,
        key=key,
    )

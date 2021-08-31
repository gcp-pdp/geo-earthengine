set -e
set -o xtrace
set -o pipefail

dag_gcs_bucket=${1}

if [ -z "${dag_gcs_bucket}" ]; then
    echo "Usage: $0 <dag_gcs_bucket>"
    exit 1
fi

gsutil -m cp -r dags/* ${dag_gcs_bucket}/dags
gsutil -m cp -r plugins/* ${dag_gcs_bucket}/plugins
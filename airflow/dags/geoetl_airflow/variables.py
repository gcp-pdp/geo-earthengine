from datetime import datetime

from airflow.models import Variable


def read_export_dag_vars(group, **kwargs):
    """Read Airflow variables for Export DAG"""

    vars = read_vars(group, **kwargs)
    return {
        "output_bucket": vars.get("output_bucket"),
        "export_start_date": parse_date(vars.get("export_start_date")),
        "export_end_date": parse_date(vars.get("export_end_date")),
        "export_schedule_interval": vars.get("export_schedule_interval"),
        "export_retries": parse_int(vars.get("export_retries")),
        "export_retry_delay": parse_int(vars.get("export_retry_delay")),
        "output_path_prefix": vars.get("output_path_prefix"),
        "notification_emails": vars.get("notification_emails"),
        "image_name": vars.get("image_name"),
        "image_version": vars.get("image_version"),
        "image_pull_policy": vars.get("image_pull_policy"),
        "namespace": vars.get("namespace"),
        "resources": vars.get("resources"),
        "export_max_active_runs": parse_int(vars.get("export_max_active_runs")),
        "export_concurrency": parse_int(vars.get("export_concurrency")),
        "node_selector": vars.get("node_selector"),
        "excluded_images": vars.get("excluded_images"),
    }


def read_load_dag_vars(group, **kwargs):
    """Read Airflow variables for Load DAG"""

    vars = read_vars(group, **kwargs)
    return {
        "output_bucket": vars.get("output_bucket"),
        "output_path_prefix": vars.get("output_path_prefix"),
        "destination_dataset_project_id": vars.get("destination_dataset_project_id"),
        "destination_dataset_name": vars.get("destination_dataset_name"),
        "destination_table_name": vars.get("destination_table_name"),
        "notification_emails": vars.get("notification_emails"),
        "load_schedule_interval": vars.get("load_schedule_interval"),
        "load_max_active_runs": parse_int(vars.get("load_max_active_runs")),
        "load_concurrency": parse_int(vars.get("load_concurrency")),
        "load_start_date": parse_date(vars.get("load_start_date")),
        "load_end_date": parse_date(vars.get("load_end_date")),
        "load_retries": parse_int(vars.get("load_retries")),
        "load_retry_delay": parse_int(vars.get("load_retry_delay")),
    }


def read_vars(var_name, **kwargs):
    vars = {
        **kwargs,
        **read_var("base", required=True, deserialize_json=True),
        **read_var(var_name, required=True, deserialize_json=True),
    }
    return vars


def read_var(
    var_name, var_prefix=None, required=False, deserialize_json=False, **kwargs
):
    """Read Airflow variable"""
    full_var_name = f"{var_prefix}{var_name}" if var_prefix is not None else var_name
    var = Variable.get(full_var_name, default_var="", deserialize_json=deserialize_json)
    var = var if var != "" else None
    if var_prefix and var is None:
        var = read_var(var_name, None, required, deserialize_json, **kwargs)
    if var is None:
        var = kwargs.get(var_name)
    if required and var is None:
        raise ValueError(f"{full_var_name} variable is required")
    return var


def parse_bool(bool_string, default=True):
    if isinstance(bool_string, bool):
        return bool_string
    if bool_string is None or len(bool_string) == 0:
        return default
    else:
        return bool_string.lower() in ["true", "yes"]


def parse_date(date_string, default=None):
    return (
        datetime.strptime(date_string, "%Y-%m-%d")
        if date_string is not None
        else default
    )


def parse_int(int_string, default=None):
    return int(int_string) if int_string is not None else default

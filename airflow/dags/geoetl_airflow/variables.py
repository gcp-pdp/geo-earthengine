from datetime import datetime

from airflow.models import Variable


def read_export_dag_vars(var_prefix, **kwargs):
    """Read Airflow variables for Export DAG"""
    export_start_date = read_var('export_start_date', var_prefix, True, **kwargs)
    export_start_date = datetime.strptime(export_start_date, '%Y-%m-%d')

    export_end_date = read_var('export_end_date', var_prefix, False, **kwargs)
    export_end_date = datetime.strptime(export_end_date, '%Y-%m-%d') if export_end_date is not None else None

    export_max_active_runs = read_var('export_max_active_runs', var_prefix, False, **kwargs)
    export_max_active_runs = int(export_max_active_runs) if export_max_active_runs is not None else None

    vars = {
        'output_bucket': read_var('output_bucket', var_prefix, True, **kwargs),
        'export_start_date': export_start_date,
        'export_end_date': export_end_date,
        'export_schedule_interval': read_var('export_schedule_interval', var_prefix, False, **kwargs),
        'notification_emails': read_var('notification_emails', None, False, **kwargs),
        'export_max_active_runs': export_max_active_runs,
    }

    return vars


def read_load_dag_vars(var_prefix, **kwargs):
    """Read Airflow variables for Load DAG"""
    load_max_active_runs = read_var('load_max_active_runs', var_prefix, False, **kwargs)
    load_max_active_runs = int(load_max_active_runs) if load_max_active_runs is not None else None

    vars = {
        'output_bucket': read_var('output_bucket', var_prefix, True, **kwargs),
        'destination_dataset_project_id': read_var('destination_dataset_project_id', var_prefix, True, **kwargs),
        'destination_dataset_name': read_var('destination_dataset_name', var_prefix, True, **kwargs),
        'notification_emails': read_var('notification_emails', None, False, **kwargs),
        'load_schedule_interval': read_var('load_schedule_interval', var_prefix, False, **kwargs),
        'load_max_active_runs': load_max_active_runs,
    }

    load_start_date = read_var('load_start_date', var_prefix, False, **kwargs)
    if load_start_date is not None:
        vars['load_start_date'] = datetime.strptime(load_start_date, '%Y-%m-%d')

    load_end_date = read_var('load_end_date', var_prefix, False, **kwargs)
    if load_end_date is not None:
        vars['load_end_date'] = datetime.strptime(load_end_date, '%Y-%m-%d')

    return vars


def read_var(var_name, var_prefix=None, required=False, **kwargs):
    """Read Airflow variable"""
    full_var_name = f'{var_prefix}{var_name}' if var_prefix is not None else var_name
    var = Variable.get(full_var_name, '')
    var = var if var != '' else None
    if var_prefix and var is None:
        var = read_var(var_name, None, required, **kwargs)
    if var is None:
        var = kwargs.get(var_name)
    if required and var is None:
        raise ValueError(f'{full_var_name} variable is required')
    return var


def parse_bool(bool_string, default=True):
    if isinstance(bool_string, bool):
        return bool_string
    if bool_string is None or len(bool_string) == 0:
        return default
    else:
        return bool_string.lower() in ["true", "yes"]
from geoetl_airflow.exporter.annual_npp_exporter import AnnualNPPExporter
from geoetl_airflow.exporter.gfs_exporter import GFSExporter
from geoetl_airflow.exporter.worldpop_exporter import WorldPopExporter


def build_export_dag(data_type, **kwargs):
    exporters = {
        "GFS": GFSExporter,
        "ANNUAL_NPP": AnnualNPPExporter,
        "WORLD_POP": WorldPopExporter,
    }
    return exporters[data_type](**kwargs).build_dag()

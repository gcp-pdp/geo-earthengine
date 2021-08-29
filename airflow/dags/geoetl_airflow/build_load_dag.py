from geoetl_airflow.loader.annual_npp_loader import AnnualNPPLoader
from geoetl_airflow.loader.gfs_loader import GFSLoader
from geoetl_airflow.loader.worldpop_loader import WorldPopLoader


def build_load_dag(data_type, **kwargs):
    loaders = {
        "GFS": GFSLoader,
        "ANNUAL_NPP": AnnualNPPLoader,
        "WORLD_POP": WorldPopLoader,
    }
    return loaders[data_type](**kwargs).build_dag()

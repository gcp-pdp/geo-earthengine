from geoetl_airflow.exporter.base_exporter import BaseExporter
import os.path


class AnnualNPPExporter(BaseExporter):
    def __init__(self, **kwargs):
        super(AnnualNPPExporter, self).__init__(**kwargs)

    def build_cmds(self):
        yield (
            "export-annual-npp",
            "export_to_gcs.sh -f {type} -o {bucket} -e {exclude} -p {path}".format(
                type="annual_npp",
                bucket=self.output_bucket,
                exclude=self.excluded_images,
                path=os.path.join(
                    self.output_path_prefix,
                    "annual_npp",
                ),
            ),
        )

from geoetl_airflow.exporter.base_exporter import BaseExporter
import os.path


class AnnualNPPExporter(BaseExporter):
    def __init__(self, **kwargs):
        super(AnnualNPPExporter, self).__init__(export_type="annual_npp", **kwargs)

    def build_cmd(self):
        return "export_to_gcs.sh -f {type} -o {bucket} -e {exclude} -p {path} {overwrite}".format(
            type="annual_npp",
            bucket=self.output_bucket,
            exclude=self.excluded_images,
            overwrite="-r" if self.export_overwrite else "",
            path=os.path.join(
                self.output_path_prefix,
                "annual_npp",
            ),
        )

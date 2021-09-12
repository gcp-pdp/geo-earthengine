from geoetl_airflow.exporter.base_exporter import BaseExporter
import os.path


class WorldPopExporter(BaseExporter):
    def __init__(self, **kwargs):
        super(WorldPopExporter, self).__init__(export_type="world_pop", **kwargs)

    def build_cmd(self):
        year = '{{ execution_date.strftime("%Y") }}'
        return "export_to_gcs.sh -f {type} -y {year} -o {bucket} -e {exclude} -p {path} {overwrite}".format(
            type="world_pop",
            year=year,
            bucket=self.output_bucket,
            exclude=self.excluded_images,
            overwrite="-r" if self.export_overwrite else "",
            path=os.path.join(
                self.output_path_prefix,
                "world_pop",
                "year={year}".format(year=year),
            ),
        )
